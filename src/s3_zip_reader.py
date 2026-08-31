"""
Read a ZIP archive that lives in S3 without downloading it.

A ZIP file stores its index of contents at the END of the file, and every
entry in that index records the exact byte offset of its member. S3 serves
byte ranges. Put those together and a 20 GB archive can be opened, listed,
and read one member at a time while only a few kilobytes ever move.

This is what makes a 20 GB import possible on a small Streamlit container:
peak disk stays at zero and peak memory stays at one chunk.
"""

from __future__ import annotations

import io
import zipfile
from typing import Iterator

from botocore.exceptions import BotoCoreError, ClientError


# Members of a WhatsApp export that are never real attachments.
_IGNORED_PREFIXES = ("__MACOSX/", ".")

# How much to pull per range request when scanning. 256 KB comfortably
# covers the end-of-archive record and most central directories in one go.
_SCAN_CHUNK = 256 * 1024


class S3RangeReader(io.RawIOBase):
    """
    A seekable, readable file object backed by S3 range requests.

    ``zipfile.ZipFile`` only needs read/seek/tell, so it accepts this as if
    it were a local file. Every read becomes a ranged ``get_object`` and
    nothing is ever cached to disk.
    """

    def __init__(self, s3_client, bucket: str, key: str, size: int | None = None):
        self._client = s3_client
        self._bucket = bucket
        self._key = key
        self._pos = 0

        if size is None:
            try:
                size = int(
                    s3_client.head_object(Bucket=bucket, Key=key)["ContentLength"]
                )
            except (ClientError, BotoCoreError) as exc:
                raise RuntimeError(
                    f"Could not read the archive '{key}' from S3: {exc}"
                ) from exc

        self._size = int(size)

        # Diagnostics, so the app can honestly report how little it moved.
        self.bytes_fetched = 0
        self.range_requests = 0

    # -- properties ---------------------------------------------------

    @property
    def size(self) -> int:
        return self._size

    # -- the only path to S3 ------------------------------------------

    def _fetch(self, start: int, length: int) -> bytes:
        if length <= 0:
            return b""

        end = min(start + length, self._size) - 1
        if end < start:
            return b""

        try:
            response = self._client.get_object(
                Bucket=self._bucket,
                Key=self._key,
                Range=f"bytes={start}-{end}",
            )
            data = response["Body"].read()
        except (ClientError, BotoCoreError) as exc:
            raise RuntimeError(
                f"Could not read bytes {start}-{end} of '{self._key}': {exc}"
            ) from exc

        self.range_requests += 1
        self.bytes_fetched += len(data)
        return data

    # -- file object protocol -----------------------------------------

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            new_position = offset
        elif whence == io.SEEK_CUR:
            new_position = self._pos + offset
        elif whence == io.SEEK_END:
            new_position = self._size + offset
        else:
            raise ValueError(f"Unsupported whence value: {whence}")

        self._pos = max(0, min(new_position, self._size))
        return self._pos

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = self._size - self._pos

        size = min(size, self._size - self._pos)
        data = self._fetch(self._pos, size)
        self._pos += len(data)
        return data

    def readinto(self, buffer) -> int:
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)


def open_s3_zip(
    s3_client,
    bucket: str,
    key: str,
    size: int | None = None,
) -> tuple[zipfile.ZipFile, S3RangeReader]:
    """
    Open a ZIP stored in S3. Returns the archive and the underlying reader
    so the caller can report how many bytes actually moved.
    """
    reader = S3RangeReader(s3_client, bucket, key, size=size)
    buffered = io.BufferedReader(reader, buffer_size=_SCAN_CHUNK)

    try:
        archive = zipfile.ZipFile(buffered)
    except zipfile.BadZipFile as exc:
        raise zipfile.BadZipFile(
            f"'{key}' is not a valid ZIP archive: {exc}"
        ) from exc

    return archive, reader


def _is_real_file(info: zipfile.ZipInfo) -> bool:
    if info.is_dir():
        return False

    name = info.filename
    if any(name.startswith(prefix) for prefix in _IGNORED_PREFIXES):
        return False

    # Ignore dotfiles in any folder (._foo, .DS_Store).
    return not name.rsplit("/", 1)[-1].startswith(".")


def find_chat_member(archive: zipfile.ZipFile) -> str:
    """
    Locate the WhatsApp transcript inside the archive.

    Mirrors the behaviour of the app's existing find_chat_file(): prefer an
    exact _chat.txt, otherwise accept a lone .txt file.
    """
    members = [info.filename for info in archive.infolist() if _is_real_file(info)]

    exact = [name for name in members if name.rsplit("/", 1)[-1] == "_chat.txt"]
    if exact:
        # Shallowest match wins, so a nested duplicate never shadows the real one.
        return min(exact, key=lambda name: name.count("/"))

    text_files = [name for name in members if name.lower().endswith(".txt")]
    if len(text_files) == 1:
        return text_files[0]

    raise FileNotFoundError(
        "This archive does not contain a recognizable WhatsApp _chat.txt file."
    )


def read_chat_text(archive: zipfile.ZipFile, member: str) -> str:
    """
    Read the transcript out of the archive. This is the only member read in
    full, and it is text, so it stays small no matter how big the ZIP is.
    """
    with archive.open(member) as handle:
        raw = handle.read()

    for encoding in ("utf-8-sig", "utf-8", "utf-16", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue

    return raw.decode("utf-8", errors="replace")


def build_attachment_index(archive: zipfile.ZipFile) -> dict[str, str]:
    """
    Map lowercased attachment filenames to their member paths, so a message
    referencing 'IMG-001.jpg' can find 'WhatsApp Chat - X/IMG-001.jpg'.
    """
    index: dict[str, str] = {}

    for info in archive.infolist():
        if not _is_real_file(info):
            continue
        base = info.filename.rsplit("/", 1)[-1].lower()
        index.setdefault(base, info.filename)

    return index


def stream_member(
    archive: zipfile.ZipFile,
    member: str,
    chunk_size: int = 8 * 1024 * 1024,
) -> Iterator[bytes]:
    """
    Yield a member's bytes in bounded chunks. Nothing is written to disk and
    no more than one chunk is held in memory at a time.
    """
    with archive.open(member) as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                return
            yield chunk


def member_size(archive: zipfile.ZipFile, member: str) -> int:
    return archive.getinfo(member).file_size
