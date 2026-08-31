"""
Keep the archive in S3 instead of on the Streamlit container's disk.

The container is disposable: anything written to it disappears on the next
restart or redeploy. This module moves the two things that matter -- the
message database and the archived photos -- into the S3 bucket, so a restart
costs nothing.

Nothing here is public. The bucket stays private and photos reach the
browser only as short-lived presigned URLs.
"""

from __future__ import annotations

import mimetypes
import shutil
import sqlite3
import tempfile
from pathlib import Path
from typing import Iterable, Iterator

import boto3
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError


# Where the database object lives in the bucket. Deliberately outside every
# properties/ prefix so it can never be mistaken for a property's archive.
DATABASE_KEY = "system/database/operations_archive.db"

# Marker that tells display_media a media_path is an S3 key, not a local file.
S3_URI_PREFIX = "s3://"


# ============================================================
# Client and configuration
# ============================================================

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
        region_name=st.secrets["AWS_REGION"],
    )


def get_bucket() -> str:
    return st.secrets["S3_BUCKET"]


def sanitize_name(value: str) -> str:
    """Match the key style already used by s3_direct_upload.build_object_key."""
    import re

    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "-", value.strip())
    return cleaned.strip("-").lower() or "unknown-property"


# ============================================================
# The database object
# ============================================================

def _workspace() -> Path:
    """A stable per-container scratch directory for the working database."""
    workspace = Path(tempfile.gettempdir()) / "waves-archive"
    workspace.mkdir(parents=True, exist_ok=True)
    return workspace


def local_database_path() -> Path:
    return _workspace() / "operations_archive.db"


def database_exists_in_s3(s3_client=None, bucket: str | None = None) -> bool:
    client = s3_client or get_s3_client()
    bucket = bucket or get_bucket()

    try:
        client.head_object(Bucket=bucket, Key=DATABASE_KEY)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise
    except BotoCoreError:
        raise


def pull_database(force: bool = False) -> Path:
    """
    Fetch the archive database from S3 into container-local scratch space.

    Returns the local path. If the bucket has no database yet (a brand new
    install), returns a path that does not exist -- callers create it.
    """
    destination = local_database_path()

    if destination.exists() and not force:
        return destination

    client = get_s3_client()
    bucket = get_bucket()

    if not database_exists_in_s3(client, bucket):
        return destination

    # Download beside the target, then move into place, so a failed or
    # partial download can never leave a corrupt database behind.
    staging = destination.with_suffix(".downloading")

    try:
        client.download_file(Bucket=bucket, Key=DATABASE_KEY, Filename=str(staging))
        shutil.move(str(staging), str(destination))
    except (ClientError, BotoCoreError, OSError) as exc:
        staging.unlink(missing_ok=True)
        raise RuntimeError(
            f"Could not download the archive database from S3: {exc}"
        ) from exc

    return destination


def push_database() -> None:
    """
    Upload the working database back to S3. Call this after every import --
    until it runs, the import exists only on a disposable container.
    """
    source = local_database_path()

    if not source.exists():
        raise RuntimeError("There is no local database to upload.")

    try:
        get_s3_client().upload_file(
            Filename=str(source),
            Bucket=get_bucket(),
            Key=DATABASE_KEY,
            ExtraArgs={"ServerSideEncryption": "AES256"},
        )
    except (ClientError, BotoCoreError, OSError) as exc:
        raise RuntimeError(
            f"Could not save the archive database to S3: {exc}"
        ) from exc


# ============================================================
# Media objects
# ============================================================

MEDIA_FOLDER_NAMES = {
    "photo": "Photos",
    "video": "Videos",
    "audio": "Audio",
    "document": "Documents",
    "other": "Other",
}


def build_media_key(
    property_name: str,
    message_date: str,
    media_type: str,
    attachment_filename: str,
) -> str:
    """
    properties/<property>/processed/media/<YYYY>/<MM>/<DD>/<Photos>/<file>

    A new prefix, deliberately separate from original-uploads/, so nothing
    that already exists in the bucket is touched.
    """
    safe_property = sanitize_name(property_name)
    safe_filename = Path(attachment_filename).name.replace(" ", "_")
    folder = MEDIA_FOLDER_NAMES.get(media_type, "Other")

    try:
        year, month, day = message_date.split("-")
    except ValueError:
        year, month, day = "unknown", "unknown", "unknown"

    return (
        f"properties/{safe_property}/processed/media/"
        f"{year}/{month}/{day}/{folder}/{safe_filename}"
    )


def media_uri(key: str) -> str:
    return f"{S3_URI_PREFIX}{key}"


def is_s3_media(media_path: str | None) -> bool:
    return bool(media_path) and str(media_path).startswith(S3_URI_PREFIX)


def key_from_media_uri(media_path: str) -> str:
    return str(media_path)[len(S3_URI_PREFIX):]


def upload_media_stream(
    key: str,
    chunks: Iterable[bytes],
    filename: str,
    s3_client=None,
    bucket: str | None = None,
) -> int:
    """
    Write a media file to S3 from an iterator of chunks.

    Uses multipart so nothing is ever held whole in memory, and so files
    above the 5 GB single-PUT limit are handled correctly. Returns the
    number of bytes written.
    """
    client = s3_client or get_s3_client()
    bucket = bucket or get_bucket()

    content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

    # S3 requires every part except the last to be at least 5 MiB.
    minimum_part = 5 * 1024 * 1024

    upload_id = None
    parts: list[dict] = []
    buffer = bytearray()
    total = 0

    def flush_part(final: bool = False) -> None:
        nonlocal buffer
        if not buffer and not final:
            return
        part_number = len(parts) + 1
        response = client.upload_part(
            Bucket=bucket,
            Key=key,
            PartNumber=part_number,
            UploadId=upload_id,
            Body=bytes(buffer),
        )
        parts.append({"ETag": response["ETag"], "PartNumber": part_number})
        buffer = bytearray()

    try:
        first = bytearray()
        iterator = iter(chunks)

        # Buffer up to one part before deciding whether multipart is needed.
        for chunk in iterator:
            first.extend(chunk)
            total += len(chunk)
            if len(first) >= minimum_part:
                break

        if len(first) < minimum_part:
            # Small file: a single PUT is cheaper and simpler.
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=bytes(first),
                ContentType=content_type,
                ServerSideEncryption="AES256",
            )
            return total

        upload_id = client.create_multipart_upload(
            Bucket=bucket,
            Key=key,
            ContentType=content_type,
            ServerSideEncryption="AES256",
        )["UploadId"]

        buffer.extend(first)
        flush_part()

        for chunk in iterator:
            buffer.extend(chunk)
            total += len(chunk)
            if len(buffer) >= minimum_part:
                flush_part()

        if buffer:
            flush_part()

        client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        return total

    except Exception:
        if upload_id:
            try:
                client.abort_multipart_upload(
                    Bucket=bucket, Key=key, UploadId=upload_id
                )
            except Exception:
                pass
        raise


def presigned_media_url(key: str, expires_in: int = 900) -> str:
    """A short-lived read link. The bucket itself stays private."""
    try:
        return get_s3_client().generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": get_bucket(), "Key": key},
            ExpiresIn=expires_in,
        )
    except (ClientError, BotoCoreError) as exc:
        raise RuntimeError(f"Could not generate a secure photo link: {exc}") from exc


# ============================================================
# Pending media bookkeeping
# ============================================================

def count_pending_media(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS pending
        FROM messages
        WHERE attachment_filename IS NOT NULL
          AND attachment_filename != ''
          AND (media_path IS NULL OR media_path = '')
        """
    ).fetchone()
    return int(row["pending"])


def pending_media_batch(
    connection: sqlite3.Connection,
    limit: int = 250,
) -> list[sqlite3.Row]:
    """
    The next slice of attachments still waiting to be copied into S3.

    Grouped by source_file so one archive is opened once per batch rather
    than once per photo.
    """
    return connection.execute(
        """
        SELECT id, property_name, message_date, media_type,
               attachment_filename, source_file
        FROM messages
        WHERE attachment_filename IS NOT NULL
          AND attachment_filename != ''
          AND (media_path IS NULL OR media_path = '')
        ORDER BY source_file, timestamp
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def set_media_path(
    connection: sqlite3.Connection,
    message_id: int,
    media_path: str,
) -> None:
    connection.execute(
        "UPDATE messages SET media_path = ? WHERE id = ?",
        (media_path, message_id),
    )
