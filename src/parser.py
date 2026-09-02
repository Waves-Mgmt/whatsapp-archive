from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


MESSAGE_PATTERN = re.compile(
    r"^\[(?P<timestamp>.+?)\]\s(?P<content>.*)$"
)

SENDER_PATTERN = re.compile(
    r"^(?P<sender>[^:]+):\s?(?P<message>.*)$",
    flags=re.DOTALL,
)

ATTACHMENT_PATTERN = re.compile(
    r"<attached:\s*(?P<filename>.+?)>",
    flags=re.IGNORECASE,
)


@dataclass
class WhatsAppMessage:
    timestamp: datetime
    sender: Optional[str]
    message_text: str
    attachment_filename: Optional[str]
    message_type: str
    is_system_message: bool
    source_file: str

    def to_dict(self) -> dict:
        """
        Convert the message object into a dictionary.

        The datetime is converted to a readable string so it can
        later be written into SQLite, CSV, or Excel.
        """
        record = asdict(self)
        record["timestamp"] = self.timestamp.isoformat(sep=" ")
        return record


def normalize_export_line(line: str) -> str:
    """
    Remove invisible Unicode formatting characters that may appear
    before timestamps or inside WhatsApp exports.

    These invisible characters can prevent a valid message line from
    matching the timestamp regular expression.
    """
    invisible_characters = {
        "\u200e",  # Left-to-right mark
        "\u200f",  # Right-to-left mark
        "\u202a",  # Left-to-right embedding
        "\u202b",  # Right-to-left embedding
        "\u202c",  # Pop directional formatting
        "\u202d",  # Left-to-right override
        "\u202e",  # Right-to-left override
        "\u2066",  # Left-to-right isolate
        "\u2067",  # Right-to-left isolate
        "\u2068",  # First strong isolate
        "\u2069",  # Pop directional isolate
        "\ufeff",  # Byte-order mark
    }

    normalized_line = line

    for character in invisible_characters:
        normalized_line = normalized_line.replace(character, "")

    return normalized_line.rstrip("\n\r")


def parse_timestamp(timestamp_text: str) -> datetime:
    """
    Parse timestamps commonly found in iPhone WhatsApp exports.

    Examples:
        11/16/24, 6:10:46 AM
        11/16/24, 6:10 AM
        16/11/24, 18:10:46
    """
    timestamp_formats = [
        "%m/%d/%y, %I:%M:%S %p",
        "%m/%d/%y, %I:%M %p",
        "%m/%d/%Y, %I:%M:%S %p",
        "%m/%d/%Y, %I:%M %p",
        "%d/%m/%y, %I:%M:%S %p",
        "%d/%m/%y, %I:%M %p",
        "%d/%m/%Y, %I:%M:%S %p",
        "%d/%m/%Y, %I:%M %p",
        "%m/%d/%y, %H:%M:%S",
        "%m/%d/%y, %H:%M",
        "%m/%d/%Y, %H:%M:%S",
        "%m/%d/%Y, %H:%M",
        "%d/%m/%y, %H:%M:%S",
        "%d/%m/%y, %H:%M",
        "%d/%m/%Y, %H:%M:%S",
        "%d/%m/%Y, %H:%M",
    ]

    cleaned_timestamp = normalize_export_line(timestamp_text)
    cleaned_timestamp = cleaned_timestamp.replace("\u202f", " ")
    cleaned_timestamp = cleaned_timestamp.replace("\xa0", " ")
    cleaned_timestamp = cleaned_timestamp.strip()

    for timestamp_format in timestamp_formats:
        try:
            return datetime.strptime(
                cleaned_timestamp,
                timestamp_format,
            )
        except ValueError:
            continue

    raise ValueError(
        f"Unsupported timestamp format: {timestamp_text!r}"
    )


def detect_media_type(
    filename: Optional[str],
) -> str:
    """
    Determine the message type from the attachment extension.
    """
    if not filename:
        return "text"

    extension = Path(filename).suffix.lower()

    image_extensions = {
        ".jpg",
        ".jpeg",
        ".png",
        ".heic",
        ".webp",
        ".gif",
        ".bmp",
        ".tiff",
        ".tif",
    }

    video_extensions = {
        ".mp4",
        ".mov",
        ".m4v",
        ".avi",
        ".mkv",
        ".webm",
    }

    audio_extensions = {
        ".opus",
        ".ogg",
        ".mp3",
        ".m4a",
        ".wav",
        ".aac",
        ".flac",
    }

    document_extensions = {
        ".pdf",
        ".doc",
        ".docx",
        ".xls",
        ".xlsx",
        ".csv",
        ".txt",
        ".ppt",
        ".pptx",
        ".zip",
        ".rar",
        ".pages",
        ".numbers",
        ".key",
    }

    if extension in image_extensions:
        return "photo"

    if extension in video_extensions:
        return "video"

    if extension in audio_extensions:
        return "audio"

    if extension in document_extensions:
        return "document"

    return "other"


def extract_attachment(
    message_text: str,
) -> tuple[Optional[str], str]:
    """
    Extract an attachment filename from a WhatsApp message.

    Example:
        <attached: 00000009-PHOTO-2024-11-16.jpg>

    Returns:
        attachment filename
        message text with the attachment marker removed
    """
    attachment_match = ATTACHMENT_PATTERN.search(
        message_text
    )

    if not attachment_match:
        return None, message_text.strip()

    filename = attachment_match.group(
        "filename"
    ).strip()

    cleaned_message = ATTACHMENT_PATTERN.sub(
        "",
        message_text,
    ).strip()

    return filename, cleaned_message


def create_message_record(
    timestamp_text: str,
    content: str,
    source_file: Path,
) -> WhatsAppMessage:
    """
    Convert one raw WhatsApp message into a structured record.
    """
    timestamp = parse_timestamp(timestamp_text)

    normalized_content = normalize_export_line(content).strip()

    sender_match = SENDER_PATTERN.match(
        normalized_content
    )

    if sender_match:
        sender = sender_match.group(
            "sender"
        ).strip()

        message_text = sender_match.group(
            "message"
        ).strip()

        is_system_message = False

    else:
        sender = None
        message_text = normalized_content
        is_system_message = True

    attachment_filename, cleaned_message = (
        extract_attachment(message_text)
    )

    message_type = detect_media_type(
        attachment_filename
    )

    return WhatsAppMessage(
        timestamp=timestamp,
        sender=sender,
        message_text=cleaned_message,
        attachment_filename=attachment_filename,
        message_type=message_type,
        is_system_message=is_system_message,
        source_file=str(source_file),
    )


def parse_whatsapp_chat_lines(
    lines,
    source_file,
) -> list[WhatsAppMessage]:
    """
    Parse an iterable of raw export lines.

    This is the shared core. parse_whatsapp_chat() feeds it a file on disk;
    parse_whatsapp_chat_text() feeds it a transcript read straight out of a
    ZIP in S3. Both take exactly the same code path, so results are identical.
    """
    messages: list[WhatsAppMessage] = []

    current_timestamp: Optional[str] = None
    current_content_lines: list[str] = []

    def flush() -> None:
        if current_timestamp is not None and current_content_lines:
            messages.append(
                create_message_record(
                    timestamp_text=current_timestamp,
                    content="\n".join(current_content_lines),
                    source_file=source_file,
                )
            )

    for raw_line in lines:
        line = normalize_export_line(raw_line)
        message_match = MESSAGE_PATTERN.match(line)

        if message_match:
            flush()
            current_timestamp = message_match.group("timestamp").strip()
            current_content_lines = [message_match.group("content")]
        elif current_timestamp is not None:
            current_content_lines.append(line)

    flush()

    return messages


def parse_whatsapp_chat_text(
    chat_text: str,
    source_file: str = "unknown",
) -> list[WhatsAppMessage]:
    """
    Parse a transcript already held as text -- for archives read directly
    from S3, where the ZIP is never written to disk.
    """
    return parse_whatsapp_chat_lines(
        chat_text.splitlines(),
        source_file=source_file,
    )


def parse_whatsapp_chat(
    chat_file: Path,
) -> list[WhatsAppMessage]:
    """
    Parse a WhatsApp export into structured message records.

    This supports:
    - standard text messages;
    - attachment messages;
    - system messages;
    - emojis;
    - Unicode sender names;
    - multiline messages;
    - invisible direction markers in iPhone exports.

    Any line that does not begin with a valid WhatsApp timestamp is
    appended to the previous message.
    """
    if not chat_file.exists():
        raise FileNotFoundError(
            f"Chat file does not exist: {chat_file}"
        )

    if not chat_file.is_file():
        raise ValueError(
            f"Chat path is not a file: {chat_file}"
        )

    with chat_file.open(
        "r",
        encoding="utf-8-sig",
        errors="replace",
    ) as file:
        return parse_whatsapp_chat_lines(
            file,
            source_file=chat_file,
        )