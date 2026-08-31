"""
Import a WhatsApp archive that already lives in S3, without downloading it.

Two stages, deliberately separate:

  1. import_archive_messages() reads only the transcript. It is fast even for
     a 20 GB archive, because the transcript is a few megabytes of text.
     After this the messages are browsable.

  2. process_media_batch() copies photos from the archive into S3 a slice at
     a time. It is resumable: if the browser disconnects or Streamlit
     restarts, running it again picks up exactly where it stopped, because
     "still to do" is a database query, not in-memory state.

Neither stage writes the archive to disk.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Callable

import archive_store
from archive_store import (
    build_media_key,
    get_bucket,
    get_s3_client,
    media_uri,
    pending_media_batch,
    sanitize_name,
    set_media_path,
    upload_media_stream,
)
from database import initialize_database, insert_messages
from parser import parse_whatsapp_chat_text
from s3_zip_reader import (
    build_attachment_index,
    find_chat_member,
    open_s3_zip,
    read_chat_text,
    stream_member,
)


SOURCE_PREFIX = "s3zip://"

# Written into media_path when a transcript references an attachment the
# export did not actually include, so it is recorded once and never retried.
MISSING_MEDIA = "missing://not-in-archive"


# ============================================================
# Listing what is in the bucket
# ============================================================

def list_property_archives(property_name: str) -> list[dict[str, Any]]:
    """
    Every ZIP stored under a property's prefix, newest first.

    Because the prefix is per-property, this physically cannot return
    another property's archives.
    """
    client = get_s3_client()
    bucket = get_bucket()
    prefix = f"properties/{sanitize_name(property_name)}/"

    archives: list[dict[str, Any]] = []
    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]

            if not key.lower().endswith(".zip"):
                continue
            # Never offer our own processed output as an importable archive.
            if "/processed/" in key:
                continue

            archives.append(
                {
                    "key": key,
                    "filename": key.rsplit("/", 1)[-1],
                    "size_bytes": int(item["Size"]),
                    "last_modified": item["LastModified"],
                }
            )

    archives.sort(key=lambda entry: entry["last_modified"], reverse=True)
    return archives


def list_known_properties() -> list[str]:
    """
    Property prefixes that exist in the bucket, so the picker can offer
    archives for properties that have never been imported yet.
    """
    client = get_s3_client()
    bucket = get_bucket()

    response = client.list_objects_v2(
        Bucket=bucket, Prefix="properties/", Delimiter="/"
    )

    return sorted(
        item["Prefix"].split("/")[1]
        for item in response.get("CommonPrefixes", [])
    )


# ============================================================
# Stage 1: messages
# ============================================================

def import_archive_messages(
    connection: sqlite3.Connection,
    object_key: str,
    property_name: str,
) -> dict[str, Any]:
    """
    Read the transcript out of an S3 archive and store its messages.

    Safe to run twice: every message carries a unique hash, so a repeated
    import reports duplicates and changes nothing.
    """
    cleaned_property_name = property_name.strip()
    if not cleaned_property_name:
        raise ValueError("Choose a property before importing.")

    client = get_s3_client()
    bucket = get_bucket()

    archive, reader = open_s3_zip(client, bucket, object_key)

    try:
        chat_member = find_chat_member(archive)
        chat_text = read_chat_text(archive, chat_member)
        attachment_index = build_attachment_index(archive)
    finally:
        archive.close()

    messages = parse_whatsapp_chat_text(chat_text, source_file=chat_member)

    if not messages:
        raise ValueError(
            "The transcript was read successfully but contained no messages "
            "in a recognizable WhatsApp format."
        )

    # Point every message back at the archive it came from, so the media
    # stage can reopen exactly the right ZIP without guessing.
    for message in messages:
        message.source_file = f"{SOURCE_PREFIX}{object_key}"

    initialize_database(connection)
    results = insert_messages(
        connection=connection,
        messages=messages,
        property_name=cleaned_property_name,
        group_name=cleaned_property_name,
    )

    attachments_referenced = sum(
        1 for message in messages if message.attachment_filename
    )
    attachments_present = sum(
        1
        for message in messages
        if message.attachment_filename
        and message.attachment_filename.lower() in attachment_index
    )

    return {
        "property_name": cleaned_property_name,
        "object_key": object_key,
        "archive_size_bytes": reader.size,
        "bytes_read": reader.bytes_fetched,
        "range_requests": reader.range_requests,
        "parsed": len(messages),
        "imported": results["imported"],
        "duplicates": results["duplicates"],
        "database_errors": results["errors"],
        "attachments_referenced": attachments_referenced,
        "attachments_present": attachments_present,
        "attachments_missing": attachments_referenced - attachments_present,
    }


# ============================================================
# Stage 2: media
# ============================================================

def process_media_batch(
    connection: sqlite3.Connection,
    limit: int = 250,
    progress_callback: Callable[[int, int], None] | None = None,
) -> dict[str, int]:
    """
    Copy the next slice of pending attachments from their archives into S3.

    Each file is streamed straight from the ZIP to the bucket in bounded
    chunks, so peak memory stays at one chunk and nothing touches disk.
    """
    rows = pending_media_batch(connection, limit=limit)

    if not rows:
        return {"copied": 0, "missing": 0, "errors": 0, "skipped": 0}

    client = get_s3_client()
    bucket = get_bucket()

    copied = 0
    missing = 0
    errors = 0
    skipped = 0

    # Group by archive so each ZIP's index is read once, not once per photo.
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(str(row["source_file"]), []).append(row)

    processed = 0
    total = len(rows)

    for source_file, group in grouped.items():
        if not source_file.startswith(SOURCE_PREFIX):
            # Imported through the old local path; leave it for media_manager.
            skipped += len(group)
            processed += len(group)
            continue

        object_key = source_file[len(SOURCE_PREFIX):]

        try:
            archive, _ = open_s3_zip(client, bucket, object_key)
        except Exception:
            errors += len(group)
            processed += len(group)
            continue

        try:
            index = build_attachment_index(archive)

            for row in group:
                processed += 1
                if progress_callback:
                    progress_callback(processed, total)

                attachment_filename = str(row["attachment_filename"])
                member = index.get(attachment_filename.lower())

                if member is None:
                    # Referenced in the transcript but not included in the
                    # export. Mark it so it is not retried forever.
                    set_media_path(connection, int(row["id"]), MISSING_MEDIA)
                    missing += 1
                    continue

                try:
                    key = build_media_key(
                        property_name=str(row["property_name"]),
                        message_date=str(row["message_date"]),
                        media_type=str(row["media_type"]),
                        attachment_filename=attachment_filename,
                    )

                    upload_media_stream(
                        key=key,
                        chunks=stream_member(archive, member),
                        filename=attachment_filename,
                        s3_client=client,
                        bucket=bucket,
                    )

                    set_media_path(connection, int(row["id"]), media_uri(key))
                    copied += 1

                except Exception as error:
                    errors += 1
                    print(f"Media error for {attachment_filename}: {error}")

            connection.commit()

        finally:
            archive.close()

    connection.commit()

    return {
        "copied": copied,
        "missing": missing,
        "errors": errors,
        "skipped": skipped,
    }
