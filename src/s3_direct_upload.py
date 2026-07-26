from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
        region_name=st.secrets["AWS_REGION"],
    )


def sanitize_name(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "-", value.strip())
    return cleaned.strip("-").lower() or "unknown-property"


def build_object_key(property_name: str, filename: str) -> str:
    safe_property = sanitize_name(property_name)
    safe_filename = Path(filename).name.replace(" ", "_")
    date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    unique_id = uuid.uuid4().hex
    return (
        f"properties/{safe_property}/original-uploads/"
        f"{date_path}/{unique_id}_{safe_filename}"
    )


def create_direct_upload_url(
    property_name: str,
    filename: str,
    content_type: str = "application/zip",
    expires_in: int = 3600,
) -> dict[str, str]:
    bucket_name = st.secrets["S3_BUCKET"]
    object_key = build_object_key(property_name, filename)

    try:
        upload_url = get_s3_client().generate_presigned_url(
            ClientMethod="put_object",
            Params={
                "Bucket": bucket_name,
                "Key": object_key,
                "ContentType": content_type,
                "ServerSideEncryption": "AES256",
            },
            ExpiresIn=expires_in,
        )
        return {"upload_url": upload_url, "object_key": object_key}
    except (ClientError, BotoCoreError) as exc:
        raise RuntimeError(
            f"Could not create the secure AWS upload link: {exc}"
        ) from exc


def verify_uploaded_object(object_key: str) -> dict[str, object]:
    bucket_name = st.secrets["S3_BUCKET"]

    try:
        response = get_s3_client().head_object(
            Bucket=bucket_name,
            Key=object_key,
        )
        return {
            "object_key": object_key,
            "size_bytes": int(response["ContentLength"]),
            "content_type": response.get("ContentType", ""),
            "etag": str(response.get("ETag", "")).strip('"'),
        }
    except (ClientError, BotoCoreError) as exc:
        raise RuntimeError(
            f"AWS could not verify the uploaded file: {exc}"
        ) from exc


def download_s3_object(
    object_key: str,
    destination_path: Path,
) -> None:
    bucket_name = st.secrets["S3_BUCKET"]
    destination_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        get_s3_client().download_file(
            Bucket=bucket_name,
            Key=object_key,
            Filename=str(destination_path),
        )
    except (ClientError, BotoCoreError, OSError) as exc:
        raise RuntimeError(
            f"Could not download the verified S3 archive: {exc}"
        ) from exc
