from __future__ import annotations

import html
import re
import sqlite3
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

import streamlit as st
import streamlit.components.v1 as components


# ============================================================
# Project paths and imports
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent
DATABASE_PATH = PROJECT_ROOT / "database" / "operations_archive.db"
MEDIA_ROOT = PROJECT_ROOT / "media"
INCOMING_ROOT = PROJECT_ROOT / "incoming"
SRC_DIRECTORY = PROJECT_ROOT / "src"

if str(SRC_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(SRC_DIRECTORY))

from database import (  # noqa: E402
    connect_database as connect_archive_database,
    initialize_database,
    insert_messages,
)
from media_manager import organize_media  # noqa: E402
from parser import parse_whatsapp_chat  # noqa: E402
from s3_direct_upload import (  # noqa: E402
    create_direct_upload_url,
    download_s3_object,
    verify_uploaded_object,
)


# ============================================================
# Streamlit configuration
# ============================================================

st.set_page_config(
    page_title="Waves Property Operations Archive",
    page_icon="📁",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ============================================================
# Constants
# ============================================================

MONTH_NAMES = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}


# ============================================================
# Database reading functions
# ============================================================

def connect_view_database() -> sqlite3.Connection:
    """
    Open the permanent SQLite archive for reading.
    """
    connection = sqlite3.connect(DATABASE_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def get_properties(search_text: str = "") -> list[str]:
    """
    Return all property names matching the optional search text.
    """
    connection = connect_view_database()

    try:
        if search_text.strip():
            rows = connection.execute(
                """
                SELECT DISTINCT property_name
                FROM messages
                WHERE property_name LIKE ?
                ORDER BY property_name
                """,
                (f"%{search_text.strip()}%",),
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT DISTINCT property_name
                FROM messages
                ORDER BY property_name
                """
            ).fetchall()

        return [str(row["property_name"]) for row in rows]

    finally:
        connection.close()


def get_years(property_name: str) -> list[int]:
    """
    Return all archived years for a property.
    """
    connection = connect_view_database()

    try:
        rows = connection.execute(
            """
            SELECT DISTINCT substr(message_date, 1, 4) AS year
            FROM messages
            WHERE property_name = ?
            ORDER BY year DESC
            """,
            (property_name,),
        ).fetchall()

        return [int(row["year"]) for row in rows if row["year"]]

    finally:
        connection.close()


def get_months(
    property_name: str,
    year: int,
) -> list[int]:
    """
    Return all archived months for a selected property and year.
    """
    connection = connect_view_database()

    try:
        rows = connection.execute(
            """
            SELECT DISTINCT substr(message_date, 6, 2) AS month
            FROM messages
            WHERE property_name = ?
              AND substr(message_date, 1, 4) = ?
            ORDER BY month
            """,
            (property_name, str(year)),
        ).fetchall()

        return [int(row["month"]) for row in rows if row["month"]]

    finally:
        connection.close()


def get_dates(
    property_name: str,
    year: int,
    month: int,
) -> list[str]:
    """
    Return all available dates for a property, year and month.
    """
    connection = connect_view_database()

    try:
        year_month = f"{year:04d}-{month:02d}"

        rows = connection.execute(
            """
            SELECT DISTINCT message_date
            FROM messages
            WHERE property_name = ?
              AND substr(message_date, 1, 7) = ?
            ORDER BY message_date DESC
            """,
            (property_name, year_month),
        ).fetchall()

        return [str(row["message_date"]) for row in rows]

    finally:
        connection.close()


def get_messages(
    property_name: str,
    selected_date: str,
    search_text: str = "",
) -> list[sqlite3.Row]:
    """
    Return messages for a selected property and date.
    """
    connection = connect_view_database()

    try:
        if search_text.strip():
            search_pattern = f"%{search_text.strip()}%"

            rows = connection.execute(
                """
                SELECT *
                FROM messages
                WHERE property_name = ?
                  AND message_date = ?
                  AND (
                        message_text LIKE ?
                        OR sender LIKE ?
                        OR attachment_filename LIKE ?
                      )
                ORDER BY timestamp ASC, id ASC
                """,
                (
                    property_name,
                    selected_date,
                    search_pattern,
                    search_pattern,
                    search_pattern,
                ),
            ).fetchall()

        else:
            rows = connection.execute(
                """
                SELECT *
                FROM messages
                WHERE property_name = ?
                  AND message_date = ?
                ORDER BY timestamp ASC, id ASC
                """,
                (property_name, selected_date),
            ).fetchall()

        return rows

    finally:
        connection.close()


def get_daily_summary(
    property_name: str,
    selected_date: str,
) -> sqlite3.Row:
    """
    Return summary statistics for one property and date.
    """
    connection = connect_view_database()

    try:
        return connection.execute(
            """
            SELECT
                COUNT(*) AS message_count,

                COUNT(
                    DISTINCT CASE
                        WHEN sender IS NOT NULL
                             AND sender != ''
                        THEN sender
                    END
                ) AS employee_count,

                GROUP_CONCAT(
                    DISTINCT CASE
                        WHEN sender IS NOT NULL
                             AND sender != ''
                        THEN sender
                    END
                ) AS employees,

                SUM(
                    CASE
                        WHEN media_type = 'photo'
                        THEN 1
                        ELSE 0
                    END
                ) AS photo_count,

                SUM(
                    CASE
                        WHEN media_type = 'video'
                        THEN 1
                        ELSE 0
                    END
                ) AS video_count,

                MIN(message_time) AS opening_activity,
                MAX(message_time) AS last_activity

            FROM messages
            WHERE property_name = ?
              AND message_date = ?
            """,
            (property_name, selected_date),
        ).fetchone()

    finally:
        connection.close()


# ============================================================
# Formatting functions
# ============================================================

def format_full_date(date_text: str) -> str:
    """
    Convert 2026-07-15 to Wednesday, July 15, 2026.
    """
    parsed_date = datetime.strptime(date_text, "%Y-%m-%d")
    return parsed_date.strftime("%A, %B %d, %Y")


def format_time(time_text: str | None) -> str:
    """
    Convert 18:10:46 to 6:10 PM.
    """
    if not time_text:
        return "Not available"

    try:
        parsed_time = datetime.strptime(time_text, "%H:%M:%S")
        return parsed_time.strftime("%I:%M %p").lstrip("0")
    except ValueError:
        return time_text


def safe_count(value: Any) -> int:
    """
    Safely convert a SQLite aggregate value to an integer.
    """
    return int(value or 0)


def clean_employee_names(employee_text: str | None) -> str:
    """
    Return readable employee names from SQLite GROUP_CONCAT output.
    """
    if not employee_text:
        return "No employee names recorded"

    names = [
        name.strip()
        for name in employee_text.split(",")
        if name.strip()
    ]

    return ", ".join(names)


def sanitize_folder_name(value: str) -> str:
    """
    Convert a property name into a safe folder name.

    Example:
        Opal Grand Resort -> Opal_Grand_Resort
    """
    cleaned = re.sub(
        r"[^A-Za-z0-9 _-]+",
        "",
        value.strip(),
    )

    cleaned = re.sub(r"\s+", "_", cleaned)

    return cleaned or "Unknown_Property"


# ============================================================
# ZIP upload and import functions
# ============================================================

def safely_extract_zip(
    zip_path: Path,
    destination_directory: Path,
) -> None:
    """
    Extract a ZIP file while preventing unsafe file paths.
    """
    destination_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    destination_root = destination_directory.resolve()

    with zipfile.ZipFile(zip_path, "r") as archive:
        for member in archive.infolist():
            destination_path = (
                destination_directory / member.filename
            ).resolve()

            if (
                destination_path != destination_root
                and destination_root not in destination_path.parents
            ):
                raise ValueError(
                    f"Unsafe ZIP entry detected: {member.filename}"
                )

        archive.extractall(destination_directory)


def find_chat_file(extracted_directory: Path) -> Path:
    """
    Find the WhatsApp _chat.txt file inside an extracted ZIP.
    """
    exact_matches = list(
        extracted_directory.rglob("_chat.txt")
    )

    if exact_matches:
        return exact_matches[0]

    text_files = list(
        extracted_directory.rglob("*.txt")
    )

    if len(text_files) == 1:
        return text_files[0]

    raise FileNotFoundError(
        "The uploaded ZIP does not contain a recognizable "
        "WhatsApp _chat.txt file."
    )


def import_s3_archive(
    object_key: str,
    property_name: str,
) -> dict[str, Any]:
    """Download a verified S3 ZIP and import its WhatsApp contents."""
    cleaned_property_name = property_name.strip()
    if not cleaned_property_name:
        raise ValueError("Enter a property name before importing.")

    verification = verify_uploaded_object(object_key)
    stored_size = int(verification["size_bytes"])
    if stored_size <= 0:
        raise RuntimeError("The S3 object exists but is empty.")

    timestamp_label = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_property_name = sanitize_folder_name(cleaned_property_name)
    import_directory = INCOMING_ROOT / safe_property_name / timestamp_label
    original_directory = import_directory / "original"
    extracted_directory = import_directory / "extracted"
    original_directory.mkdir(parents=True, exist_ok=True)

    zip_path = original_directory / Path(object_key).name

    download_s3_object(
        object_key=object_key,
        destination_path=zip_path,
    )

    if zip_path.stat().st_size != stored_size:
        raise RuntimeError(
            "The downloaded ZIP size does not match the verified S3 size."
        )

    safely_extract_zip(zip_path, extracted_directory)
    chat_file = find_chat_file(extracted_directory)
    messages = parse_whatsapp_chat(chat_file)

    if not messages:
        raise ValueError("No WhatsApp messages were found in the export.")

    archive_connection = connect_archive_database(DATABASE_PATH)
    try:
        initialize_database(archive_connection)
        import_results = insert_messages(
            connection=archive_connection,
            messages=messages,
            property_name=cleaned_property_name,
            group_name=cleaned_property_name,
        )
        media_results = organize_media(
            connection=archive_connection,
            media_root=MEDIA_ROOT,
        )
    finally:
        archive_connection.close()

    return {
        "property_name": cleaned_property_name,
        "parsed": len(messages),
        "imported": import_results["imported"],
        "duplicates": import_results["duplicates"],
        "database_errors": import_results["errors"],
        "media_copied": media_results["copied"],
        "media_reused": media_results["reused"],
        "media_missing": media_results["missing"],
        "media_errors": media_results["errors"],
        "import_directory": str(import_directory),
        "s3_object_key": object_key,
        "s3_size_bytes": stored_size,
        "s3_etag": verification["etag"],
    }


def render_direct_s3_uploader(upload_url: str, object_key: str) -> None:
    """Render a browser uploader that sends a ZIP directly to S3."""
    safe_url = html.escape(upload_url, quote=True)
    safe_key = html.escape(object_key, quote=True)

    components.html(
        f"""
        <div style="font-family:Arial,sans-serif;border:1px solid #d1d5db;
                    border-radius:10px;padding:16px;">
          <input id="archive-file" type="file"
                 accept=".zip,application/zip"
                 style="width:100%;margin-bottom:12px;" />
          <button id="upload-button"
                  style="width:100%;padding:10px 14px;border:0;border-radius:8px;
                         background:#ff4b4b;color:white;font-weight:600;
                         cursor:pointer;">
            Upload directly to AWS S3
          </button>
          <div style="margin-top:12px;width:100%;background:#e5e7eb;
                      border-radius:999px;overflow:hidden;height:14px;">
            <div id="progress-bar"
                 style="width:0%;height:100%;background:#16a34a;
                        transition:width .2s;"></div>
          </div>
          <div id="status" style="margin-top:10px;"></div>
          <div style="margin-top:10px;font-size:12px;color:#6b7280;
                      overflow-wrap:anywhere;">
            AWS object key: {safe_key}
          </div>
        </div>
        <script>
        const input = document.getElementById("archive-file");
        const button = document.getElementById("upload-button");
        const status = document.getElementById("status");
        const bar = document.getElementById("progress-bar");

        button.addEventListener("click", () => {{
          const file = input.files[0];
          if (!file) {{
            status.textContent = "Select a ZIP file first.";
            status.style.color = "#dc2626";
            return;
          }}
          if (!file.name.toLowerCase().endsWith(".zip")) {{
            status.textContent = "Only ZIP files are allowed.";
            status.style.color = "#dc2626";
            return;
          }}

          button.disabled = true;
          status.textContent = "Uploading directly to AWS...";
          status.style.color = "#374151";
          bar.style.width = "0%";

          const request = new XMLHttpRequest();
          request.open("PUT", "{safe_url}", true);
          request.setRequestHeader("Content-Type", "application/zip");
          request.setRequestHeader("x-amz-server-side-encryption", "AES256");

          request.upload.onprogress = (event) => {{
            if (event.lengthComputable) {{
              const percent = Math.round((event.loaded / event.total) * 100);
              bar.style.width = percent + "%";
              status.textContent = "Uploading directly to AWS: " + percent + "%";
            }}
          }};

          request.onload = () => {{
            button.disabled = false;
            if (request.status >= 200 && request.status < 300) {{
              bar.style.width = "100%";
              status.textContent =
                "Upload complete. Click 'Verify and import from AWS' below.";
              status.style.color = "#15803d";
            }} else {{
              status.textContent =
                "Upload failed. AWS returned status " + request.status + ".";
              status.style.color = "#dc2626";
            }}
          }};

          request.onerror = () => {{
            button.disabled = false;
            status.textContent =
              "Upload failed because of a network or CORS error.";
            status.style.color = "#dc2626";
          }};

          request.send(file);
        }});
        </script>
        """,
        height=300,
        scrolling=False,
    )


# ============================================================
# Navigation state
# ============================================================

def initialize_navigation() -> None:
    defaults = {
        "page": "properties",
        "selected_property": None,
        "selected_year": None,
        "selected_month": None,
        "selected_date": None,
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def go_to_properties() -> None:
    st.session_state.page = "properties"
    st.session_state.selected_property = None
    st.session_state.selected_year = None
    st.session_state.selected_month = None
    st.session_state.selected_date = None


def go_to_years(property_name: str) -> None:
    st.session_state.page = "years"
    st.session_state.selected_property = property_name
    st.session_state.selected_year = None
    st.session_state.selected_month = None
    st.session_state.selected_date = None


def go_to_months(year: int) -> None:
    st.session_state.page = "months"
    st.session_state.selected_year = year
    st.session_state.selected_month = None
    st.session_state.selected_date = None


def go_to_dates(month: int) -> None:
    st.session_state.page = "dates"
    st.session_state.selected_month = month
    st.session_state.selected_date = None


def go_to_timeline(date_text: str) -> None:
    st.session_state.page = "timeline"
    st.session_state.selected_date = date_text


# ============================================================
# General interface helpers
# ============================================================

def show_breadcrumbs() -> None:
    parts = ["Properties"]

    if st.session_state.selected_property:
        parts.append(st.session_state.selected_property)

    if st.session_state.selected_year:
        parts.append(str(st.session_state.selected_year))

    if st.session_state.selected_month:
        parts.append(
            MONTH_NAMES.get(
                st.session_state.selected_month,
                str(st.session_state.selected_month),
            )
        )

    if st.session_state.selected_date:
        parts.append(
            format_full_date(st.session_state.selected_date)
        )

    st.caption("  ›  ".join(parts))


def show_back_button(target_page: str) -> None:
    if st.button("← Back"):
        st.session_state.page = target_page
        st.rerun()


def display_media(message: sqlite3.Row) -> None:
    """
    Display archived media attached to a message.
    """
    media_path = message["media_path"]
    media_type = message["media_type"]
    attachment_filename = message["attachment_filename"]

    if not attachment_filename:
        return

    if not media_path:
        st.warning(
            f"Attachment unavailable: {attachment_filename}"
        )
        return

    file_path = Path(media_path)

    if not file_path.exists():
        st.warning(
            f"Archived file not found: {attachment_filename}"
        )
        return

    if media_type == "photo":
        with st.expander("📷 View photo"):
            st.image(
                str(file_path),
                caption=attachment_filename,
                use_container_width=True,
            )

    elif media_type == "video":
        with st.expander("🎥 View video"):
            st.video(str(file_path))

    elif media_type == "audio":
        with st.expander("🔊 Play audio"):
            st.audio(str(file_path))

    elif media_type == "document":
        st.markdown(
            f"📄 **Document:** `{attachment_filename}`"
        )

    else:
        st.markdown(
            f"📎 **Attachment:** `{attachment_filename}`"
        )


def display_message(message: sqlite3.Row) -> None:
    """
    Display one message in the chronological daily timeline.
    """
    sender = message["sender"] or "System update"
    message_text = message["message_text"] or ""
    time_label = format_time(message["message_time"])

    with st.container(border=True):
        st.markdown(f"### {time_label}")
        st.markdown(f"**{sender}**")

        if message_text:
            st.write(message_text)

        display_media(message)


# ============================================================
# Upload interface
# ============================================================

def show_upload_section() -> None:
    """Display the direct browser-to-S3 upload and import workflow."""
    with st.expander(
        "➕ Import a WhatsApp property archive",
        expanded=False,
    ):
        st.markdown(
            """
The ZIP uploads **directly from the browser to private AWS S3 storage**.

1. Enter the property name and the original ZIP filename.
2. Create a temporary secure AWS upload link.
3. Select the ZIP and upload it directly to AWS.
4. Click **Verify and import from AWS**.
"""
        )

        columns = st.columns(2)
        with columns[0]:
            property_name = st.text_input(
                "Property name",
                placeholder="Example: Opal Grand",
                key="direct_property_name",
            )
        with columns[1]:
            filename = st.text_input(
                "Original ZIP filename",
                placeholder="Example: WhatsApp Chat - Opal Grand.zip",
                key="direct_filename",
            )

        if st.button(
            "Create secure AWS upload",
            type="primary",
            use_container_width=True,
        ):
            if not property_name.strip():
                st.error("Enter the property name.")
            elif not filename.strip():
                st.error("Enter the original ZIP filename.")
            elif not filename.lower().endswith(".zip"):
                st.error("The filename must end with .zip.")
            else:
                try:
                    details = create_direct_upload_url(
                        property_name=property_name,
                        filename=filename,
                        content_type="application/zip",
                        expires_in=3600,
                    )
                    st.session_state.direct_upload = {
                        "property_name": property_name.strip(),
                        "filename": filename.strip(),
                        "object_key": details["object_key"],
                        "upload_url": details["upload_url"],
                    }
                    st.success(
                        "Secure AWS upload created. The link expires in one hour."
                    )
                except Exception as error:
                    st.error(f"Could not create the AWS upload: {error}")

        upload_state = st.session_state.get("direct_upload")

        if upload_state:
            st.divider()
            st.subheader("Direct AWS upload")
            render_direct_s3_uploader(
                upload_url=upload_state["upload_url"],
                object_key=upload_state["object_key"],
            )

            st.warning(
                "Wait until the uploader says 'Upload complete' "
                "before clicking the next button."
            )

            if st.button(
                "Verify and import from AWS",
                use_container_width=True,
            ):
                try:
                    with st.spinner(
                        "Verifying AWS backup, downloading and importing..."
                    ):
                        results = import_s3_archive(
                            object_key=upload_state["object_key"],
                            property_name=upload_state["property_name"],
                        )

                    st.success(
                        f"{results['property_name']} was imported successfully."
                    )
                    st.success(
                        "✅ Original ZIP is stored and verified in AWS S3."
                    )
                    st.caption(
                        "AWS backup size: "
                        f"{results['s3_size_bytes'] / (1024 * 1024):.2f} MB"
                    )

                    with st.expander("AWS backup details"):
                        st.code(results["s3_object_key"])
                        st.write(f"ETag: {results['s3_etag']}")

                    metrics = st.columns(4)
                    metrics[0].metric("Messages parsed", results["parsed"])
                    metrics[1].metric("New messages", results["imported"])
                    metrics[2].metric(
                        "Duplicates skipped", results["duplicates"]
                    )
                    metrics[3].metric("Media copied", results["media_copied"])

                    if results["media_reused"]:
                        st.info(
                            f"{results['media_reused']} existing media file(s) "
                            "were reused."
                        )
                    if results["media_missing"]:
                        st.warning(
                            f"{results['media_missing']} referenced attachment(s) "
                            "were not included in the ZIP."
                        )

                    total_errors = (
                        results["database_errors"] + results["media_errors"]
                    )
                    if total_errors:
                        st.error(
                            f"The import completed with {total_errors} error(s)."
                        )
                    else:
                        st.success(
                            "Backup and import verification completed."
                        )

                    st.session_state.pop("direct_upload", None)

                except zipfile.BadZipFile:
                    st.error("The S3 object is not a valid ZIP archive.")
                except Exception as error:
                    st.error(f"Import failed: {error}")

            if st.button(
                "Cancel this upload",
                use_container_width=True,
            ):
                st.session_state.pop("direct_upload", None)
                st.rerun()

        st.caption(
            "Do not delete the WhatsApp data from the phone until the app "
            "confirms both AWS backup verification and successful import."
        )


# ============================================================
# Page: properties
# ============================================================

def show_properties_page() -> None:
    st.title("📁 Waves Property Operations Archive")

    st.caption(
        "Select a property to review its archived daily operations."
    )

    show_upload_section()

    st.divider()

    property_search = st.text_input(
        "Search properties",
        placeholder=(
            "Search Opal Grand, Delray Pool, "
            "H2O Waterpark..."
        ),
        key="property_search",
    )

    properties = get_properties(property_search)

    if not properties:
        st.info("No matching properties were found.")
        return

    columns_per_row = 3

    for start_index in range(
        0,
        len(properties),
        columns_per_row,
    ):
        row_properties = properties[
            start_index:start_index + columns_per_row
        ]

        columns = st.columns(columns_per_row)

        for column, property_name in zip(
            columns,
            row_properties,
        ):
            with column:
                with st.container(border=True):
                    st.markdown("## 📁")
                    st.markdown(f"### {property_name}")
                    st.caption("Open property archive")

                    if st.button(
                        "Open property",
                        key=f"property-{property_name}",
                        use_container_width=True,
                    ):
                        go_to_years(property_name)
                        st.rerun()


# ============================================================
# Page: years
# ============================================================

def show_years_page() -> None:
    show_back_button("properties")
    show_breadcrumbs()

    property_name = st.session_state.selected_property

    st.title(property_name)
    st.subheader("Select a year")

    years = get_years(property_name)

    if not years:
        st.info("No archived years were found.")
        return

    columns = st.columns(4)

    for index, year in enumerate(years):
        with columns[index % 4]:
            with st.container(border=True):
                st.markdown("## 📁")
                st.markdown(f"### {year}")

                if st.button(
                    "Open year",
                    key=f"year-{year}",
                    use_container_width=True,
                ):
                    go_to_months(year)
                    st.rerun()


# ============================================================
# Page: months
# ============================================================

def show_months_page() -> None:
    show_back_button("years")
    show_breadcrumbs()

    property_name = st.session_state.selected_property
    selected_year = st.session_state.selected_year

    st.title(f"{property_name} — {selected_year}")
    st.subheader("Select a month")

    months = get_months(
        property_name=property_name,
        year=selected_year,
    )

    if not months:
        st.info("No archived months were found.")
        return

    columns = st.columns(4)

    for index, month in enumerate(months):
        month_name = MONTH_NAMES.get(month, str(month))

        with columns[index % 4]:
            with st.container(border=True):
                st.markdown("## 📁")
                st.markdown(f"### {month_name}")

                if st.button(
                    "Open month",
                    key=f"month-{month}",
                    use_container_width=True,
                ):
                    go_to_dates(month)
                    st.rerun()


# ============================================================
# Page: dates
# ============================================================

def show_dates_page() -> None:
    show_back_button("months")
    show_breadcrumbs()

    property_name = st.session_state.selected_property
    selected_year = st.session_state.selected_year
    selected_month = st.session_state.selected_month

    month_name = MONTH_NAMES.get(
        selected_month,
        str(selected_month),
    )

    st.title(
        f"{property_name} — {month_name} {selected_year}"
    )

    st.subheader("Select a date")

    date_search = st.text_input(
        "Search dates",
        placeholder="Example: 2024-11-16 or November 16",
    )

    dates = get_dates(
        property_name=property_name,
        year=selected_year,
        month=selected_month,
    )

    if date_search.strip():
        search_value = date_search.strip().lower()

        dates = [
            date_text
            for date_text in dates
            if (
                search_value in date_text.lower()
                or search_value
                in format_full_date(date_text).lower()
            )
        ]

    if not dates:
        st.info("No matching dates were found.")
        return

    columns = st.columns(4)

    for index, date_text in enumerate(dates):
        parsed_date = datetime.strptime(
            date_text,
            "%Y-%m-%d",
        )

        with columns[index % 4]:
            with st.container(border=True):
                st.markdown(
                    f"### {parsed_date.strftime('%B %d')}"
                )

                st.caption(
                    parsed_date.strftime("%A")
                )

                if st.button(
                    "View daily timeline",
                    key=f"date-{date_text}",
                    use_container_width=True,
                ):
                    go_to_timeline(date_text)
                    st.rerun()


# ============================================================
# Page: daily timeline
# ============================================================

def show_timeline_page() -> None:
    show_back_button("dates")
    show_breadcrumbs()

    property_name = st.session_state.selected_property
    selected_date = st.session_state.selected_date

    summary = get_daily_summary(
        property_name=property_name,
        selected_date=selected_date,
    )

    st.title(property_name)
    st.subheader(format_full_date(selected_date))

    employees = clean_employee_names(
        summary["employees"]
    )

    st.markdown(
        f"""
**Opening activity:** {format_time(summary["opening_activity"])}  
**Last recorded activity:** {format_time(summary["last_activity"])}  
**Employees active:** {employees}  
**Photos:** {safe_count(summary["photo_count"])}  
**Videos:** {safe_count(summary["video_count"])}  
**Messages:** {safe_count(summary["message_count"])}
"""
    )

    st.divider()

    st.header("Daily Timeline")

    message_search = st.text_input(
        "Search this day",
        placeholder=(
            "Search employee, message or attachment"
        ),
    )

    messages = get_messages(
        property_name=property_name,
        selected_date=selected_date,
        search_text=message_search,
    )

    if not messages:
        st.info(
            "No messages matched the selected date "
            "and search."
        )
        return

    for message in messages:
        display_message(message)


# ============================================================
# Main application
# ============================================================

def main() -> None:
    initialize_navigation()

    DATABASE_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    MEDIA_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    INCOMING_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    if not DATABASE_PATH.exists():
        connection = connect_archive_database(
            DATABASE_PATH
        )

        try:
            initialize_database(connection)
        finally:
            connection.close()

    page = st.session_state.page

    if page == "properties":
        show_properties_page()

    elif page == "years":
        show_years_page()

    elif page == "months":
        show_months_page()

    elif page == "dates":
        show_dates_page()

    elif page == "timeline":
        show_timeline_page()

    else:
        go_to_properties()
        st.rerun()


if __name__ == "__main__":
    main()