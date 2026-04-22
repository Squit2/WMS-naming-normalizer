"""
app.py
======
WMS Order File Converter — Streamlit Web Interface

Changes
-------
Issue 4 — Store CSV bytes in session state instead of DataFrames.
    DataFrames are reconstructed from bytes only when the display tabs render.
    Eliminates multi-MB pickle/unpickle on every widget interaction.

Issue 5 — Cache df_raw in session state; skip second file read on Convert.
    The file is read once on upload to detect unmapped columns. The resulting
    df_raw is stored in session state keyed by file fingerprint. The Convert
    button reuses the cached df_raw instead of writing a second temp file
    and running read_order_file() again.

Issue 8 — Prevent duplicate WMS uploads.
    upload_done is stored in session state after a successful upload.
    The upload button is replaced with a success message for the remainder
    of the session to prevent the same file being imported to the WMS twice.

Requires SFTP credentials in .streamlit/secrets.toml:
    SFTP_HOST         = "your-sftp-host"
    SFTP_USERNAME     = "your-username"
    SFTP_PASSWORD     = "your-password"
    SFTP_PORT         = 22
    SFTP_MAPPINGS_DIR = "/mappings"
    SFTP_TARGET_DIR   = "/wms-import"

Run with:
    streamlit run app.py
"""

import os
import re
import hashlib
import tempfile
from io import BytesIO
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd

from converter import (
    read_order_file,
    apply_mapping,
    validate,
    clean_data,
    is_valid_xlsx,
    is_valid_pdf,
    ordered_wms_columns,
    ALL_WMS_FIELDS,
    MANDATORY_FIELDS,
)

from sftp_config_store import (
    is_sftp_configured,
    fetch_all_raw_configs,
    validate_all_sftp_configs,
    list_sftp_customers,
    parse_config_from_csv_string,
    upload_config_csv,
    merge_and_save_mappings,
)

from sftp_uploader import upload_to_wms

for _key in ("SFTP_HOST", "SFTP_PORT", "SFTP_USER", "SFTP_PASSWORD",
             "SFTP_PRIVATE_KEY", "SFTP_TARGET_DIR"):
    try:
        if _key in st.secrets and _key not in os.environ:
            os.environ[_key] = str(st.secrets[_key])
    except Exception:
        pass

MAX_CONFIG_BYTES = 1 * 1024 * 1024

# ─── PAGE CONFIG ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="WMS Order Converter",
    page_icon="📦",
    layout="wide",
)

st.title("📦 WMS Order File Converter")
st.caption("Convert customer order files into WMS-ready CSV format.")

# ─── PASSWORD PROTECTION ─────────────────────────────────────────────────────
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if not st.session_state["authenticated"]:
    pwd = st.text_input("Enter app password:", type="password")
    if pwd == st.secrets["APP_PASSWORD"]:
        st.session_state["authenticated"] = True
        st.rerun()
    elif pwd:
        st.error("Incorrect password.")
    st.stop()  # This entirely hides the rest of the app until unlocked

# ─── GUARD ───────────────────────────────────────────────────────────────────

if not is_sftp_configured():
    st.error(
        "**SFTP credentials are not configured.**  \n"
        "Add the following keys to `.streamlit/secrets.toml` and restart:  \n"
        "```\n"
        "SFTP_HOST     = \"your-sftp-host\"\n"
        "SFTP_USERNAME = \"your-username\"\n"
        "SFTP_PASSWORD = \"your-password\"\n"
        "```"
    )
    st.stop()

# ─── SESSION STATE ───────────────────────────────────────────────────────────

for _k, _v in [
    ("export_payload", None),
    ("proc_result",    None),
    ("proc_file_key",  None),
    ("raw_df_key",     None),   # Issue 5: key for cached df_raw
    ("raw_df_cache",   None),   # Issue 5: the cached df_raw itself
    ("saved_mapping",  {}),
    ("upload_done",    False),  # Issue 8: tracks whether WMS upload completed
]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def sanitise_config_key(raw_name: str) -> str:
    stem = Path(raw_name).stem
    return re.sub(r"[^\w\-]", "_", stem)


def _file_key(file_bytes: bytes, customer_key: str) -> str:
    digest = hashlib.md5(file_bytes, usedforsecurity=False).hexdigest()[:12]
    return "{}_{}".format(digest, customer_key)


# ─── LOAD CONFIGS ────────────────────────────────────────────────────────────

try:
    raw_configs        = fetch_all_raw_configs()
    all_config_reports = validate_all_sftp_configs(raw_configs)
    customers          = list_sftp_customers(raw_configs)
except (RuntimeError, ImportError) as _sftp_err:
    st.error(
        "**SFTP connection failed.**  \n"
        "Credentials were found but the server could not be reached.  \n"
        "Error: `{}`".format(_sftp_err)
    )
    st.stop()

invalid_configs = sorted(
    key for key, r in all_config_reports.items() if r["errors"]
)

# ─── SIDEBAR ─────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Settings")
    st.caption("☁ SFTP mode")

    st.subheader("Upload config")

    with st.form("config_upload_form", clear_on_submit=True):
        uploaded_config = st.file_uploader(
            "Drop a customer config here (.csv or .xlsx)",
            type=["csv", "xlsx"],
            help=(
                "Config must have columns: customer_column, wms_field.  \n"
                "Optional: customer_name, date_format.  \n"
                "The filename stem becomes the customer key. Max size: 1 MB."
            ),
        )
        submit_config = st.form_submit_button("Save Config")

    if submit_config and uploaded_config is not None:
        config_bytes = uploaded_config.getvalue()
        ext          = Path(uploaded_config.name).suffix.lower()

        if len(config_bytes) > MAX_CONFIG_BYTES:
            st.error(
                "Config file is too large ({} KB). Maximum is 1 MB.".format(
                    len(config_bytes) // 1024
                )
            )
        else:
            safe_key = sanitise_config_key(uploaded_config.name)

            if safe_key == "template":
                st.error("Cannot overwrite the template. Rename your config and re-upload.")
            else:
                try:
                    if ext == ".csv":
                        csv_string = config_bytes.decode("utf-8-sig")
                    else:
                        df_tmp     = pd.read_excel(BytesIO(config_bytes), dtype=str)
                        csv_string = df_tmp.to_csv(index=False)

                    parse_config_from_csv_string(csv_string, safe_key)

                    result = upload_config_csv(safe_key, csv_string)
                    if result["success"]:
                        st.success("Config saved to SFTP: **{}**".format(safe_key))
                        fetch_all_raw_configs.clear()
                        st.rerun()
                    else:
                        st.error(result["message"])

                except ValueError as _ve:
                    st.error(
                        "Invalid config — not saved.  \n`{}`".format(_ve)
                    )
                except Exception as _ue:
                    st.error("Unexpected error: `{}`".format(_ue))

    st.divider()

    if not customers:
        st.error(
            "No customer configs found on SFTP (`/mappings/*.csv`).  \n"
            "Upload a config file above to get started."
        )
        st.stop()

    if invalid_configs:
        st.warning("{} invalid config(s) detected.".format(len(invalid_configs)))

    if st.button("Validate all configs"):
        for key in customers:
            report = all_config_reports.get(key, {"errors": [], "warnings": []})
            if report["errors"]:
                st.error("{}: {}".format(key, " | ".join(report["errors"])))
            elif report["warnings"]:
                st.warning("{}: {}".format(key, " | ".join(report["warnings"])))
            else:
                st.success("{}: OK".format(key))

    customer_key = st.selectbox(
        "Customer",
        options=customers,
        help="Select which customer's mapping config to use.",
        format_func=lambda k: "{} {}".format(
            k, "(⚠ invalid)" if k in invalid_configs else ""
        ),
    )

    selected_report = all_config_reports.get(
        customer_key, {"errors": [], "warnings": [], "config": None}
    )
    config_is_valid = len(selected_report["errors"]) == 0

    if config_is_valid:
        config = selected_report["config"]
        st.success("Config loaded: **{}**".format(config["customer_name"]))
    else:
        config = None
        st.error("Selected config is invalid and cannot be used.")
        for err in selected_report["errors"]:
            st.error(err)

    st.subheader("Config health")
    if selected_report["warnings"]:
        for warn in selected_report["warnings"]:
            st.warning(warn)
    elif config_is_valid:
        st.success("No config warnings detected.")

    st.divider()

    st.subheader("Column mapping preview")
    if config:
        mapping_df = pd.DataFrame(
            list(config["column_map"].items()),
            columns=["Customer column", "WMS field"],
        )
        mapping_df["Mandatory"] = mapping_df["WMS field"].apply(
            lambda f: "✓" if f in MANDATORY_FIELDS else ""
        )
        st.dataframe(mapping_df, hide_index=True, use_container_width=True)
    else:
        st.info("Mapping preview unavailable for invalid config.")


# ─── MAIN: FILE UPLOAD ───────────────────────────────────────────────────────

st.subheader("1. Upload customer order file")

uploaded_file = st.file_uploader(
    "Upload order file (.xlsx, .xls or .pdf)",
    type=["xlsx", "xls", "pdf"],
    help=(
        "The customer's raw order file. Column names do not need to match — "
        "the mapping config handles translation."
    ),
    disabled=not config_is_valid,
)

sheet_input = st.text_input(
    "Sheet name or index (Excel only)",
    value="0",
    help="Enter 0 for the first sheet, 1 for the second, or the exact sheet name.",
    disabled=not config_is_valid,
)
try:
    sheet_name = int(sheet_input)
except ValueError:
    sheet_name = sheet_input

page_number = 0
if uploaded_file and Path(uploaded_file.name).suffix.lower() == ".pdf":
    page_input = st.number_input(
        "PDF page number (1 = first page)",
        min_value=1, value=1, step=1,
        help="Which page of the PDF contains the order table.",
        disabled=not config_is_valid,
    )
    page_number = int(page_input) - 1


# ─── MAIN: PROCESS ───────────────────────────────────────────────────────────

if uploaded_file and config_is_valid:
    st.subheader("2. Column mapping")

    file_bytes = uploaded_file.read()
    ext        = Path(uploaded_file.name).suffix.lower()

    if ext == ".pdf":
        if not is_valid_pdf(file_bytes):
            st.error("The uploaded file does not appear to be a valid PDF.")
            st.stop()
    else:
        if not is_valid_xlsx(file_bytes):
            st.error("The uploaded file does not appear to be a valid Excel file.")
            st.stop()

    current_file_key = _file_key(file_bytes, customer_key)

    # Invalidate stored results when file or customer changes
    if st.session_state["proc_file_key"] != current_file_key:
        st.session_state["proc_result"]   = None
        st.session_state["proc_file_key"] = current_file_key
        st.session_state["saved_mapping"] = {}
        st.session_state["upload_done"]   = False   # Issue 8: reset on new file
        st.session_state["raw_df_key"]    = None
        st.session_state["raw_df_cache"]  = None

    # ── Issue 5: Read raw file once and cache in session state ────────────────
    # df_raw is only needed to detect unmapped columns for the mapping UI.
    # If the same file+customer key was already read this session, reuse it.
    if st.session_state["raw_df_key"] != current_file_key:
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                tmp.write(file_bytes)
                tmp_path = Path(tmp.name)
            try:
                df_raw = read_order_file(
                    tmp_path,
                    sheet_name=sheet_name,
                    page_number=page_number,
                )
                st.session_state["raw_df_cache"] = df_raw
                st.session_state["raw_df_key"]   = current_file_key
            except (FileNotFoundError, ValueError, ImportError) as exc:
                st.error(str(exc))
                st.stop()
        finally:
            if tmp_path is not None:
                tmp_path.unlink(missing_ok=True)
    else:
        df_raw = st.session_state["raw_df_cache"]

    # ── Detect unmapped columns ───────────────────────────────────────────────
    known_source_cols = {k.lower() for k in config["column_map"]}
    unmapped_cols     = [
        col for col in df_raw.columns
        if col.lower() not in known_source_cols
    ]

    # ── Dynamic mapping UI ────────────────────────────────────────────────────
    user_mapping: dict = {}

    if unmapped_cols:
        st.info(
            "**{} column(s)** in the uploaded file are not in the mapping "
            "config.  \nAssign each to a WMS field, or leave as *Ignore* to "
            "drop it.".format(len(unmapped_cols))
        )
        _COLS_PER_ROW = 3
        for _row_start in range(0, len(unmapped_cols), _COLS_PER_ROW):
            _grid = st.columns(_COLS_PER_ROW)
            for _j, _col in enumerate(
                unmapped_cols[_row_start : _row_start + _COLS_PER_ROW]
            ):
                with _grid[_j]:
                    _selected = st.selectbox(
                        "Map **'{}'** to:".format(_col),
                        ["— Ignore —"] + ALL_WMS_FIELDS,
                        key="umap_{}".format(_col),
                    )
                    if _selected != "— Ignore —":
                        user_mapping[_col] = _selected
    else:
        st.success(
            "All {} column(s) are covered by the mapping config.".format(
                len(df_raw.columns)
            )
        )

    # ── Convert button ────────────────────────────────────────────────────────
    _btn_label = (
        "▶ Convert  ({} new mapping(s) applied)".format(len(user_mapping))
        if user_mapping else "▶ Convert"
    )

    if st.button(_btn_label, type="primary"):
        final_map = {**config["column_map"], **user_mapping}

        # Issue 5: reuse the already-cached df_raw — no second file read needed
        with st.spinner("Mapping and validating…"):
            try:
                df_mapped, warnings         = apply_mapping(
                    df_raw, final_map, case_insensitive_source=True
                )
                df_valid, df_errors, val_errors = validate(df_mapped)
                df_clean, clean_warnings        = clean_data(
                    df_valid, date_format=config.get("date_format")
                )
                warnings = warnings + clean_warnings

            except (FileNotFoundError, ValueError, ImportError) as exc:
                st.error(str(exc))
                st.stop()

        # Issue 4: store CSV bytes, row counts, and metadata — not DataFrames
        cols_ordered = ordered_wms_columns(df_clean)
        csv_bytes    = (
            df_clean[cols_ordered].to_csv(index=False).encode("utf-8-sig")
        )
        err_csv = (
            df_errors.to_csv(index=False).encode("utf-8-sig")
            if not df_errors.empty else None
        )
        raw_csv = df_raw.to_csv(index=False).encode("utf-8-sig")

        st.session_state["proc_result"] = {
            # Issue 4: bytes only — no live DataFrames in session state
            "csv_bytes":        csv_bytes,
            "err_csv":          err_csv,
            "raw_csv":          raw_csv,
            "row_count_raw":    len(df_raw),
            "row_count_valid":  len(df_clean),
            "row_count_errors": len(df_errors),
            "warnings":         warnings,
            "val_errors":       val_errors,
            "user_mapping":     dict(user_mapping),
            # Preserve export metadata
            "cols_ordered":     cols_ordered,
        }
        st.session_state["saved_mapping"] = {}
        st.session_state["upload_done"]   = False  # Issue 8: new result, reset

    # ── Display results ───────────────────────────────────────────────────────
    proc = st.session_state.get("proc_result")

    if proc and st.session_state["proc_file_key"] == current_file_key:

        if proc["warnings"]:
            with st.expander(
                "{} warning(s)".format(len(proc["warnings"])), expanded=True
            ):
                for w in proc["warnings"]:
                    st.warning(w)

        st.subheader("3. Results")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total rows read", proc["row_count_raw"])
        col2.metric("Valid rows",       proc["row_count_valid"])
        col3.metric(
            "Error rows",
            proc["row_count_errors"],
            delta=(
                "-{}".format(proc["row_count_errors"])
                if proc["row_count_errors"] > 0 else None
            ),
            delta_color="inverse",
        )

        if proc["val_errors"]:
            with st.expander(
                "{} validation issue(s)".format(len(proc["val_errors"])),
                expanded=True,
            ):
                for e in proc["val_errors"]:
                    st.error(e)

        st.subheader("4. Review")
        tab1, tab2, tab3 = st.tabs(["Mapped output", "Error rows", "Raw input"])

        # Issue 4: reconstruct DataFrames from bytes only when the tab renders
        with tab1:
            if proc["row_count_valid"] == 0:
                st.warning("No valid rows to display.")
            else:
                df_display = pd.read_csv(BytesIO(proc["csv_bytes"]))
                mandatory_in_df = [
                    f for f in MANDATORY_FIELDS if f in df_display.columns
                ]
                st.caption(
                    "Showing {} valid rows. Mandatory fields present: {}".format(
                        proc["row_count_valid"], ", ".join(mandatory_in_df)
                    )
                )
                st.dataframe(df_display, use_container_width=True, hide_index=True)

        with tab2:
            if proc["row_count_errors"] == 0:
                st.success("No error rows.")
            else:
                st.caption(
                    "{} row(s) failed validation. "
                    "Fix the source file and re-upload.".format(
                        proc["row_count_errors"]
                    )
                )
                df_err_display = pd.read_csv(BytesIO(proc["err_csv"]))
                st.dataframe(df_err_display, use_container_width=True, hide_index=True)

        with tab3:
            st.caption("Original columns as received from customer.")
            df_raw_display = pd.read_csv(BytesIO(proc["raw_csv"]))
            st.dataframe(df_raw_display, use_container_width=True, hide_index=True)

        # ── Export ────────────────────────────────────────────────────────────
        st.subheader("5. Export")

        if proc["row_count_valid"] == 0:
            st.error("Nothing to export — all rows have validation errors.")
            st.session_state["export_payload"] = None
        else:
            ts           = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_fname = "wms_output_{}_{}.csv".format(customer_key, ts)
            error_fname  = "wms_errors_{}_{}.csv".format(customer_key, ts)

            st.session_state["export_payload"] = {
                "output_fname": output_fname,
                "csv_bytes":    proc["csv_bytes"],
                "error_fname":  error_fname,
                "err_csv":      proc["err_csv"],
            }

            st.info(
                "Ready to export **{} valid rows** as WMS CSV.{}".format(
                    proc["row_count_valid"],
                    " **{} error row(s)** will be excluded.".format(
                        proc["row_count_errors"]
                    ) if proc["row_count_errors"] > 0 else "",
                )
            )

        # ── Save new mappings ─────────────────────────────────────────────────
        conv_user_mapping = proc.get("user_mapping", {})

        if conv_user_mapping:
            st.divider()
            st.subheader("6. Save new column mappings")

            already_saved = (
                st.session_state["saved_mapping"] == conv_user_mapping
            )

            if already_saved:
                st.success(
                    "{} mapping(s) already saved for this conversion.".format(
                        len(conv_user_mapping)
                    )
                )
            else:
                st.caption(
                    "These mappings were applied during this conversion but are "
                    "not yet in the config. Save them so future files from this "
                    "customer are recognised automatically."
                )
                st.dataframe(
                    pd.DataFrame(
                        [{"Customer column": k, "WMS field": v}
                         for k, v in conv_user_mapping.items()]
                    ),
                    hide_index=True,
                    use_container_width=True,
                )

                if st.button("💾 Save new mappings to SFTP"):
                    save_result = merge_and_save_mappings(
                        customer_key=customer_key,
                        existing_csv_string=raw_configs.get(customer_key, ""),
                        new_mappings=conv_user_mapping,
                    )
                    if save_result["success"]:
                        st.success(
                            "{} — {} new row(s) added.".format(
                                save_result["message"],
                                save_result.get("rows_added", 0),
                            )
                        )
                        st.session_state["saved_mapping"] = dict(conv_user_mapping)
                        fetch_all_raw_configs.clear()
                    else:
                        st.error(save_result["message"])

else:
    st.info("Upload a customer order file above to begin.")


# ─── DOWNLOAD + WMS UPLOAD ───────────────────────────────────────────────────

payload = st.session_state.get("export_payload")
if payload:
    st.divider()
    st.download_button(
        label="⬇ Download WMS CSV",
        data=payload["csv_bytes"],
        file_name=payload["output_fname"],
        mime="text/csv",
        type="primary",
    )
    if payload["err_csv"] is not None:
        st.download_button(
            label="⬇ Download error report",
            data=payload["err_csv"],
            file_name=payload["error_fname"],
            mime="text/csv",
        )

    # Issue 8: disable upload button after a successful upload to prevent
    # the same file being imported to the WMS twice in the same session.
    if st.session_state.get("upload_done"):
        st.success(
            "Already uploaded to WMS server: **{}**".format(
                payload["output_fname"]
            )
        )
    elif st.button("☁ Upload CSV to WMS file server", type="secondary"):
        with st.spinner("Connecting to WMS SFTP server…"):
            result = upload_to_wms(
                local_file_bytes=payload["csv_bytes"],
                remote_filename=payload["output_fname"],
            )
        if result["success"]:
            st.success(result["message"])
            st.session_state["upload_done"] = True
        else:
            st.error(result["message"])


# ─── FOOTER ──────────────────────────────────────────────────────────────────

st.divider()
st.caption(
    "WMS Order File Converter | WMS / GIT Department | "
    "Mapping configs are stored on SFTP. Upload new configs via the sidebar."
)
