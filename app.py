"""
app.py
======
WMS Order File Converter — Streamlit Web Interface

Run with:
    streamlit run app.py

Dependencies:
    pip install streamlit pandas paramiko pdfplumber openpyxl

Operating modes
---------------
SFTP mode  — active when SFTP_HOST, SFTP_USERNAME, SFTP_PASSWORD are present
             in .streamlit/secrets.toml.  Mapping configs are loaded from and
             saved to an SFTP server; changes survive Streamlit restarts.

Local mode — active when SFTP credentials are absent.  Mapping configs are
             read from (and written to) the local  mappings/  directory.
             This is the original behaviour; suitable for local development.

Streamlit secrets (SFTP mode only)
-----------------------------------
SFTP_HOST         = "your-sftp-host"
SFTP_USERNAME     = "your-username"
SFTP_PASSWORD     = "your-password"
SFTP_PORT         = 22              # optional, default 22
SFTP_MAPPINGS_DIR = "/mappings"     # optional, default /mappings

# Output-delivery SFTP (WMS import folder — used by sftp_uploader.py)
SFTP_TARGET_DIR   = "/wms-import"
"""

import os
import re
import hashlib
import tempfile
from io import BytesIO, StringIO
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd

# ── Core processing (never modified) ──────────────────────────────────────────
from converter import (
    list_customers,
    validate_all_customer_configs,
    read_order_file,
    apply_mapping,
    validate,
    clean_data,
    is_valid_xlsx,
    is_valid_pdf,
    ordered_wms_columns,
    ALL_WMS_FIELDS,
    MANDATORY_FIELDS,
    MAPPINGS_DIR,
    MAX_CONFIG_BYTES,
)

# ── SFTP config persistence layer ─────────────────────────────────────────────
from sftp_config_store import (
    is_sftp_configured,
    fetch_all_raw_configs,
    validate_all_sftp_configs,
    list_sftp_customers,
    parse_config_from_csv_string,
    upload_config_csv,
    merge_and_save_mappings,
)

# ── WMS output delivery SFTP (unchanged) ──────────────────────────────────────
from sftp_uploader import upload_to_wms

# Bridge st.secrets → environment variables consumed by sftp_uploader.py.
# Only output-delivery keys are bridged; config-storage keys are read directly
# from st.secrets inside sftp_config_store.py.
for _key in ("SFTP_HOST", "SFTP_PORT", "SFTP_USER", "SFTP_PASSWORD",
             "SFTP_PRIVATE_KEY", "SFTP_TARGET_DIR"):
    try:
        if _key in st.secrets and _key not in os.environ:
            os.environ[_key] = str(st.secrets[_key])
    except Exception:
        pass   # secrets.toml absent — local mode; env vars handled externally


# ─── PAGE CONFIG ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="WMS Order Converter",
    page_icon="📦",
    layout="wide",
)

st.title("📦 WMS Order File Converter")
st.caption("Convert customer order files into WMS-ready CSV format.")


# ─── OPERATING MODE ──────────────────────────────────────────────────────────
# Detected once per render cycle via a pure secrets-key check (no network).

USE_SFTP: bool = is_sftp_configured()

if USE_SFTP:
    st.info(
        "☁ **SFTP mode** — mapping configs are loaded from and saved to the "
        "SFTP config store. Changes persist across restarts.",
        icon="☁",
    )
else:
    st.info(
        "💾 **Local mode** — mapping configs are read from the local "
        "`mappings/` directory. "
        "Add SFTP credentials to `.streamlit/secrets.toml` to enable "
        "cloud persistence.",
        icon="💾",
    )


# ─── SESSION STATE ───────────────────────────────────────────────────────────

for _k, _v in [
    ("export_payload", None),
    ("proc_result",    None),
    ("proc_file_key",  None),
    ("saved_mapping",  {}),
]:
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def sanitise_config_key(raw_name: str) -> str:
    """
    Derive a safe customer_key from an uploaded filename.

    Keeps only alphanumerics, hyphens and underscores; strips the extension.
    e.g.  "Gigly Gulp 2026.csv"  →  "Gigly_Gulp_2026"
          "../../evil.py"        →  "______evil"
    """
    stem = Path(raw_name).stem
    return re.sub(r"[^\w\-]", "_", stem)


def _file_key(file_bytes: bytes, customer_key: str) -> str:
    """Fingerprint of (file content, customer) used to invalidate proc_result."""
    digest = hashlib.md5(file_bytes, usedforsecurity=False).hexdigest()[:12]
    return "{}_{}".format(digest, customer_key)


def _local_merge_and_save(customer_key: str, new_mappings: dict) -> dict:
    """
    Merge new UI-defined mappings into the local config file and write it back.

    Used only in local mode. Searches for an existing .csv or .xlsx config in
    MAPPINGS_DIR. Always writes the merged result back as .csv.

    Returns
    -------
    dict : {success: bool, message: str, rows_added: int}
    """
    existing_df = pd.DataFrame(columns=["customer_column", "wms_field"])

    for _ext in (".csv", ".xlsx"):
        _candidate = MAPPINGS_DIR / "{}{}".format(customer_key, _ext)
        if _candidate.exists():
            try:
                if _ext == ".csv":
                    existing_df = pd.read_csv(_candidate, dtype=str)
                else:
                    existing_df = pd.read_excel(_candidate, dtype=str)
                existing_df.columns = [c.strip().lower() for c in existing_df.columns]
                if "customer_column" in existing_df.columns and \
                   "wms_field" in existing_df.columns:
                    existing_df = existing_df[
                        ["customer_column", "wms_field"]
                    ].copy()
                else:
                    existing_df = pd.DataFrame(columns=["customer_column", "wms_field"])
            except Exception:
                existing_df = pd.DataFrame(columns=["customer_column", "wms_field"])
            break

    df_new = pd.DataFrame(
        [{"customer_column": k, "wms_field": v} for k, v in new_mappings.items()]
    )
    df_merged = (
        pd.concat([existing_df, df_new], ignore_index=True)
        .drop_duplicates(subset=["customer_column"], keep="last")
        .reset_index(drop=True)
    )
    rows_added = max(len(df_merged) - len(existing_df), 0)
    dest       = MAPPINGS_DIR / "{}.csv".format(customer_key)

    try:
        dest.write_text(df_merged.to_csv(index=False), encoding="utf-8")
        return {
            "success":    True,
            "message":    "Config '{}' updated locally ({}).".format(customer_key, dest.name),
            "rows_added": rows_added,
        }
    except OSError as exc:
        return {
            "success":    False,
            "message":    "Could not write local config: {}".format(exc),
            "rows_added": 0,
        }


# ─── LOAD CONFIGS (mode-aware) ───────────────────────────────────────────────

raw_configs: dict = {}   # populated in SFTP mode only

if USE_SFTP:
    try:
        raw_configs        = fetch_all_raw_configs()
        all_config_reports = validate_all_sftp_configs(raw_configs)
        customers          = list_sftp_customers(raw_configs)
    except (RuntimeError, ImportError) as _sftp_err:
        # Credentials were present but the connection failed — this is a real
        # infrastructure error, not a missing-secrets issue.
        st.error(
            "**SFTP connection failed.**  \n"
            "Credentials were found in secrets but the server could not be reached.  \n"
            "Check network, host and port settings, then reload the page.  \n\n"
            "Error: `{}`".format(_sftp_err)
        )
        st.stop()
else:
    # Local mode — delegate entirely to converter's existing functions.
    all_config_reports = validate_all_customer_configs()
    customers          = list_customers()

invalid_configs = sorted(
    key for key, r in all_config_reports.items() if r["errors"]
)


# ─── SIDEBAR ─────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Settings")
    st.caption("☁ SFTP mode" if USE_SFTP else "💾 Local mode")

    # ── Upload a new mapping config ──────────────────────────────────────────
    st.subheader("Upload config")
    uploaded_config = st.file_uploader(
        "Drop a customer config here (.csv or .xlsx)",
        type=["csv", "xlsx"],
        help=(
            "Config must have columns: customer_column, wms_field.  \n"
            "Optional: customer_name, date_format.  \n"
            "The filename stem becomes the customer key.  Max size: 1 MB."
        ),
    )

    if uploaded_config is not None:
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
                st.error(
                    "Cannot overwrite the template. "
                    "Rename your config and re-upload."
                )
            else:
                try:
                    # Normalise to CSV string (xlsx uploads are converted)
                    if ext == ".csv":
                        csv_string = config_bytes.decode("utf-8-sig")
                    else:
                        df_tmp     = pd.read_excel(BytesIO(config_bytes), dtype=str)
                        csv_string = df_tmp.to_csv(index=False)

                    # Validate config structure before writing anywhere
                    parse_config_from_csv_string(csv_string, safe_key)

                    if USE_SFTP:
                        result = upload_config_csv(safe_key, csv_string)
                        if result["success"]:
                            st.success(
                                "Config saved to SFTP: **{}**".format(safe_key)
                            )
                            if ext == ".xlsx":
                                st.info("XLSX converted to CSV before upload.")
                            fetch_all_raw_configs.clear()   # bust TTL cache
                            st.rerun()
                        else:
                            st.error(result["message"])
                    else:
                        dest = MAPPINGS_DIR / "{}.csv".format(safe_key)
                        dest.write_text(csv_string, encoding="utf-8")
                        st.success(
                            "Config saved locally: **{}**".format(safe_key)
                        )
                        if ext == ".xlsx":
                            st.info("XLSX converted to CSV on save.")
                        st.rerun()

                except ValueError as _ve:
                    st.error(
                        "Invalid config — not saved.  \n"
                        "Fix the error below and re-upload:  \n"
                        "`{}`".format(_ve)
                    )
                except Exception as _ue:
                    st.error("Unexpected error reading config: `{}`".format(_ue))

    st.divider()

    # ── Customer selector ────────────────────────────────────────────────────
    if not customers:
        if USE_SFTP:
            st.error(
                "No customer configs found on SFTP (`/mappings/*.csv`).  \n"
                "Upload a config file above to get started."
            )
        else:
            st.error(
                "No customer configs found in `mappings/`.  \n"
                "Add a `.csv` or `.xlsx` config file to that directory."
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

    # ── Mapping preview ──────────────────────────────────────────────────────
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
        min_value=1,
        value=1,
        step=1,
        help="Which page of the PDF contains the order table.",
        disabled=not config_is_valid,
    )
    page_number = int(page_input) - 1


# ─── MAIN: PROCESS ───────────────────────────────────────────────────────────

if uploaded_file and config_is_valid:
    st.subheader("2. Column mapping")

    file_bytes = uploaded_file.read()
    ext        = Path(uploaded_file.name).suffix.lower()

    # ── Magic-byte validation ─────────────────────────────────────────────────
    if ext == ".pdf":
        if not is_valid_pdf(file_bytes):
            st.error(
                "The uploaded file does not appear to be a valid PDF. "
                "Please upload a genuine .pdf file."
            )
            st.stop()
    else:
        if not is_valid_xlsx(file_bytes):
            st.error(
                "The uploaded file does not appear to be a valid Excel file. "
                "Please upload a genuine .xlsx or .xls file."
            )
            st.stop()

    # ── Invalidate stored results when file or customer changes ───────────────
    current_file_key = _file_key(file_bytes, customer_key)
    if st.session_state["proc_file_key"] != current_file_key:
        st.session_state["proc_result"]   = None
        st.session_state["proc_file_key"] = current_file_key
        st.session_state["saved_mapping"] = {}

    # ── Read raw file (fast — always done to detect unmapped columns) ─────────
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
        except (FileNotFoundError, ValueError, ImportError) as exc:
            st.error(str(exc))
            st.stop()
    finally:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)

    # ── Detect unmapped columns (case-insensitive, mirrors apply_mapping) ─────
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

        _tmp2 = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as _f2:
                _f2.write(file_bytes)
                _tmp2 = Path(_f2.name)

            with st.spinner("Mapping and validating…"):
                try:
                    df_raw2             = read_order_file(
                        _tmp2,
                        sheet_name=sheet_name,
                        page_number=page_number,
                    )
                    df_mapped, warnings = apply_mapping(
                        df_raw2, final_map, case_insensitive_source=True
                    )
                    df_valid, df_errors, val_errors = validate(df_mapped)
                    df_clean, clean_warnings        = clean_data(
                        df_valid, date_format=config.get("date_format")
                    )
                    warnings = warnings + clean_warnings

                except (FileNotFoundError, ValueError, ImportError) as exc:
                    st.error(str(exc))
                    st.stop()

        finally:
            if _tmp2 is not None:
                _tmp2.unlink(missing_ok=True)

        st.session_state["proc_result"] = {
            "df_raw":       df_raw,
            "df_clean":     df_clean,
            "df_errors":    df_errors,
            "warnings":     warnings,
            "val_errors":   val_errors,
            "user_mapping": dict(user_mapping),
        }
        st.session_state["saved_mapping"] = {}

    # ── Display results (persisted in session state) ──────────────────────────
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
        col1.metric("Total rows read", len(proc["df_raw"]))
        col2.metric("Valid rows",       len(proc["df_clean"]))
        col3.metric(
            "Error rows",
            len(proc["df_errors"]),
            delta=(
                "-{}".format(len(proc["df_errors"]))
                if len(proc["df_errors"]) > 0 else None
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

        with tab1:
            if proc["df_clean"].empty:
                st.warning("No valid rows to display.")
            else:
                mandatory_in_df = [
                    f for f in MANDATORY_FIELDS if f in proc["df_clean"].columns
                ]
                st.caption(
                    "Showing {} valid rows. Mandatory fields present: {}".format(
                        len(proc["df_clean"]), ", ".join(mandatory_in_df)
                    )
                )
                st.dataframe(
                    proc["df_clean"], use_container_width=True, hide_index=True
                )

        with tab2:
            if proc["df_errors"].empty:
                st.success("No error rows.")
            else:
                st.caption(
                    "{} row(s) failed validation. "
                    "Fix the source file and re-upload.".format(
                        len(proc["df_errors"])
                    )
                )
                st.dataframe(
                    proc["df_errors"], use_container_width=True, hide_index=True
                )

        with tab3:
            st.caption("Original columns as received from customer.")
            st.dataframe(
                proc["df_raw"], use_container_width=True, hide_index=True
            )

        # ── Export ────────────────────────────────────────────────────────────
        st.subheader("5. Export")

        if proc["df_clean"].empty:
            st.error("Nothing to export — all rows have validation errors.")
            st.session_state["export_payload"] = None
        else:
            cols_ordered = ordered_wms_columns(proc["df_clean"])
            csv_bytes    = (
                proc["df_clean"][cols_ordered]
                .to_csv(index=False)
                .encode("utf-8-sig")
            )
            err_csv = (
                proc["df_errors"].to_csv(index=False).encode("utf-8-sig")
                if not proc["df_errors"].empty else None
            )
            ts           = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_fname = "wms_output_{}_{}.csv".format(customer_key, ts)
            error_fname  = "wms_errors_{}_{}.csv".format(customer_key, ts)

            st.session_state["export_payload"] = {
                "output_fname": output_fname,
                "csv_bytes":    csv_bytes,
                "error_fname":  error_fname,
                "err_csv":      err_csv,
            }

            st.info(
                "Ready to export **{} valid rows** as WMS CSV.{}".format(
                    len(proc["df_clean"]),
                    " **{} error row(s)** will be excluded.".format(
                        len(proc["df_errors"])
                    ) if len(proc["df_errors"]) > 0 else "",
                )
            )

        # ── Save new mappings back (mode-aware) ───────────────────────────────
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

                _save_label = (
                    "💾 Save new mappings to SFTP"
                    if USE_SFTP
                    else "💾 Save new mappings to local config"
                )

                if st.button(_save_label):
                    if USE_SFTP:
                        save_result = merge_and_save_mappings(
                            customer_key=customer_key,
                            existing_csv_string=raw_configs.get(customer_key, ""),
                            new_mappings=conv_user_mapping,
                        )
                    else:
                        save_result = _local_merge_and_save(
                            customer_key, conv_user_mapping
                        )

                    if save_result["success"]:
                        st.success(
                            "{} — {} new row(s) added.".format(
                                save_result["message"],
                                save_result.get("rows_added", 0),
                            )
                        )
                        st.session_state["saved_mapping"] = dict(conv_user_mapping)
                        if USE_SFTP:
                            fetch_all_raw_configs.clear()
                    else:
                        st.error(save_result["message"])

else:
    if config_is_valid:
        st.info("Upload a customer order file above to begin.")
    else:
        st.info("Fix config errors in the sidebar first, then upload a file.")


# ─── DOWNLOAD + WMS UPLOAD (outside if/else — survives reruns) ───────────────

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

    if st.button("☁ Upload CSV to WMS file server", type="secondary"):
        with st.spinner("Connecting to WMS SFTP server…"):
            result = upload_to_wms(
                local_file_bytes=payload["csv_bytes"],
                remote_filename=payload["output_fname"],
            )
        if result["success"]:
            st.success(result["message"])
        else:
            st.error(result["message"])


# ─── FOOTER ──────────────────────────────────────────────────────────────────

st.divider()
st.caption(
    "WMS Order File Converter | WMS / GIT Department | "
    + (
        "Mapping configs are stored on SFTP. Upload new configs via the sidebar."
        if USE_SFTP else
        "Mapping configs are in the local mappings/ directory. "
        "Add SFTP credentials to secrets.toml to enable cloud persistence."
    )
)
