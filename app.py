"""
app.py
======
WMS Order File Converter — Streamlit Web Interface

Run with:
    streamlit run app.py

Dependencies:
    - streamlit   : web UI framework
    - pandas      : dataframe display
    - converter   : local core engine (converter.py)
"""

import re
import tempfile
import os
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd

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

from sftp_uploader import upload_to_wms
# FIX: load_customer_config removed from imports. The web app no longer calls it
# directly — the config object is retrieved from validate_all_customer_configs()
# which already loads every config internally. Importing and calling it separately
# caused each valid customer's config to be loaded twice per page render.
for _key in ("SFTP_HOST", "SFTP_PORT", "SFTP_USER", "SFTP_PASSWORD",
             "SFTP_PRIVATE_KEY", "SFTP_TARGET_DIR"):
    if _key in st.secrets and _key not in os.environ:
        os.environ[_key] = str(st.secrets[_key])
# ─── PAGE CONFIG ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="WMS Order Converter",
    page_icon="📦",
    layout="wide"
)

st.title("📦 WMS Order File Converter")
st.caption("Convert customer order files into WMS-ready CSV format.")

if "export_payload" not in st.session_state:
    st.session_state["export_payload"] = None

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def get_mappings_fingerprint() -> str:
    """
    Build a cache-busting string from every config file's name, size and
    modification time. Changes whenever a config is added, removed or updated.
    """
    config_files = sorted(
        p for ext in ("*.csv", "*.xlsx")
        for p in MAPPINGS_DIR.glob(ext)
        if p.stem != "template"
    )
    parts = []
    for p in config_files:
        stat = p.stat()
        parts.append("{}:{}:{}".format(p.name, stat.st_mtime_ns, stat.st_size))
    return "|".join(parts)


def sanitise_config_filename(raw_name: str) -> str:
    """
    Sanitise an uploaded config filename to prevent path traversal.
    Keeps only alphanumerics, hyphens and underscores in the stem.
    Forces the extension to lowercase .csv or .xlsx.

    e.g. "../../converter.py"  → rejected (bad extension)
         "Gigly Gulp 2026.csv" → "Gigly_Gulp_2026.csv"
    """
    p    = Path(raw_name)
    stem = re.sub(r"[^\w\-]", "_", p.stem)
    ext  = p.suffix.lower()
    return "{}{}".format(stem, ext)


@st.cache_data(show_spinner=False)
def cached_list_customers(fingerprint: str):
    _ = fingerprint
    return list_customers()


@st.cache_data(show_spinner=False)
def cached_validate_all_configs(fingerprint: str):
    # FIX: validate_all_customer_configs() now returns the loaded config object
    # alongside each report dict. This cache therefore serves both the config
    # health display AND the config object needed for processing — eliminating
    # the separate cached_load_customer_config() call that previously caused
    # every valid config to be loaded twice per page render.
    _ = fingerprint
    return validate_all_customer_configs()


# ─── SIDEBAR ─────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Settings")

    # ── Upload a new config file ─────────────────────────────────────────────
    st.subheader("Upload config")
    uploaded_config = st.file_uploader(
        "Drop a customer config here (.csv or .xlsx)",
        type=["csv", "xlsx"],
        help=(
            "Config must have columns: customer_column, wms_field. "
            "Optional columns: customer_name, date_format. "
            "The filename becomes the customer key. Maximum size: 1 MB."
        ),
    )

    if uploaded_config is not None:
        config_bytes = uploaded_config.getvalue()

        # Guard 1 — file size
        if len(config_bytes) > MAX_CONFIG_BYTES:
            st.error(
                "Config file is too large ({} KB). Maximum allowed is 1 MB.".format(
                    len(config_bytes) // 1024
                )
            )
        else:
            # Guard 2 — sanitise filename (path traversal protection)
            safe_name   = sanitise_config_filename(uploaded_config.name)
            config_dest = MAPPINGS_DIR / safe_name

            if config_dest.stem == "template":
                st.error("Cannot overwrite the template file. Rename your config and re-upload.")
            # FIX: the original code had a second extension check here
            # (config_dest.suffix.lower() not in (".csv", ".xlsx")) which could never
            # be reached — the file_uploader above already restricts accepted types
            # to csv and xlsx at the browser level. Removed as confirmed dead code.
            else:
                config_dest.write_bytes(config_bytes)
                st.success(
                    "Config saved: **{}**. Customer key: **{}**".format(
                        safe_name, config_dest.stem
                    )
                )
                if safe_name != uploaded_config.name:
                    st.info(
                        "Filename was sanitised: '{}' → '{}'".format(
                            uploaded_config.name, safe_name
                        )
                    )

    st.divider()

    # Fingerprint computed after any upload so new files are visible immediately
    mappings_fingerprint = get_mappings_fingerprint()

    customers = cached_list_customers(mappings_fingerprint)
    if not customers:
        st.error("No customer configs found in mappings/ directory.")
        st.stop()

    all_config_reports = cached_validate_all_configs(mappings_fingerprint)
    invalid_configs    = sorted(
        key for key, r in all_config_reports.items() if r["errors"]
    )
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
            k, "(invalid)" if k in invalid_configs else ""
        ),
    )

    selected_report = all_config_reports.get(customer_key, {"errors": [], "warnings": [], "config": None})
    config_is_valid = len(selected_report["errors"]) == 0

    if config_is_valid:
        # FIX: config retrieved directly from the report returned by
        # cached_validate_all_configs() — no second load_customer_config() call needed.
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
            lambda f: "Yes" if f in MANDATORY_FIELDS else ""
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

# Sheet selector — Excel only
sheet_input = st.text_input(
    "Sheet name or index (Excel only)",
    value="0",
    help="Enter 0 for the first sheet, 1 for second, or type the exact sheet name.",
    disabled=not config_is_valid,
)

try:
    sheet_name = int(sheet_input)
except ValueError:
    sheet_name = sheet_input

# Page selector — PDF only, shown conditionally
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
    page_number = int(page_input) - 1   # convert to 0-indexed internally

# ─── MAIN: PROCESS ───────────────────────────────────────────────────────────

if uploaded_file:
    st.subheader("2. Processing")

    file_bytes = uploaded_file.read()
    ext        = Path(uploaded_file.name).suffix.lower()

    # Magic byte validation — confirm the file is genuine before passing to reader
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

    # Write to temp file using the uploaded file's actual extension.
    # FIX: removed the dead fallback `ext if ext else ".xlsx"` — at this point
    # ext is always one of .pdf / .xlsx / .xls (magic byte check above passed).
    tmp_path = None

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
            tmp.write(file_bytes)
            tmp_path = Path(tmp.name)

        with st.spinner("Reading and mapping file..."):
            try:
                df_raw              = read_order_file(
                                          tmp_path,
                                          sheet_name=sheet_name,
                                          page_number=page_number,
                                      )
                df_mapped, warnings = apply_mapping(
                    df_raw, config["column_map"], case_insensitive_source=True
                )
                df_valid, df_errors, val_errors = validate(df_mapped)
                # FIX: clean_data() now returns (df, clean_warnings); capture both.
                df_clean, clean_warnings        = clean_data(
                    df_valid, date_format=config.get("date_format")
                )
                warnings = warnings + clean_warnings
            except (FileNotFoundError, ValueError, ImportError) as e:
                st.error(str(e))
                st.stop()

    finally:
        # Always clean up temp file — runs even if st.stop() is called above
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)

    # ── Warnings ──────────────────────────────────────────────────────────────
    if warnings:
        with st.expander("{} warning(s)".format(len(warnings)), expanded=True):
            for w in warnings:
                st.warning(w)

    # ── Metrics ───────────────────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    col1.metric("Total rows read", len(df_raw))
    col2.metric("Valid rows",       len(df_valid))
    col3.metric(
        "Error rows",
        len(df_errors),
        delta="-{}".format(len(df_errors)) if len(df_errors) > 0 else None,
        delta_color="inverse",
    )

    if val_errors:
        with st.expander("{} validation issue(s)".format(len(val_errors)), expanded=True):
            for e in val_errors:
                st.error(e)

    # ── Review tabs ───────────────────────────────────────────────────────────
    st.subheader("3. Review")
    tab1, tab2, tab3 = st.tabs(["Mapped output", "Error rows", "Raw input"])

    with tab1:
        if df_clean.empty:
            st.warning("No valid rows to display.")
        else:
            mandatory_in_df = [f for f in MANDATORY_FIELDS if f in df_clean.columns]
            st.caption(
                "Showing {} valid rows. Mandatory fields: {}".format(
                    len(df_clean), ", ".join(mandatory_in_df)
                )
            )
            st.dataframe(df_clean, use_container_width=True, hide_index=True)

    with tab2:
        if df_errors.empty:
            st.success("No error rows.")
        else:
            st.caption(
                "{} row(s) failed validation. Fix the source file and re-upload.".format(
                    len(df_errors)
                )
            )
            st.dataframe(df_errors, use_container_width=True, hide_index=True)

    with tab3:
        st.caption("Original columns as received from customer.")
        st.dataframe(df_raw, use_container_width=True, hide_index=True)

    # ── Export ────────────────────────────────────────────────────────────────
    st.subheader("4. Export")

    if df_clean.empty:
        st.error("Nothing to export — all rows have validation errors.")
        st.session_state["export_payload"] = None
    else:
        # FIX: use shared ordered_wms_columns() — eliminates the duplicate
        # [c for c in ALL_WMS_FIELDS if c in df.columns] expression that also
        # exists inside export_csv() in converter.py.
        # FIX: removed encoding="utf-8-sig" from to_csv(). When to_csv() is
        # called without a file path it returns a plain Python str; the encoding
        # argument is silently ignored. The BOM is correctly inserted by the
        # subsequent .encode("utf-8-sig") call. Having both implied double-encoding
        # and obscured where the BOM was actually applied.
        cols_ordered = ordered_wms_columns(df_clean)
        csv_bytes    = (
            df_clean[cols_ordered]
            .to_csv(index=False)
            .encode("utf-8-sig")
        )
        err_csv = (
            df_errors.to_csv(index=False).encode("utf-8-sig")
            if not df_errors.empty else None
        )

        st.info(
            "Ready to export **{} valid rows** as WMS CSV.{}".format(
                len(df_clean),
                " **{} error row(s)** will be excluded.".format(len(df_errors))
                if len(df_errors) > 0 else ""
            )
        )

        ts           = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_fname = "wms_output_{}_{}.csv".format(customer_key, ts)
        error_fname  = "wms_errors_{}_{}.csv".format(customer_key, ts)

        # Store in session state so download buttons survive Streamlit reruns
        st.session_state["export_payload"] = {
            "output_fname": output_fname,
            "csv_bytes":    csv_bytes,
            "error_fname":  error_fname,
            "err_csv":      err_csv,
        }

else:
    if config_is_valid:
        st.info("Upload a customer order file to begin.")
    else:
        st.info("Fix config errors in mappings first, then upload a file.")

# Download buttons live outside the if/else so they persist after rerun
payload = st.session_state.get("export_payload")
if payload:
    st.download_button(
        label="⬇ Download CSV",
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
if st.button("Upload CSV to file server", type="primary"):
    with st.spinner("Connecting to SFTP server..."):
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
    "To add a new customer, copy mappings/template.csv and fill in "
    "customer_column / wms_field."
)
