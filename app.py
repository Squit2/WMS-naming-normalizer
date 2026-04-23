"""
app.py
======
WMS Order File Converter — Streamlit Web Interface

Run with:
    streamlit run app.py

Dependencies:
    pip install streamlit pandas paramiko pdfplumber openpyxl
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

# ── Core processing (never modified) ──────────────────────────────────────────
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

# ── WMS output delivery SFTP ──────────────────────────────────────────────────
from sftp_uploader import upload_to_wms

# ── SFTP Folder routing (Replaces file_router) ────────────────────────────────
from sftp_router import (
    ensure_workspace_folders,
    list_pending_files,
    folder_stats,
    read_source_file,
    archive_source_file,
    write_processed,
    write_error,
    make_output_stem
)

# Bridge st.secrets → env vars consumed by sftp_uploader.py
for _key in ("SFTP_HOST", "SFTP_PORT", "SFTP_USER", "SFTP_PASSWORD",
             "SFTP_PRIVATE_KEY", "SFTP_TARGET_DIR"):
    try:
        if _key in st.secrets and _key not in os.environ:
            os.environ[_key] = str(st.secrets[_key])
    except Exception:
        pass


# ─── STARTUP ─────────────────────────────────────────────────────────────────
# Ensure all four workspace folders exist on the FileZilla server before UI loads.

try:
    ensure_workspace_folders()
except Exception as _folder_err:
    st.error(
        "**Cannot connect to or create SFTP workspace folders.** \n"
        "Check your network and FileZilla permissions.  \n"
        f"Error: `{_folder_err}`"
    )
    st.stop()


# ─── PAGE CONFIG ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="WMS Order Converter",
    page_icon="📦",
    layout="wide",
)

# ─── PASSWORD PROTECTION ─────────────────────────────────────────────────────
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if not st.session_state["authenticated"]:
    # Using a simple password from secrets to lock the app from the public
    if "APP_PASSWORD" in st.secrets:
        pwd = st.text_input("Enter app password:", type="password")
        if pwd == st.secrets["APP_PASSWORD"]:
            st.session_state["authenticated"] = True
            st.rerun()
        elif pwd:
            st.error("Incorrect password.")
        st.stop()
    else:
        # If no password is set in secrets, just let them in (for local dev)
        st.session_state["authenticated"] = True


st.title("📦 WMS Order File Converter")
st.caption("Convert customer order files into WMS-ready CSV format.")


# ─── OPERATING MODE ──────────────────────────────────────────────────────────

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
    ("export_payload",   None),   
    ("proc_result",      None),   
    ("proc_file_key",    None),   
    ("parse_key",        None),   # Cache key for preventing double-reads
    ("parsed_df",        None),   # In-memory cached dataframe
    ("saved_mapping",    {}),     
    ("source_mode",      None),   
    ("routed",           False),  
]:
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def sanitise_config_key(raw_name: str) -> str:
    stem = Path(raw_name).stem
    return re.sub(r"[^\w\-]", "_", stem)


def _file_key(file_bytes: bytes, customer_key: str) -> str:
    digest = hashlib.md5(file_bytes, usedforsecurity=False).hexdigest()[:12]
    return f"{digest}_{customer_key}"


def _local_merge_and_save(customer_key: str, new_mappings: dict) -> dict:
    existing_df = pd.DataFrame(columns=["customer_column", "wms_field"])
    for _ext in (".csv", ".xlsx"):
        _c = MAPPINGS_DIR / f"{customer_key}{_ext}"
        if _c.exists():
            try:
                existing_df = (
                    pd.read_csv(_c, dtype=str) if _ext == ".csv"
                    else pd.read_excel(_c, dtype=str)
                )
                existing_df.columns = [c.strip().lower() for c in existing_df.columns]
                existing_df = (
                    existing_df[["customer_column", "wms_field"]].copy()
                    if "customer_column" in existing_df.columns and
                       "wms_field"       in existing_df.columns
                    else pd.DataFrame(columns=["customer_column", "wms_field"])
                )
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
    dest       = MAPPINGS_DIR / f"{customer_key}.csv"
    try:
        dest.write_text(df_merged.to_csv(index=False), encoding="utf-8")
        return {
            "success":    True,
            "message":    f"Config '{customer_key}' updated locally.",
            "rows_added": rows_added,
        }
    except OSError as exc:
        return {
            "success":    False,
            "message":    f"Could not write local config: {exc}",
            "rows_added": 0,
        }


# ─── LOAD CONFIGS (mode-aware) ───────────────────────────────────────────────

raw_configs: dict = {}

if USE_SFTP:
    try:
        raw_configs        = fetch_all_raw_configs()
        all_config_reports = validate_all_sftp_configs(raw_configs)
        customers          = list_sftp_customers(raw_configs)
    except (RuntimeError, ImportError) as _sftp_err:
        st.error(
            "**SFTP connection failed.** \n"
            "Credentials were found in secrets but the server could not be reached.  \n"
            f"Error: `{_sftp_err}`"
        )
        st.stop()
else:
    all_config_reports = validate_all_customer_configs()
    customers          = list_customers()

invalid_configs = sorted(
    key for key, r in all_config_reports.items() if r["errors"]
)


# ─── SIDEBAR ─────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Settings")
    st.caption("☁ SFTP mode" if USE_SFTP else "💾 Local mode")

    # ── Workspace folder stats ───────────────────────────────────────────────
    st.subheader("📁 Workspace")
    _stats = folder_stats()

    _col_a, _col_b = st.columns(2)
    _col_a.metric(
        "Pending",
        _stats["pending"]["count"],
        help="Files waiting to be processed.",
    )
    _col_b.metric(
        "Processed",
        _stats["processed"]["count"],
        help="Clean WMS CSVs.",
    )
    _col_c, _col_d = st.columns(2)
    _col_c.metric(
        "Archived",
        _stats["archive"]["count"],
        help="Original source files.",
    )
    _col_d.metric(
        "Errors",
        _stats["errors"]["count"],
        help="Error-row CSVs.",
    )

    if st.button("🔄 Refresh counts"):
        st.rerun()

    st.divider()

    # ── Upload a new mapping config ──────────────────────────────────────────
    st.subheader("Upload config")
    with st.form("config_upload_form", clear_on_submit=True):
        uploaded_config = st.file_uploader(
            "Drop a customer config here (.csv or .xlsx)",
            type=["csv", "xlsx"],
            help=(
                "Required columns: customer_column, wms_field.  \n"
                "Optional: customer_name, date_format.  \n"
                "Filename stem → customer key.  Max 1 MB."
            ),
        )
        submit_config = st.form_submit_button("Save Config")

    if submit_config and uploaded_config is not None:
        _cfg_bytes = uploaded_config.getvalue()
        _cfg_ext   = Path(uploaded_config.name).suffix.lower()

        if len(_cfg_bytes) > MAX_CONFIG_BYTES:
            st.error(f"Config too large. Max is 1 MB.")
        else:
            _safe_key = sanitise_config_key(uploaded_config.name)

            if _safe_key == "template":
                st.error("Cannot overwrite the template. Rename and re-upload.")
            else:
                try:
                    if _cfg_ext == ".csv":
                        _csv_str = _cfg_bytes.decode("utf-8-sig")
                    else:
                        _csv_str = pd.read_excel(
                            BytesIO(_cfg_bytes), dtype=str
                        ).to_csv(index=False)

                    parse_config_from_csv_string(_csv_str, _safe_key)

                    if USE_SFTP:
                        _res = upload_config_csv(_safe_key, _csv_str)
                        if _res["success"]:
                            st.success(f"Config saved to SFTP: **{_safe_key}**")
                            fetch_all_raw_configs.clear()
                            st.rerun()
                        else:
                            st.error(_res["message"])
                    else:
                        (MAPPINGS_DIR / f"{_safe_key}.csv").write_text(
                            _csv_str, encoding="utf-8"
                        )
                        st.success(f"Config saved locally: **{_safe_key}**")
                        st.rerun()

                except ValueError as _ve:
                    st.error(f"Invalid config — not saved.  \n`{_ve}`")
                except Exception as _ue:
                    st.error(f"Unexpected error: `{_ue}`")

    st.divider()

    # ── Customer selector ────────────────────────────────────────────────────
    if not customers:
        st.error(
            "No customer configs found.  \nUpload a config above."
        )
        st.stop()

    if invalid_configs:
        st.warning(f"{len(invalid_configs)} invalid config(s).")

    if st.button("Validate all configs"):
        for _key in customers:
            _r = all_config_reports.get(_key, {"errors": [], "warnings": []})
            if _r["errors"]:
                st.error(f"{_key}: {' | '.join(_r['errors'])}")
            elif _r["warnings"]:
                st.warning(f"{_key}: {' | '.join(_r['warnings'])}")
            else:
                st.success(f"{_key}: OK")

    customer_key = st.selectbox(
        "Customer",
        options=customers,
        format_func=lambda k: f"{k} {'(⚠ invalid)' if k in invalid_configs else ''}",
    )

    selected_report = all_config_reports.get(
        customer_key, {"errors": [], "warnings": [], "config": None}
    )
    config_is_valid = len(selected_report["errors"]) == 0

    if config_is_valid:
        config = selected_report["config"]
        st.success(f"Config loaded: **{config['customer_name']}**")
    else:
        config = None
        st.error("Selected config is invalid.")
        for _err in selected_report["errors"]:
            st.error(_err)

    if selected_report["warnings"]:
        for _w in selected_report["warnings"]:
            st.warning(_w)
    elif config_is_valid:
        st.success("No warnings.")

    st.divider()

    # ── Mapping preview ──────────────────────────────────────────────────────
    st.subheader("Column mapping")
    if config:
        _mdf = pd.DataFrame(
            list(config["column_map"].items()),
            columns=["Customer column", "WMS field"],
        )
        _mdf["Mandatory"] = _mdf["WMS field"].apply(
            lambda f: "✓" if f in MANDATORY_FIELDS else ""
        )
        st.dataframe(_mdf, hide_index=True, use_container_width=True)
    else:
        st.info("Unavailable for invalid config.")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN AREA
# ═══════════════════════════════════════════════════════════════════════════════

# ─── STEP 1 — SOURCE SELECTION ───────────────────────────────────────────────

st.subheader("1. Select source")

_mode_opts  = ["📂 Folder (client_mocks/)", "⬆ Manual upload"]
_mode_index = 0 if st.session_state["source_mode"] != "upload" else 1

source_mode = st.radio(
    "Where is the order file?",
    _mode_opts,
    index=_mode_index,
    horizontal=True,
    disabled=not config_is_valid,
)
st.session_state["source_mode"] = (
    "upload" if source_mode == _mode_opts[1] else "folder"
)
is_folder_mode = st.session_state["source_mode"] == "folder"

# ── File selection ────────────────────────────────────────────────────────────

file_bytes:    bytes = b""
source_name:   str   = ""

if is_folder_mode:
    st.subheader("2. Pick file from remote SFTP folder")
    with st.spinner("Scanning SFTP server..."):
        pending = list_pending_files()

    if not pending:
        st.info(
            "No files found in the workspace folder on the FileZilla server.  \n"
            "Drop files into that folder via FTP, then click **🔄 Refresh counts**."
        )
    else:
        def _label(f: dict) -> str:
            sz  = f["size"] / 1024
            mod = datetime.fromtimestamp(f["mtime"]).strftime("%H:%M %d %b")
            return "{:<40}  {:>7.1f} KB   {}".format(f["name"], sz, mod)

        _selected_file = st.selectbox(
            "Choose a pending file:",
            pending,
            format_func=_label,
            disabled=not config_is_valid,
        )

        if _selected_file is not None:
            source_name = _selected_file["name"]
            try:
                # Fetches securely using our SFTP cache
                with st.spinner(f"Downloading {source_name} into memory..."):
                    file_bytes = read_source_file(source_name)
            except Exception as _read_err:
                st.error(f"Could not read `{source_name}` from SFTP: {_read_err}")

else:
    st.subheader("2. Upload order file")
    uploaded_file = st.file_uploader(
        "Upload order file (.xlsx, .xls or .pdf)",
        type=["xlsx", "xls", "pdf"],
        disabled=not config_is_valid,
    )
    if uploaded_file is not None:
        file_bytes  = uploaded_file.read()
        source_name = uploaded_file.name

# ── Shared file options ───────────────────────────────────────────────────────

sheet_input = st.text_input(
    "Sheet name or index (Excel only — 0 = first sheet)",
    value="0",
    disabled=not config_is_valid or not file_bytes,
)
try:
    sheet_name = int(sheet_input)
except ValueError:
    sheet_name = sheet_input

page_number = 0
if source_name and Path(source_name).suffix.lower() == ".pdf":
    page_number = int(
        st.number_input(
            "PDF page number (1 = first page)",
            min_value=1, value=1, step=1,
            disabled=not config_is_valid or not file_bytes,
        )
    ) - 1


# ─── PROCESS ─────────────────────────────────────────────────────────────────

if file_bytes and config_is_valid:
    ext = Path(source_name).suffix.lower()

    if ext == ".pdf":
        if not is_valid_pdf(file_bytes):
            st.error("File does not appear to be a valid PDF.")
            st.stop()
    else:
        if not is_valid_xlsx(file_bytes):
            st.error("File does not appear to be a valid Excel file.")
            st.stop()

    # ── Invalidate session state for the cache ────────────────────────────────
    current_file_key = _file_key(file_bytes, customer_key)
    current_parse_key = f"{current_file_key}_{sheet_name}_{page_number}"

    if st.session_state.get("parse_key") != current_parse_key:
        st.session_state["proc_result"]    = None
        st.session_state["proc_file_key"]  = current_file_key
        st.session_state["parse_key"]      = current_parse_key
        st.session_state["saved_mapping"]  = {}
        st.session_state["export_payload"] = None
        st.session_state["routed"]         = False
        st.session_state["parsed_df"]      = None  # Reset dataframe cache

    # ── Read raw file ONLY if not cached (Fixes the Double-Read!) ─────────────
    if st.session_state["parsed_df"] is None:
        _tmp = None
        try:
            # We still need one tempfile for the first pdfplumber/pandas read
            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as _f:
                _f.write(file_bytes)
                _tmp = Path(_f.name)
            try:
                with st.spinner("Extracting data from file..."):
                    df_raw = read_order_file(_tmp, sheet_name=sheet_name,
                                             page_number=page_number)
                    # Cache the result!
                    st.session_state["parsed_df"] = df_raw
            except (FileNotFoundError, ValueError, ImportError) as _e:
                st.error(str(_e))
                st.stop()
        finally:
            if _tmp:
                _tmp.unlink(missing_ok=True)
    else:
        # Load instantly from memory!
        df_raw = st.session_state["parsed_df"]

    # ── Dynamic column mapping UI ─────────────────────────────────────────────
    st.subheader("3. Column mapping")

    known_source_cols = {k.lower() for k in config["column_map"]}
    unmapped_cols     = [
        c for c in df_raw.columns if c.lower() not in known_source_cols
    ]
    user_mapping: dict = {}

    if unmapped_cols:
        st.info(
            f"**{len(unmapped_cols)} column(s)** not in the mapping config.  \n"
            "Assign each to a WMS field or leave as *Ignore*."
        )
        for _rs in range(0, len(unmapped_cols), 3):
            _grid = st.columns(3)
            for _j, _col in enumerate(unmapped_cols[_rs : _rs + 3]):
                with _grid[_j]:
                    _sel = st.selectbox(
                        f"Map **'{_col}'** to:",
                        ["— Ignore —"] + ALL_WMS_FIELDS,
                        key=f"umap_{_col}",
                    )
                    if _sel != "— Ignore —":
                        user_mapping[_col] = _sel
    else:
        st.success(
            f"All {len(df_raw.columns)} column(s) covered by the mapping config."
        )

    # ── Convert button ────────────────────────────────────────────────────────
    _btn = (
        f"▶ Convert & Route  ({len(user_mapping)} new mapping(s))" if user_mapping and is_folder_mode
        else "▶ Convert & Route" if is_folder_mode
        else f"▶ Convert  ({len(user_mapping)} new mapping(s))" if user_mapping
        else "▶ Convert"
    )

    if st.button(_btn, type="primary"):
        final_map = {**config["column_map"], **user_mapping}
        ts        = datetime.now().strftime("%Y%m%d_%H%M%S")

        with st.spinner("Mapping and validating…"):
            try:
                # INSTANT PROCESSING using cached dataframe!
                df_mapped, warnings = apply_mapping(
                    df_raw, final_map, case_insensitive_source=True
                )
                df_valid, df_errors, val_errors = validate(df_mapped)
                df_clean, clean_warnings        = clean_data(
                    df_valid, date_format=config.get("date_format")
                )
                warnings = warnings + clean_warnings

            except (FileNotFoundError, ValueError, ImportError) as _pe:
                st.error(str(_pe))
                st.stop()

        # ── Build output bytes ────────────────────────────────────────────────
        cols_ordered  = ordered_wms_columns(df_clean)
        csv_bytes_out = (
            df_clean[cols_ordered].to_csv(index=False).encode("utf-8-sig")
            if not df_clean.empty else b""
        )
        err_bytes_out = (
            df_errors.to_csv(index=False).encode("utf-8-sig")
            if not df_errors.empty else b""
        )

        # ── Folder-mode: route outputs to FileZilla ─────────────────────────
        routing_log = []   

        if is_folder_mode and source_name:
            output_stem = make_output_stem(source_name, ts)
            
            with st.spinner("Routing files on SFTP server..."):
                # 1. Write processed CSV
                if csv_bytes_out:
                    _r = write_processed(csv_bytes_out, Path(source_name).stem, ts)
                    routing_log.append(("✅" if _r["success"] else "❌", _r["message"]))

                # 2. Write error CSV
                if err_bytes_out:
                    _r = write_error(err_bytes_out, Path(source_name).stem, ts)
                    routing_log.append(("⚠️" if _r["success"] else "❌", _r["message"]))

                # 3. Archive source (Server-side rename)
                _arc = archive_source_file(source_name, ts)
                routing_log.append(("📦" if _arc["success"] else "❌", _arc["message"]))

            st.session_state["routed"] = True

            # Download payload for UI
            _out_fname = f"{make_output_stem(Path(source_name).stem, ts)}.csv"
            _err_fname = f"{Path(source_name).stem}_errors_{ts}.csv"
        else:
            # Upload mode: just populate payload
            routing_log   = []
            _out_fname    = f"wms_output_{customer_key}_{ts}.csv"
            _err_fname    = f"wms_errors_{customer_key}_{ts}.csv"

        # ── Store everything in session state ─────────────────────────────────
        st.session_state["proc_result"] = {
            "df_raw":       df_raw,
            "df_clean":     df_clean,
            "df_errors":    df_errors,
            "warnings":     warnings,
            "val_errors":   val_errors,
            "user_mapping": dict(user_mapping),
            "routing_log":  routing_log,
            "ts":           ts,
        }
        st.session_state["export_payload"] = {
            "output_fname": _out_fname,
            "csv_bytes":    csv_bytes_out,
            "error_fname":  _err_fname,
            "err_csv":      err_bytes_out or None,
        }
        st.session_state["saved_mapping"] = {}


    # ── Display results ───────────────────────────────────────────────────────
    proc = st.session_state.get("proc_result")

    if proc and st.session_state["proc_file_key"] == current_file_key:

        if proc["warnings"]:
            with st.expander(f"{len(proc['warnings'])} warning(s)", expanded=True):
                for _w in proc["warnings"]:
                    st.warning(_w)

        # ── Routing result banner (folder mode only) ──────────────────────────
        if proc.get("routing_log"):
            st.subheader("4. File routing")
            for _icon, _msg in proc["routing_log"]:
                if _icon == "❌":
                    st.error(f"{_icon} {_msg}")
                elif _icon == "⚠️":
                    st.warning(f"{_icon} {_msg}")
                else:
                    st.success(f"{_icon} {_msg}")

            _ws = st.secrets.get("SFTP_WORKSPACE_DIR", "/client_mocks").rstrip("/")
            _folder_map = {
                "Processed": f"{_ws}/Processed",
                "Error":     f"{_ws}/Error",
                "Archive":   f"{_ws}/Archive",
            }
            with st.expander("SFTP Folder paths"):
                for _name, _path in _folder_map.items():
                    st.code(f"{_name:<12} {_path}")

        # ── Metrics ───────────────────────────────────────────────────────────
        st.subheader("5. Results")
        _m1, _m2, _m3 = st.columns(3)
        _m1.metric("Total rows read", len(proc["df_raw"]))
        _m2.metric("Valid rows",       len(proc["df_clean"]))
        _m3.metric(
            "Error rows",
            len(proc["df_errors"]),
            delta=(f"-{len(proc['df_errors'])}" if len(proc["df_errors"]) > 0 else None),
            delta_color="inverse",
        )

        if proc["val_errors"]:
            with st.expander(f"{len(proc['val_errors'])} validation issue(s)", expanded=True):
                for _ve in proc["val_errors"]:
                    st.error(_ve)

        # ── Review tabs ───────────────────────────────────────────────────────
        st.subheader("6. Review")
        _t1, _t2, _t3 = st.tabs(["Mapped output", "Error rows", "Raw input"])

        with _t1:
            if proc["df_clean"].empty:
                st.warning("No valid rows.")
            else:
                _mand = [f for f in MANDATORY_FIELDS if f in proc["df_clean"].columns]
                st.caption(f"{len(proc['df_clean'])} valid rows. Mandatory fields: {', '.join(_mand)}")
                st.dataframe(proc["df_clean"], use_container_width=True, hide_index=True)

        with _t2:
            if proc["df_errors"].empty:
                st.success("No error rows.")
            else:
                st.caption(f"{len(proc['df_errors'])} row(s) failed validation.")
                st.dataframe(proc["df_errors"], use_container_width=True, hide_index=True)

        with _t3:
            st.caption("Original columns from customer file.")
            st.dataframe(proc["df_raw"], use_container_width=True, hide_index=True)

        # ── Export / download (upload mode) ───────────────────────────────────
        st.subheader("7. Export")

        if is_folder_mode:
            if st.session_state["routed"]:
                st.success(
                    "Outputs have been securely routed on the SFTP server. "
                    "Use the download buttons below if you also need a local copy."
                )
            else:
                st.info("Convert the file first to route outputs to disk.")

        # Download buttons are always shown
        payload = st.session_state.get("export_payload")
        if payload:
            if payload["csv_bytes"]:
                st.download_button(
                    "⬇ Download WMS CSV",
                    data=payload["csv_bytes"],
                    file_name=payload["output_fname"],
                    mime="text/csv",
                    type="primary",
                )
            else:
                st.error("Nothing to export — all rows have validation errors.")

            if payload["err_csv"]:
                st.download_button(
                    "⬇ Download error report",
                    data=payload["err_csv"],
                    file_name=payload["error_fname"],
                    mime="text/csv",
                )

        # ── Save new mappings ──────────────────────────────────────────────────
        _conv_map = proc.get("user_mapping", {})
        if _conv_map:
            st.divider()
            st.subheader("8. Save new column mappings")
            _already = st.session_state["saved_mapping"] == _conv_map

            if _already:
                st.success(f"{len(_conv_map)} mapping(s) already saved.")
            else:
                st.caption(
                    "These mappings were applied this run but are not yet in "
                    "the config. Save them so future files are mapped automatically."
                )
                st.dataframe(
                    pd.DataFrame(
                        [{"Customer column": k, "WMS field": v}
                         for k, v in _conv_map.items()]
                    ),
                    hide_index=True, use_container_width=True,
                )
                _save_lbl = "💾 Save to SFTP" if USE_SFTP else "💾 Save to local config"
                
                if st.button(_save_lbl):
                    _sr = (
                        merge_and_save_mappings(
                            customer_key=customer_key,
                            existing_csv_string=raw_configs.get(customer_key, ""),
                            new_mappings=_conv_map,
                        ) if USE_SFTP
                        else _local_merge_and_save(customer_key, _conv_map)
                    )
                    if _sr["success"]:
                        st.success(f"{_sr['message']} — {_sr.get('rows_added', 0)} row(s) added.")
                        st.session_state["saved_mapping"] = dict(_conv_map)
                        if USE_SFTP:
                            fetch_all_raw_configs.clear()
                    else:
                        st.error(_sr["message"])

else:
    # Nothing selected yet
    if config_is_valid:
        if is_folder_mode:
            try:
                pending_count = len(list_pending_files())
                if pending_count:
                    st.info(
                        f"**{pending_count} file(s)** waiting on SFTP.  \n"
                        "Select a file above and click **▶ Convert & Route**."
                    )
                else:
                    st.info(
                        "No files in the workspace yet.  \n"
                        "Drop `.xlsx`, `.xls`, or `.pdf` order files via FileZilla."
                    )
            except Exception as e:
                 st.error(f"Could not connect to list pending files: {e}")
        else:
            st.info("Upload an order file above to begin.")
    else:
        st.info("Fix config errors in the sidebar first.")


# ─── FOOTER ──────────────────────────────────────────────────────────────────

st.divider()
st.caption(
    "WMS Order File Converter | WMS / GIT Department  |  "
    "Folder workflow: SFTP client_mocks/ → Processed/ & Error/ → Archive/  |  "
    + ("Config store: SFTP" if USE_SFTP else "Config store: local mappings/")
)
