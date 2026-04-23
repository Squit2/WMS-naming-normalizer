"""
app.py
======
WMS Order File Converter — Streamlit Web Interface

Run with:
    streamlit run app.py

Dependencies:
    pip install streamlit pandas paramiko pdfplumber openpyxl

── File workflow ──────────────────────────────────────────────────────────────

  Folder mode  (primary)
    1. Drop order files into  client_mocks/
    2. Select a file and customer in the UI, then click Convert
    3. On success:
         clean rows  → Processed/{original_name}_{ts}.csv
         error rows  → Error/{original_name}_errors_{ts}.csv   (if any)
         original    → Archive/{original_name}_{ts}{ext}   then deleted from client_mocks/
    4. If the entire pipeline fails the source file is left untouched.

  Manual upload mode  (fallback / ad-hoc)
    The existing st.file_uploader flow.  Outputs are downloaded via the browser;
    no server-side folder routing is performed.

── Config storage ─────────────────────────────────────────────────────────────

  SFTP mode — SFTP_HOST, SFTP_USERNAME, SFTP_PASSWORD present in secrets.toml
  Local mode — falls back to mappings/ directory (original behaviour)

── Streamlit secrets (SFTP mode) ─────────────────────────────────────────────

  SFTP_HOST         = "your-sftp-host"
  SFTP_USERNAME     = "your-username"
  SFTP_PASSWORD     = "your-password"
  SFTP_PORT         = 22              # optional
  SFTP_MAPPINGS_DIR = "/mappings"     # optional
  SFTP_TARGET_DIR   = "/wms-import"   # for output delivery via sftp_uploader
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

# ── WMS output delivery SFTP ───────────────────────────────────────────────────
from sftp_uploader import upload_to_wms

# ── SFTP Folder routing (replaces file_router) ──────────────────────────────
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
# Ensure all four workspace folders exist before any UI is rendered.

try:
    ensure_workspace_folders()
except OSError as _folder_err:
    st.error(
        "**Cannot create workspace folders.**  \n"
        "Check filesystem permissions in the project directory.  \n"
        "Error: `{}`".format(_folder_err)
    )
    st.stop()


# ─── PAGE CONFIG ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="WMS Order Converter",
    page_icon="📦",
    layout="wide",
)

st.title("📦 WMS Order File Converter")
st.caption("Convert customer order files into WMS-ready CSV format.")


# ─── OPERATING MODE ──────────────────────────────────────────────────────────

USE_SFTP: bool = is_sftp_configured()

if USE_SFTP:
    st.info(
        "☁ **SFTP mode** — mapping configs are loaded from and saved to the "
        "SFTP config store. Changes persist across restarts.",
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
    ("export_payload",   None),   # bytes ready to download (upload mode only)
    ("proc_result",      None),   # full pipeline output dict
    ("proc_file_key",    None),   # fingerprint to detect file/customer change
    ("saved_mapping",    {}),     # user_mapping already written to SFTP/local
    ("source_mode",      None),   # "folder" | "upload" — persisted across reruns
    ("routed",           False),  # True once folder-mode outputs have been written
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


def _local_merge_and_save(customer_key: str, new_mappings: dict) -> dict:
    existing_df = pd.DataFrame(columns=["customer_column", "wms_field"])
    for _ext in (".csv", ".xlsx"):
        _c = MAPPINGS_DIR / "{}{}".format(customer_key, _ext)
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
    dest       = MAPPINGS_DIR / "{}.csv".format(customer_key)
    try:
        dest.write_text(df_merged.to_csv(index=False), encoding="utf-8")
        return {
            "success":    True,
            "message":    "Config '{}' updated locally.".format(customer_key),
            "rows_added": rows_added,
        }
    except OSError as exc:
        return {
            "success":    False,
            "message":    "Could not write local config: {}".format(exc),
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
            "**SFTP connection failed.**  \n"
            "Credentials were found in secrets but the server could not be reached.  \n"
            "Error: `{}`".format(_sftp_err)
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
        help="Files in client_mocks/ waiting to be processed.",
    )
    _col_b.metric(
        "Processed",
        _stats["processed"]["count"],
        help="Clean WMS CSVs in Processed/.",
    )
    _col_c, _col_d = st.columns(2)
    _col_c.metric(
        "Archived",
        _stats["archive"]["count"],
        help="Original source files stored in Archive/.",
    )
    _col_d.metric(
        "Errors",
        _stats["errors"]["count"],
        help="Error-row CSVs in Error/.",
    )

    if st.button("🔄 Refresh counts"):
        st.rerun()

    st.divider()

    # ── Upload a new mapping config ──────────────────────────────────────────
    st.subheader("Upload config")
    uploaded_config = st.file_uploader(
        "Drop a customer config here (.csv or .xlsx)",
        type=["csv", "xlsx"],
        help=(
            "Required columns: customer_column, wms_field.  \n"
            "Optional: customer_name, date_format.  \n"
            "Filename stem → customer key.  Max 1 MB."
        ),
    )

    if uploaded_config is not None:
        _cfg_bytes = uploaded_config.getvalue()
        _cfg_ext   = Path(uploaded_config.name).suffix.lower()

        if len(_cfg_bytes) > MAX_CONFIG_BYTES:
            st.error("Config too large ({} KB). Max is 1 MB.".format(
                len(_cfg_bytes) // 1024
            ))
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
                            st.success("Config saved to SFTP: **{}**".format(_safe_key))
                            if _cfg_ext == ".xlsx":
                                st.info("XLSX converted to CSV before upload.")
                            fetch_all_raw_configs.clear()
                            st.rerun()
                        else:
                            st.error(_res["message"])
                    else:
                        (MAPPINGS_DIR / "{}.csv".format(_safe_key)).write_text(
                            _csv_str, encoding="utf-8"
                        )
                        st.success("Config saved locally: **{}**".format(_safe_key))
                        if _cfg_ext == ".xlsx":
                            st.info("XLSX converted to CSV on save.")
                        st.rerun()

                except ValueError as _ve:
                    st.error("Invalid config — not saved.  \n`{}`".format(_ve))
                except Exception as _ue:
                    st.error("Unexpected error: `{}`".format(_ue))

    st.divider()

    # ── Customer selector ────────────────────────────────────────────────────
    if not customers:
        st.error(
            "No customer configs found{}.  \nUpload a config above.".format(
                " on SFTP" if USE_SFTP else " in mappings/"
            )
        )
        st.stop()

    if invalid_configs:
        st.warning("{} invalid config(s).".format(len(invalid_configs)))

    if st.button("Validate all configs"):
        for _key in customers:
            _r = all_config_reports.get(_key, {"errors": [], "warnings": []})
            if _r["errors"]:
                st.error("{}: {}".format(_key, " | ".join(_r["errors"])))
            elif _r["warnings"]:
                st.warning("{}: {}".format(_key, " | ".join(_r["warnings"])))
            else:
                st.success("{}: OK".format(_key))

    customer_key = st.selectbox(
        "Customer",
        options=customers,
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

# Persist the mode choice across reruns so the result panel doesn't vanish
# when Streamlit re-renders due to widget interaction.
_mode_opts  = ["📂 Folder (client_mocks/)", "⬆ Manual upload"]
_mode_index = 0 if st.session_state["source_mode"] != "upload" else 1

source_mode = st.radio(
    "Where is the order file?",
    _mode_opts,
    index=_mode_index,
    horizontal=True,
    disabled=not config_is_valid,
    help=(
        "**Folder mode**: pick a file already in client_mocks/. "
        "The original is archived automatically after conversion.  \n"
        "**Manual upload**: upload directly from your computer. "
        "Outputs are downloaded via the browser."
    ),
)
st.session_state["source_mode"] = (
    "upload" if source_mode == _mode_opts[1] else "folder"
)
is_folder_mode = st.session_state["source_mode"] == "folder"

# ── File selection ────────────────────────────────────────────────────────────

file_bytes:    bytes       = b""
source_name:   str         = ""
source_path:   Path | None = None   # only set in folder mode

if is_folder_mode:
    st.subheader("2. Pick file from client_mocks/")
    
    with st.spinner("Scanning SFTP server..."):
        pending = list_pending_files()

    if not pending:
        st.info(
            "No files found in `client_mocks/` on the FileZilla server.  \n"
            "Drop files into that folder via FTP, then click **🔄 Refresh counts**."
        )
    else:
        # Build display labels: "filename.xlsx  (45.2 KB,  modified 14:30)"
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
                # Fetch bytes over SFTP only when selected
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
        help="Outputs will be available as browser downloads.",
    )
    if uploaded_file is not None:
        file_bytes  = uploaded_file.read()
        source_name = uploaded_file.name
        source_path = None   # no server-side path — came from browser

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

    # ── Magic-byte validation ─────────────────────────────────────────────────
    if ext == ".pdf":
        if not is_valid_pdf(file_bytes):
            st.error("File does not appear to be a valid PDF.")
            st.stop()
    else:
        if not is_valid_xlsx(file_bytes):
            st.error("File does not appear to be a valid Excel file.")
            st.stop()

    # ── Invalidate session when file or customer changes ──────────────────────
    current_file_key = _file_key(file_bytes, customer_key)
    if st.session_state["proc_file_key"] != current_file_key:
        st.session_state["proc_result"]    = None
        st.session_state["proc_file_key"]  = current_file_key
        st.session_state["saved_mapping"]  = {}
        st.session_state["export_payload"] = None
        st.session_state["routed"]         = False

    # ── Read raw file — always done so unmapped columns can be shown ──────────
    _tmp = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as _f:
            _f.write(file_bytes)
            _tmp = Path(_f.name)
        try:
            df_raw = read_order_file(_tmp, sheet_name=sheet_name,
                                     page_number=page_number)
        except (FileNotFoundError, ValueError, ImportError) as _e:
            st.error(str(_e))
            st.stop()
    finally:
        if _tmp:
            _tmp.unlink(missing_ok=True)

    # ── Dynamic column mapping UI ─────────────────────────────────────────────
    st.subheader("3. Column mapping")

    known_source_cols = {k.lower() for k in config["column_map"]}
    unmapped_cols     = [
        c for c in df_raw.columns if c.lower() not in known_source_cols
    ]
    user_mapping: dict = {}

    if unmapped_cols:
        st.info(
            "**{} column(s)** not in the mapping config.  \n"
            "Assign each to a WMS field or leave as *Ignore*.".format(
                len(unmapped_cols)
            )
        )
        for _rs in range(0, len(unmapped_cols), 3):
            _grid = st.columns(3)
            for _j, _col in enumerate(unmapped_cols[_rs : _rs + 3]):
                with _grid[_j]:
                    _sel = st.selectbox(
                        "Map **'{}'** to:".format(_col),
                        ["— Ignore —"] + ALL_WMS_FIELDS,
                        key="umap_{}".format(_col),
                    )
                    if _sel != "— Ignore —":
                        user_mapping[_col] = _sel
    else:
        st.success(
            "All {} column(s) covered by the mapping config.".format(
                len(df_raw.columns)
            )
        )

    # ── Convert button ────────────────────────────────────────────────────────
    _btn = (
        "▶ Convert & Route  ({} new mapping(s))".format(len(user_mapping))
        if user_mapping and is_folder_mode
        else "▶ Convert & Route" if is_folder_mode
        else "▶ Convert  ({} new mapping(s))".format(len(user_mapping))
        if user_mapping
        else "▶ Convert"
    )

    if st.button(_btn, type="primary"):
        final_map = {**config["column_map"], **user_mapping}
        ts        = datetime.now().strftime("%Y%m%d_%H%M%S")

        _tmp2 = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as _f2:
                _f2.write(file_bytes)
                _tmp2 = Path(_f2.name)

            with st.spinner("Mapping and validating…"):
                df_raw2             = read_order_file(
                    _tmp2, sheet_name=sheet_name, page_number=page_number
                )
                df_mapped, warnings = apply_mapping(
                    df_raw2, final_map, case_insensitive_source=True
                )
                df_valid, df_errors, val_errors = validate(df_mapped)
                df_clean, clean_warnings        = clean_data(
                    df_valid, date_format=config.get("date_format")
                )
                warnings = warnings + clean_warnings

        except (FileNotFoundError, ValueError, ImportError) as _pe:
            st.error(str(_pe))
            # Pipeline failed — do NOT archive; leave source intact.
            st.stop()
        finally:
            if _tmp2:
                _tmp2.unlink(missing_ok=True)

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
            
            with st.spinner("Routing files on FileZilla server..."):
                # 1. Write processed CSV
                if csv_bytes_out:
                    _r = write_processed(csv_bytes_out, Path(source_name).stem, ts)
                    routing_log.append(("✅" if _r["success"] else "❌", _r["message"]))

                # 2. Write error CSV
                if err_bytes_out:
                    _r = write_error(err_bytes_out, Path(source_name).stem, ts)
                    routing_log.append(("⚠️" if _r["success"] else "❌", _r["message"]))

                # 3. Archive source (Server-side move)
                _arc = archive_source_file(source_name, ts)
                routing_log.append(("📦" if _arc["success"] else "❌", _arc["message"]))

            st.session_state["routed"] = True

            # Build download payload 
            _out_fname = f"{make_output_stem(Path(source_name).stem, ts)}.csv"
            _err_fname = f"{Path(source_name).stem}_errors_{ts}.csv"
            
        else:
            # Upload mode: only populate the download payload
            routing_log   = []
            _out_fname    = "wms_output_{}_{}.csv".format(customer_key, ts)
            _err_fname    = "wms_errors_{}_{}.csv".format(customer_key, ts)

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

        # Warnings
        if proc["warnings"]:
            with st.expander(
                "{} warning(s)".format(len(proc["warnings"])), expanded=True
            ):
                for _w in proc["warnings"]:
                    st.warning(_w)

        # ── Routing result banner (folder mode only) ──────────────────────────
        if proc.get("routing_log"):
            st.subheader("4. File routing")
            for _icon, _msg in proc["routing_log"]:
                if _icon == "❌":
                    st.error("{} {}".format(_icon, _msg))
                elif _icon == "⚠️":
                    st.warning("{} {}".format(_icon, _msg))
                else:
                    st.success("{} {}".format(_icon, _msg))

            _ws = st.secrets.get("SFTP_WORKSPACE_DIR", "/client_mocks").rstrip("/")
            _folder_map = {
                "Processed": f"{_ws}/Processed",
                "Error":     f"{_ws}/Error",
                "Archive":   f"{_ws}/Archive",
            }
            with st.expander("Folder paths"):
                for _name, _path in _folder_map.items():
                    st.code("{:<12} {}".format(_name + ":", _path))

        # ── Metrics ───────────────────────────────────────────────────────────
        st.subheader("5. Results")
        _m1, _m2, _m3 = st.columns(3)
        _m1.metric("Total rows read", len(proc["df_raw"]))
        _m2.metric("Valid rows",       len(proc["df_clean"]))
        _m3.metric(
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
                for _ve in proc["val_errors"]:
                    st.error(_ve)

        # ── Review tabs ───────────────────────────────────────────────────────
        st.subheader("6. Review")
        _t1, _t2, _t3 = st.tabs(["Mapped output", "Error rows", "Raw input"])

        with _t1:
            if proc["df_clean"].empty:
                st.warning("No valid rows.")
            else:
                _mand = [f for f in MANDATORY_FIELDS
                         if f in proc["df_clean"].columns]
                st.caption(
                    "{} valid rows. Mandatory fields: {}".format(
                        len(proc["df_clean"]), ", ".join(_mand)
                    )
                )
                st.dataframe(proc["df_clean"],
                             use_container_width=True, hide_index=True)

        with _t2:
            if proc["df_errors"].empty:
                st.success("No error rows.")
            else:
                st.caption(
                    "{} row(s) failed validation.".format(len(proc["df_errors"]))
                )
                st.dataframe(proc["df_errors"],
                             use_container_width=True, hide_index=True)

        with _t3:
            st.caption("Original columns from customer file.")
            st.dataframe(proc["df_raw"],
                         use_container_width=True, hide_index=True)

        # ── Export / download (upload mode) ───────────────────────────────────
        st.subheader("7. Export")

        if is_folder_mode:
            if st.session_state["routed"]:
                st.success(
                    "Outputs have been routed to disk automatically. "
                    "Use the download buttons below if you also need a local copy."
                )
            else:
                st.info("Convert the file first to route outputs to disk.")

        # Download buttons are always shown (useful in both modes)
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

            if payload["csv_bytes"]:
                if st.button("☁ Upload CSV to WMS file server", type="secondary"):
                    with st.spinner("Connecting to WMS SFTP server…"):
                        _res = upload_to_wms(
                            local_file_bytes=payload["csv_bytes"],
                            remote_filename=payload["output_fname"],
                        )
                    if _res["success"]:
                        st.success(_res["message"])
                    else:
                        st.error(_res["message"])

        # ── Save new mappings ──────────────────────────────────────────────────
        _conv_map = proc.get("user_mapping", {})
        if _conv_map:
            st.divider()
            st.subheader("8. Save new column mappings")
            _already = st.session_state["saved_mapping"] == _conv_map

            if _already:
                st.success(
                    "{} mapping(s) already saved.".format(len(_conv_map))
                )
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
                _save_lbl = (
                    "💾 Save to SFTP" if USE_SFTP
                    else "💾 Save to local config"
                )
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
                        st.success("{} — {} row(s) added.".format(
                            _sr["message"], _sr.get("rows_added", 0)
                        ))
                        st.session_state["saved_mapping"] = dict(_conv_map)
                        if USE_SFTP:
                            fetch_all_raw_configs.clear()
                    else:
                        st.error(_sr["message"])

else:
    # Nothing selected yet
    if config_is_valid:
        if is_folder_mode:
            pending_count = len(list_pending_files())
            if pending_count:
                st.info(
                    "**{} file(s)** waiting in `client_mocks/`.  \n"
                    "Select a file above and click **▶ Convert & Route**.".format(
                        pending_count
                    )
                )
            else:
                st.info(
                    "No files in `client_mocks/` yet.  \n"
                    "Drop `.xlsx`, `.xls`, or `.pdf` order files into that folder."
                )
        else:
            st.info("Upload an order file above to begin.")
    else:
        st.info("Fix config errors in the sidebar first.")


# ─── FOOTER ──────────────────────────────────────────────────────────────────

st.divider()
st.caption(
    "WMS Order File Converter | WMS / GIT Department  |  "
    "Folder workflow: client_mocks/ → Processed/ & Error/ → Archive/  |  "
    + (
        "Config store: SFTP"
        if USE_SFTP else
        "Config store: local mappings/"
    )
)
