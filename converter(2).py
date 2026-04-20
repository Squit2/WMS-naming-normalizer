"""
converter.py
============
WMS Order File Converter - Core Engine

Pipeline:
  Step 1 — load_customer_config   : read customer column mapping from .csv/.xlsx config
  Step 2 — read_order_file        : dispatcher — routes to read_excel() or read_pdf()
         — read_excel             : read .xlsx/.xls into DataFrame
         — read_pdf               : extract table from native PDF into DataFrame
         — is_valid_xlsx          : magic byte check for Excel files
         — is_valid_pdf           : magic byte check for PDF files
  Step 3 — apply_mapping          : rename customer columns to WMS standard names
  Step 4 — validate               : check mandatory fields and numeric types (vectorised)
  Step 5 — clean_data             : normalise dates, numerics, whitespace
  Step 6 — export_csv             : write WMS-ready CSV to output/
           export_error_report    : write failed rows to output/
           cleanup_output_dir     : delete output files older than N days
"""

import logging
from pathlib import Path
from datetime import datetime

import pandas as pd

# ─── LOGGING ─────────────────────────────────────────────────────────────────
# Named logger only — never touch the root logger from a library module.

log = logging.getLogger(__name__)

if not log.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    ))
    log.addHandler(_handler)
    log.setLevel(logging.INFO)
    log.propagate = False

# ─── CONFIGURATION ───────────────────────────────────────────────────────────

WMS_FIELDS = {
    "ORDER_REF":        {"mandatory": True,  "dtype": str},
    "ORDER_DATE":       {"mandatory": True,  "dtype": str},
    "CUST_CODE":        {"mandatory": True,  "dtype": str},
    "CUST_NAME":        {"mandatory": False, "dtype": str},
    "ADDRESS_NO":       {"mandatory": True,  "dtype": str},
    "ADDRESS1":         {"mandatory": False, "dtype": str},
    "PROD_CODE":        {"mandatory": True,  "dtype": str},
    "ORIG_QTY_ORDERED": {"mandatory": True,  "dtype": float},
    "ORIG_UOM_CODE":    {"mandatory": True,  "dtype": str},
    "AOPT_DEC_FIELD3":  {"mandatory": False, "dtype": float},
    "AOPT_DEC_FIELD4":  {"mandatory": False, "dtype": float},
    "LOT_NO":           {"mandatory": False, "dtype": str},
    "REMARKS2":         {"mandatory": False, "dtype": str},
    "CUST_PO":          {"mandatory": False, "dtype": str},
    "AOPT_FIELD4":      {"mandatory": False, "dtype": str},
    "AOPT_FIELD5":      {"mandatory": False, "dtype": str},
    "REMARKS":          {"mandatory": False, "dtype": str},
}

MANDATORY_FIELDS = [f for f, m in WMS_FIELDS.items() if m["mandatory"]]
ALL_WMS_FIELDS   = list(WMS_FIELDS.keys())
NUMERIC_FIELDS   = ("ORIG_QTY_ORDERED", "AOPT_DEC_FIELD3", "AOPT_DEC_FIELD4")

MAPPINGS_DIR     = Path(__file__).parent / "mappings"
OUTPUT_DIR       = Path(__file__).parent / "output"

MAX_CONFIG_BYTES = 1 * 1024 * 1024   # 1 MB — enforced in app.py on upload

# FIX: OUTPUT_DIR.mkdir() removed from module level. The directory is created
# lazily inside export_csv() and export_error_report() only when actually needed,
# avoiding the side-effect of directory creation on every library import.


# ─── SHARED UTILITY ──────────────────────────────────────────────────────────

def ordered_wms_columns(df: pd.DataFrame) -> list:
    """
    Return the subset of ALL_WMS_FIELDS that exist in df, preserving canonical order.

    Used by both export_csv() and app.py to ensure consistent column ordering
    without duplicating the filter expression in multiple places.
    """
    return [c for c in ALL_WMS_FIELDS if c in df.columns]


# ─── STEP 1: LOAD CONFIG ─────────────────────────────────────────────────────

def load_customer_config(customer_key: str) -> dict:
    """
    Load a customer mapping config from the mappings/ directory.

    Accepts .csv or .xlsx files with the following column structure:

        customer_column | wms_field   | customer_name       | date_format
        ----------------|-------------|---------------------|------------
        DocNo           | ORDER_REF   | Gigly Gulp Sdn Bhd  | %d/%m/%Y
        DocDate         | ORDER_DATE  |                     |
        DebtorCode      | CUST_CODE   |                     |

    Required columns : customer_column, wms_field
    Optional columns : customer_name, date_format
        customer_name — display name shown in the UI.
                        Only needs a value on the first data row.
                        If absent or blank, derived from the filename.
        date_format   — strftime format for ORDER_DATE values.
                        e.g. %d/%m/%Y for DD/MM/YYYY
                             %m/%d/%Y for MM/DD/YYYY
                        If absent or blank, dayfirst=True is used as fallback.

    Parameters
    ----------
    customer_key : str
        Config filename without extension, e.g. "gigly_gulp"

    Returns
    -------
    dict:
        customer_name : str
        column_map    : dict {customer_column: wms_field}
        date_format   : str or None
    """
    config_path = None
    for ext in (".csv", ".xlsx"):
        candidate = MAPPINGS_DIR / "{}{}".format(customer_key, ext)
        if candidate.exists():
            config_path = candidate
            break

    if config_path is None:
        # FIX: removed the internal list_customers() call that was made solely
        # to embed the list in the error message. Every call site has already
        # resolved the customer list before reaching here, so re-fetching it
        # was a redundant file-system scan on every config-not-found error.
        raise FileNotFoundError(
            "No config found for '{}' in '{}'. "
            "Create mappings/{}.csv with columns: customer_column, wms_field".format(
                customer_key, MAPPINGS_DIR, customer_key
            )
        )

    try:
        if config_path.suffix.lower() == ".csv":
            df_config = pd.read_csv(config_path, dtype=str)
        else:
            df_config = pd.read_excel(config_path, dtype=str)
    except Exception as e:
        raise ValueError("Could not read config '{}': {}".format(config_path.name, e))

    # Normalise headers — strip whitespace and lowercase
    df_config.columns = [c.strip().lower() for c in df_config.columns]

    # Validate required columns exist
    required_cols = {"customer_column", "wms_field"}
    missing_cols  = required_cols - set(df_config.columns)
    if missing_cols:
        raise ValueError(
            "Config '{}' is missing columns: {}. "
            "Required headers: customer_column, wms_field".format(
                config_path.name, missing_cols
            )
        )

    # Strip all values — converts every cell to str in the process
    for col in df_config.columns:
        df_config[col] = df_config[col].astype(str).str.strip()

    # Read optional metadata from dedicated columns — first non-blank value wins
    def _first_value(col_name):
        if col_name not in df_config.columns:
            return None
        vals = df_config[col_name].replace({"nan": "", "None": "", "": None}).dropna()
        return vals.iloc[0] if not vals.empty else None

    customer_name = _first_value("customer_name") or customer_key.replace("_", " ").title()
    date_format   = _first_value("date_format")

    # Keep only the two mapping columns from here on
    df_config = df_config[["customer_column", "wms_field"]].copy()

    # FIX: the original code called dropna() here, but all values were already
    # converted to str above, so NaN had already become the string "nan".
    # dropna() therefore never removed any rows and created a false sense of safety.
    # Replace with a single explicit string-value filter that matches the actual data.
    _null_strings = {"", "nan", "None"}
    df_config = df_config[
        ~df_config["customer_column"].isin(_null_strings) &
        ~df_config["wms_field"].isin(_null_strings)
    ]

    if df_config.empty:
        raise ValueError("Config '{}' has no valid mapping rows.".format(config_path.name))

    # Detect duplicate customer_column entries — last-write-wins is silent and dangerous
    dupes = df_config[df_config["customer_column"].duplicated()]["customer_column"].tolist()
    if dupes:
        raise ValueError(
            "Config '{}' has duplicate customer_column entries: {}. "
            "Each source column must appear only once.".format(config_path.name, dupes)
        )

    # Validate all wms_field values are recognised WMS fields
    invalid_fields = [v for v in df_config["wms_field"].tolist() if v not in ALL_WMS_FIELDS]
    if invalid_fields:
        raise ValueError(
            "Config '{}' contains unrecognised WMS fields: {}. "
            "Valid fields: {}".format(config_path.name, invalid_fields, ALL_WMS_FIELDS)
        )

    column_map = dict(zip(df_config["customer_column"], df_config["wms_field"]))

    log.info("Loaded config: '{}' — {} mapping(s) for {}".format(
        config_path.name, len(column_map), customer_name
    ))
    return {
        "customer_name": customer_name,
        "column_map":    column_map,
        "date_format":   date_format,
    }


def list_customers() -> list:
    """Return all available customer keys (config filenames without extension)."""
    keys = set()
    for ext in ("*.csv", "*.xlsx"):
        for p in MAPPINGS_DIR.glob(ext):
            if p.stem != "template":
                keys.add(p.stem)
    return sorted(keys)


def validate_all_customer_configs() -> dict:
    """
    Load every customer config and return a health report for each.

    Returns
    -------
    dict of {customer_key: {"errors": [...], "warnings": [...], "config": dict or None}}

    FIX: Missing mandatory field mappings are now classified as *errors* (not
    warnings). Previously, a config that omitted a mandatory WMS field mapping
    was flagged as a warning, leaving config_is_valid=True in the UI. When that
    config was actually used, validate() raised a hard MISSING COLUMN error and
    failed all rows — a confusing discrepancy. Elevating these to errors ensures
    the config-health check and the processing pipeline agree on what is valid.

    The loaded config object is returned alongside the report so callers can
    reuse it without a second load_customer_config() call.
    """
    reports = {}
    for customer_key in list_customers():
        errors   = []
        warnings = []
        config   = None
        try:
            config     = load_customer_config(customer_key)
            mapped_wms = set(config["column_map"].values())
            for field in MANDATORY_FIELDS:
                if field not in mapped_wms:
                    # FIX: was warnings.append — see docstring above.
                    errors.append(
                        "Mandatory field '{}' is not mapped in this config.".format(field)
                    )
        except (FileNotFoundError, ValueError) as e:
            errors.append(str(e))

        reports[customer_key] = {
            "errors":   errors,
            "warnings": warnings,
            # Expose config only when it is fully valid; None otherwise so
            # callers never accidentally use a broken config.
            "config":   config if not errors else None,
        }
    return reports


# ─── STEP 2: READ FILE ───────────────────────────────────────────────────────

def is_valid_xlsx(file_bytes: bytes) -> bool:
    """
    Verify a file is a genuine XLSX or XLS by checking magic bytes.
    XLSX/ZIP magic : PK\\x03\\x04      (50 4B 03 04)
    XLS BIFF8 magic: \\xd0\\xcf\\x11\\xe0 (D0 CF 11 E0)
    """
    if len(file_bytes) < 4:
        return False
    return (
        file_bytes[:4] == b"PK\x03\x04" or
        file_bytes[:4] == b"\xd0\xcf\x11\xe0"
    )


def is_valid_pdf(file_bytes: bytes) -> bool:
    """
    Verify a file is a genuine PDF by checking its magic bytes.
    PDF magic: %PDF (25 50 44 46)
    """
    return len(file_bytes) >= 4 and file_bytes[:4] == b"%PDF"


def read_excel(file_path, sheet_name=0) -> pd.DataFrame:
    """
    Read a customer Excel order file into a DataFrame.

    Uses keep_default_na=False so values like "NA", "N/A" are kept
    as literal strings rather than being silently converted to NaN.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError("File not found: {}".format(file_path))
    if file_path.suffix.lower() not in (".xlsx", ".xls"):
        raise ValueError("Unsupported file type: '{}'".format(file_path.suffix))
    try:
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            dtype=str,
            keep_default_na=False,
        )
    except Exception as e:
        raise ValueError("Could not read Excel: {}".format(e))

    df.columns = [str(c).strip() for c in df.columns]
    df.replace("", pd.NA, inplace=True)
    df.dropna(how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)

    if df.empty:
        raise ValueError("Excel file has no data rows.")

    log.info("Read {} rows from Excel '{}' (sheet: {}).".format(
        len(df), file_path.name, sheet_name
    ))
    return df


def read_pdf(file_path, page_number: int = 0) -> pd.DataFrame:
    """
    Extract a table from a digitally-generated PDF using pdfplumber.

    Reads the first table found starting from page_number, then falls
    through remaining pages if no table is found on the requested page.
    The first row of the extracted table is treated as the column header.

    Only works on native/digital PDFs (ERP printouts, Excel-to-PDF exports).
    Scanned PDFs are not supported — they contain no extractable text layer.

    Parameters
    ----------
    file_path   : str or Path
    page_number : int
        0-indexed page to read first. Default 0 (first page).

    Returns
    -------
    pd.DataFrame with the same normalised shape as read_excel() output.

    Raises
    ------
    ImportError       if pdfplumber is not installed
    FileNotFoundError if the file does not exist
    ValueError        if no table can be found in the PDF
    """
    try:
        import pdfplumber
    except ImportError:
        raise ImportError(
            "pdfplumber is required for PDF processing. "
            "Install it with: pip install pdfplumber"
        )

    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError("File not found: {}".format(file_path))

    table     = None
    page_used = None

    with pdfplumber.open(file_path) as pdf:
        total_pages  = len(pdf.pages)
        # Try the requested page first, then fall through the rest in order
        pages_to_try = [page_number] + [
            i for i in range(total_pages) if i != page_number
        ]
        for page_idx in pages_to_try:
            if page_idx >= total_pages:
                continue
            extracted = pdf.pages[page_idx].extract_table()
            if extracted:
                table     = extracted
                page_used = page_idx
                break

    if table is None:
        raise ValueError(
            "No table found in '{}'. "
            "Ensure the PDF is digitally generated (not scanned) "
            "and contains a structured table.".format(file_path.name)
        )

    # First row becomes column headers; replace blank headers with positional fallback.
    # FIX: str.strip() is applied here during header construction. The original code
    # then repeated df.columns = [str(c).strip() ...] on the resulting DataFrame,
    # which was always a no-op. The second strip has been removed.
    raw_headers = table[0]
    headers = [
        str(h).strip() if h and str(h).strip() else "col_{}".format(i)
        for i, h in enumerate(raw_headers)
    ]

    rows = table[1:]

    if not rows:
        raise ValueError(
            "Table found in '{}' (page {}) has no data rows.".format(
                file_path.name, page_used + 1
            )
        )

    df = pd.DataFrame(rows, columns=headers, dtype=str)
    df.replace("", pd.NA, inplace=True)
    df.dropna(how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)

    if df.empty:
        raise ValueError(
            "PDF table in '{}' contains no data rows after cleaning.".format(
                file_path.name
            )
        )

    log.info("Read {} rows from PDF '{}' (page {}).".format(
        len(df), file_path.name, page_used + 1
    ))
    return df


def read_order_file(file_path, sheet_name=0, page_number: int = 0) -> pd.DataFrame:
    """
    Dispatcher — routes to read_excel() or read_pdf() based on file extension.
    Returns a normalised DataFrame regardless of source format.

    Parameters
    ----------
    file_path   : str or Path
    sheet_name  : int or str   Excel only — sheet index or name. Default 0.
    page_number : int          PDF only   — 0-indexed page number. Default 0.
    """
    ext = Path(file_path).suffix.lower()
    if ext in (".xlsx", ".xls"):
        return read_excel(file_path, sheet_name=sheet_name)
    elif ext == ".pdf":
        return read_pdf(file_path, page_number=page_number)
    else:
        raise ValueError(
            "Unsupported file type: '{}'. "
            "Supported formats: .xlsx, .xls, .pdf".format(ext)
        )


# ─── STEP 3: APPLY MAPPING ───────────────────────────────────────────────────

def apply_mapping(
    df: pd.DataFrame,
    column_map: dict,
    case_insensitive_source: bool = False,
) -> tuple:
    """
    Rename DataFrame columns from customer names to WMS standard names.

    Parameters
    ----------
    df : pd.DataFrame
    column_map : dict
        {customer_column: wms_field} from the config.
    case_insensitive_source : bool
        When True, incoming column names are matched case-insensitively.
        e.g. "docno" matches a config entry of "DocNo". Default False.
    """
    warnings   = []
    rename_map = {}

    # Single unified lookup — lowercased when case_insensitive_source is True
    lookup = (
        {k.lower(): v for k, v in column_map.items()}
        if case_insensitive_source
        else column_map
    )

    for col in df.columns:
        key       = col.lower() if case_insensitive_source else col
        wms_field = lookup.get(key)
        if wms_field:
            rename_map[col] = wms_field
        else:
            warnings.append(
                "UNMAPPED: '{}' is not in the config and will be dropped. "
                "Add it to the config file if it is needed.".format(col)
            )

    df_mapped    = df.rename(columns=rename_map)
    cols_present = [c for c in ALL_WMS_FIELDS if c in df_mapped.columns]
    return df_mapped[cols_present], warnings


# ─── STEP 4: VALIDATE ────────────────────────────────────────────────────────

def validate(df: pd.DataFrame) -> tuple:
    """
    Validate mandatory fields and numeric fields using vectorised operations.
    No row-by-row iteration — all checks use pandas boolean masks.

    Returns
    -------
    df_valid       : pd.DataFrame   rows that passed all checks
    df_errors      : pd.DataFrame   rows that failed, with VALIDATION_ERRORS column
    error_messages : list of str    summary of issues found
    """
    error_messages = []
    row_errors: dict = {}

    def _add_error(idx, msg):
        row_errors.setdefault(idx, []).append(msg)

    # Column-level check: mandatory fields must exist in the dataframe at all
    for field in MANDATORY_FIELDS:
        if field not in df.columns:
            error_messages.append(
                "MISSING COLUMN: Mandatory field '{}' not found in mapped output.".format(field)
            )

    # Mandatory blank checks — vectorised across each column
    for field in MANDATORY_FIELDS:
        if field not in df.columns:
            continue
        blank_mask = (
            df[field].isna() |
            df[field].astype(str).str.strip().isin(["", "nan", "None"])
        )
        for idx in df.index[blank_mask]:
            _add_error(idx, "'{}' is blank".format(field))

    # Numeric field checks — vectorised, only on rows where a value is present
    for field in NUMERIC_FIELDS:
        if field not in df.columns:
            continue
        present_mask = (
            df[field].notna() &
            ~df[field].astype(str).str.strip().isin(["", "nan", "None"])
        )
        if not present_mask.any():
            continue
        cleaned = (
            df.loc[present_mask, field]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        numeric_attempt = pd.to_numeric(cleaned, errors="coerce")
        for idx in numeric_attempt.index[numeric_attempt.isna()]:
            _add_error(idx, "'{}' non-numeric: '{}'".format(field, df.at[idx, field]))

    # Split valid / error rows
    error_indices = list(row_errors.keys())
    valid_indices = [i for i in df.index if i not in error_indices]

    df_valid  = df.loc[valid_indices].copy()
    df_errors = df.loc[error_indices].copy() if error_indices else pd.DataFrame()

    if not df_errors.empty:
        df_errors["VALIDATION_ERRORS"] = df_errors.index.map(
            lambda i: " | ".join(row_errors[i])
        )

    if row_errors:
        error_messages.append(
            "{} row(s) failed validation (source rows: {})".format(
                len(row_errors), [i + 2 for i in error_indices]
            )
        )

    log.info("Validation: {} valid, {} error row(s).".format(len(df_valid), len(df_errors)))
    return df_valid, df_errors, error_messages


# ─── STEP 5: CLEAN DATA ──────────────────────────────────────────────────────

def clean_data(df: pd.DataFrame, date_format: str = None) -> tuple:
    """
    Apply final type cleaning before export.

    - Strip whitespace from all string fields
    - Normalise residual NA string representations to empty string
    - Parse ORDER_DATE using per-customer date_format if provided,
      otherwise infer with dayfirst=True
    - Emit a warning for each row where ORDER_DATE parsing fails
    - Cast numeric fields to float
    - Display ORIG_QTY_ORDERED as integer when it has no fractional part

    Parameters
    ----------
    df          : pd.DataFrame
    date_format : str or None
        strftime format string from customer config, e.g. "%d/%m/%Y".
        When None, pandas infers the date with dayfirst=True.

    Returns
    -------
    (df_clean : pd.DataFrame, warnings : list of str)

    FIX: Previously returned only df_clean. Silent date-parse failures
    (errors="coerce") could produce NaT → empty-string in the output for rows
    that passed validate() with a non-blank ORDER_DATE. There was no feedback
    to the user. Now returns a warnings list with one entry per affected row
    so the caller can surface them in the UI or CLI output.
    """
    df       = df.copy()
    warnings = []

    # Strip whitespace and normalise NA representations to empty string
    for col in df.select_dtypes(include=["str", "object"]).columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .replace({"nan": "", "None": "", "NaN": "", "<NA>": ""})
        )

    # Date parsing — use explicit format from config when available
    if "ORDER_DATE" in df.columns:
        if date_format:
            parsed = pd.to_datetime(df["ORDER_DATE"], format=date_format, errors="coerce")
        else:
            parsed = pd.to_datetime(df["ORDER_DATE"], dayfirst=True, errors="coerce")

        # Identify rows where the source value was non-blank but parsing produced NaT
        failed_mask = parsed.isna() & df["ORDER_DATE"].str.strip().ne("")
        for idx in df.index[failed_mask]:
            warnings.append(
                "DATE PARSE FAILED row {}: ORDER_DATE '{}' could not be parsed{}. "
                "Row exported with empty date.".format(
                    idx + 2,
                    df.at[idx, "ORDER_DATE"],
                    " using format '{}'".format(date_format) if date_format else "",
                )
            )

        # Write formatted dates; rows that failed parsing get an empty string
        df["ORDER_DATE"] = parsed.dt.strftime("%Y-%m-%d").where(parsed.notna(), other="")

    # Numeric fields
    for field in NUMERIC_FIELDS:
        if field not in df.columns:
            continue
        df[field] = pd.to_numeric(
            df[field].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )

    # Display ORIG_QTY_ORDERED as integer when it has no fractional part (1.0 → 1)
    if "ORIG_QTY_ORDERED" in df.columns:
        df["ORIG_QTY_ORDERED"] = df["ORIG_QTY_ORDERED"].apply(
            lambda x: int(x) if pd.notna(x) and x == int(x) else x
        )

    return df, warnings


# ─── STEP 6: EXPORT ──────────────────────────────────────────────────────────

def export_csv(
    df: pd.DataFrame,
    customer_key: str,
    output_dir: Path = OUTPUT_DIR,
) -> Path:
    """Export clean validated DataFrame to a timestamped WMS-ready CSV."""
    output_dir.mkdir(exist_ok=True)   # created lazily — not at import time
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / "wms_output_{}_{}.csv".format(customer_key, ts)
    # FIX: use shared ordered_wms_columns() instead of duplicating the filter expression
    df[ordered_wms_columns(df)].to_csv(out_path, index=False, encoding="utf-8-sig")
    log.info("Exported {} rows to '{}'".format(len(df), out_path))
    return out_path


def export_error_report(
    df_errors: pd.DataFrame,
    customer_key: str,
    output_dir: Path = OUTPUT_DIR,
) -> "Path | None":
    """Export rows that failed validation to a separate error report CSV."""
    if df_errors.empty:
        return None
    output_dir.mkdir(exist_ok=True)   # created lazily — not at import time
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / "wms_errors_{}_{}.csv".format(customer_key, ts)
    df_errors.to_csv(out_path, index=False, encoding="utf-8-sig")
    log.warning("Error report written to '{}'".format(out_path))
    return out_path


def cleanup_output_dir(output_dir: Path = OUTPUT_DIR, keep_days: int = 30) -> int:
    """
    Delete CSV files in output_dir older than keep_days days.

    Parameters
    ----------
    output_dir : Path
    keep_days  : int  Files older than this are deleted. Default 30.

    Returns
    -------
    int  Number of files deleted.
    """
    if not output_dir.exists():
        return 0
    cutoff  = datetime.now().timestamp() - (keep_days * 86400)
    deleted = 0
    for f in output_dir.glob("*.csv"):
        if f.stat().st_mtime < cutoff:
            f.unlink()
            deleted += 1
    if deleted:
        log.info("Cleanup: {} file(s) removed from '{}'".format(deleted, output_dir))
    return deleted


# ─── MAIN PIPELINE ───────────────────────────────────────────────────────────

def run_conversion(
    file_path,
    customer_key: str,
    sheet_name=0,
    page_number: int = 0,
    auto_confirm: bool = False,
    output_dir: Path = OUTPUT_DIR,
) -> dict:
    """
    Full end-to-end conversion pipeline.

    Parameters
    ----------
    file_path    : str or Path   Path to the customer order file (.xlsx or .pdf)
    customer_key : str           Config key matching a file in mappings/
    sheet_name   : int or str    Excel only — sheet index or name. Default 0.
    page_number  : int           PDF only   — 0-indexed page. Default 0.
    auto_confirm : bool          Skip manual review prompt. Default False.
    output_dir   : Path          Where to write output CSVs.

    Returns
    -------
    dict:
        success      : bool
        output_path  : Path or None
        error_path   : Path or None
        valid_rows   : int
        error_rows   : int
        warnings     : list of str
        errors       : list of str
    """
    result = {
        "success":     False,
        "output_path": None,
        "error_path":  None,
        "valid_rows":  0,
        "error_rows":  0,
        "warnings":    [],
        "errors":      [],
    }
    try:
        config              = load_customer_config(customer_key)
        df_raw              = read_order_file(
                                  file_path,
                                  sheet_name=sheet_name,
                                  page_number=page_number,
                              )
        # FIX: case_insensitive_source=True added to match web UI behaviour.
        # Previously the CLI used the default False, meaning the same input file
        # could produce fully-mapped output in the UI but near-empty output via CLI.
        df_mapped, warnings = apply_mapping(
            df_raw, config["column_map"], case_insensitive_source=True
        )
        result["warnings"].extend(warnings)
        for w in warnings:
            log.warning(w)

        df_valid, df_errors, val_errors = validate(df_mapped)
        result["errors"].extend(val_errors)
        result["valid_rows"] = len(df_valid)
        result["error_rows"] = len(df_errors)
        for e in val_errors:
            log.error(e)

        if df_valid.empty:
            result["errors"].append("No valid rows to export.")
            return result

        # FIX: clean_data() now returns (df, warnings); capture and surface both.
        df_clean, clean_warnings = clean_data(df_valid, date_format=config.get("date_format"))
        result["warnings"].extend(clean_warnings)
        for w in clean_warnings:
            log.warning(w)

        if not auto_confirm:
            print("\n" + "=" * 60)
            print("PREVIEW — First 5 rows:")
            print("=" * 60)
            print(df_clean.head(5).to_string(index=False))
            print("\nValid rows  : {}".format(len(df_clean)))
            print("Error rows  : {}".format(len(df_errors)))
            if result["warnings"]:
                print("\nWarnings:")
                for w in result["warnings"]:
                    print("  ! {}".format(w))
            print()
            if input("Export to CSV? (yes / no): ").strip().lower() != "yes":
                print("Export cancelled.")
                return result

        result["output_path"] = export_csv(df_clean, customer_key, output_dir)
        result["error_path"]  = export_error_report(df_errors, customer_key, output_dir)
        result["success"]     = True

    except (FileNotFoundError, ValueError, ImportError) as e:
        result["errors"].append(str(e))
        log.error(str(e))
    return result


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    # FIX: argparse moved from module-level import to inside main().
    # When converter.py is used as a library (imported by app.py), argparse
    # was loaded and held in memory for no purpose. Lazy import keeps the
    # library surface clean.
    import argparse

    parser = argparse.ArgumentParser(description="WMS Order File Converter")
    parser.add_argument("--file",     "-f", required=False,
                        help="Path to the customer order file (.xlsx or .pdf)")
    parser.add_argument("--customer", "-c", required=False,
                        help="Customer config key. Available: {}".format(list_customers()))
    parser.add_argument("--sheet",    "-s", default=0,
                        help="Excel only: sheet name or index (default: 0)")
    # FIX: default changed from 0 to 1 so the help text ("1-based, default: 1")
    # is accurate and the subtraction math max(0, args.page - 1) is self-consistent.
    # With default=0, omitting --page produced args.page=0 → max(0,-1)=0 which
    # worked by accident but was misleading and inconsistent.
    parser.add_argument("--page",     "-p", default=1, type=int,
                        help="PDF only: page number, 1-based (default: 1)")
    parser.add_argument("--yes",      "-y", action="store_true",
                        help="Skip manual review prompt and export immediately")
    parser.add_argument("--cleanup",  action="store_true",
                        help="Delete output files older than 30 days and exit")
    args = parser.parse_args()

    if args.cleanup:
        n = cleanup_output_dir()
        print("Cleanup: {} file(s) removed.".format(n))
        return

    if not args.file or not args.customer:
        parser.error("--file and --customer are required unless --cleanup is used")

    # CLI uses 1-based page numbers for usability; convert to 0-based internally
    page_number = max(0, int(args.page) - 1)

    result = run_conversion(
        args.file, args.customer,
        sheet_name=args.sheet,
        page_number=page_number,
        auto_confirm=args.yes,
    )
    print("\n" + "=" * 60)
    print("RESULT:", "SUCCESS" if result["success"] else "FAILED")
    print("Valid rows : {}".format(result["valid_rows"]))
    print("Error rows : {}".format(result["error_rows"]))
    if result["output_path"]: print("Output     : {}".format(result["output_path"]))
    if result["error_path"]:  print("Errors     : {}".format(result["error_path"]))
    for e in result["errors"]:   print("  x {}".format(e))
    for w in result["warnings"]: print("  ! {}".format(w))


if __name__ == "__main__":
    main()
