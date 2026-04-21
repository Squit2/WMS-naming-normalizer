"""
converter.py
============
WMS Order File Converter - Core Engine

Config loading (load_customer_config, list_customers, validate_all_customer_configs)
has been removed. Configs are now loaded exclusively from SFTP via sftp_config_store.py.
MAPPINGS_DIR has been removed for the same reason.

Pipeline:
  Step 2 — read_order_file        : dispatcher → read_excel() or read_pdf()
         — read_excel             : read .xlsx/.xls into DataFrame
         — read_pdf               : extract table from native PDF into DataFrame
         — is_valid_xlsx          : magic byte check for Excel files
         — is_valid_pdf           : magic byte check for PDF files
  Step 3 — apply_mapping          : rename customer columns to WMS standard names
  Step 4 — validate               : mandatory field + numeric checks (vectorised)
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

# Shared null-string sentinel — used by both converter.py and sftp_config_store.py
NULL_STRINGS = {"", "nan", "None"}

OUTPUT_DIR   = Path(__file__).parent / "output"
MAX_CONFIG_BYTES = 1 * 1024 * 1024   # 1 MB


# ─── SHARED UTILITY ──────────────────────────────────────────────────────────

def ordered_wms_columns(df: pd.DataFrame) -> list:
    """
    Return the subset of ALL_WMS_FIELDS present in df, preserving canonical order.
    Used by export_csv() and app.py to ensure consistent column ordering.
    """
    return [c for c in ALL_WMS_FIELDS if c in df.columns]


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
    keep_default_na=False prevents values like NA/N/A being silently coerced to NaN.
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
    page_number : int   0-indexed page to read first. Default 0 (first page).

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

    for field in MANDATORY_FIELDS:
        if field not in df.columns:
            error_messages.append(
                "MISSING COLUMN: Mandatory field '{}' not found in mapped output.".format(field)
            )

    for field in MANDATORY_FIELDS:
        if field not in df.columns:
            continue
        blank_mask = (
            df[field].isna() |
            df[field].astype(str).str.strip().isin(["", "nan", "None"])
        )
        for idx in df.index[blank_mask]:
            _add_error(idx, "'{}' is blank".format(field))

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

    Returns
    -------
    (df_clean : pd.DataFrame, warnings : list of str)

    Warnings are emitted for each row where ORDER_DATE parsing fails so the
    caller can surface them in the UI rather than silently producing empty dates.
    """
    df       = df.copy()
    warnings = []

    for col in df.select_dtypes(include=["str", "object"]).columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .replace({"nan": "", "None": "", "NaN": "", "<NA>": ""})
        )

    if "ORDER_DATE" in df.columns:
        if date_format:
            parsed = pd.to_datetime(df["ORDER_DATE"], format=date_format, errors="coerce")
        else:
            parsed = pd.to_datetime(df["ORDER_DATE"], dayfirst=True, errors="coerce")

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

        df["ORDER_DATE"] = parsed.dt.strftime("%Y-%m-%d").where(parsed.notna(), other="")

    for field in NUMERIC_FIELDS:
        if field not in df.columns:
            continue
        df[field] = pd.to_numeric(
            df[field].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )

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
    output_dir.mkdir(exist_ok=True)
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / "wms_output_{}_{}.csv".format(customer_key, ts)
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
    output_dir.mkdir(exist_ok=True)
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / "wms_errors_{}_{}.csv".format(customer_key, ts)
    df_errors.to_csv(out_path, index=False, encoding="utf-8-sig")
    log.warning("Error report written to '{}'".format(out_path))
    return out_path


def cleanup_output_dir(output_dir: Path = OUTPUT_DIR, keep_days: int = 30) -> int:
    """Delete CSV files in output_dir older than keep_days days."""
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
