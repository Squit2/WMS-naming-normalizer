# sftp_config_store.py
"""
sftp_config_store.py
====================
SFTP-backed persistence layer for WMS customer mapping configs.

Replaces the local mappings/ directory with .csv configs stored on an SFTP
server, enabling persistence across Streamlit restarts (ephemeral filesystem).

Directory layout on SFTP server
--------------------------------
/mappings/           ← default; override with SFTP_MAPPINGS_DIR secret
  customer_a.csv
  customer_b.csv
  template.csv       ← always skipped

Required Streamlit secrets
--------------------------
  SFTP_HOST         hostname or IP of the SFTP server
  SFTP_USERNAME     SSH username
  SFTP_PASSWORD     SSH password

Optional Streamlit secrets
--------------------------
  SFTP_PORT         default 22
  SFTP_MAPPINGS_DIR default /mappings

Public API
----------
  fetch_all_raw_configs()                → {key: csv_str}   (cached, TTL 5 min)
  parse_config_from_csv_string(s, key)  → config dict
  validate_all_sftp_configs(raw)        → {key: report}
  list_sftp_customers(raw)              → [key, ...]
  upload_config_csv(key, csv_str)       → {success, message}
  merge_and_save_mappings(key, old, new)→ {success, message}
"""

import contextlib
import logging
from io import StringIO

import pandas as pd
import streamlit as st

from converter import ALL_WMS_FIELDS, MANDATORY_FIELDS

# ─── INTERNALS ───────────────────────────────────────────────────────────────

log = logging.getLogger(__name__)

_DEFAULT_MAPPINGS_DIR = "/mappings"
_NULL_STRINGS         = {"", "nan", "None"}


def _sftp_credentials() -> tuple:
    """
    Read SFTP credentials from st.secrets.

    Returns
    -------
    (host, port, user, password, remote_dir)

    Raises
    ------
    ValueError if mandatory credentials are absent.
    """
    host       = st.secrets.get("SFTP_HOST", "").strip()
    port       = int(st.secrets.get("SFTP_PORT", 50))
    user       = st.secrets.get("SFTP_USERNAME", "").strip()
    password   = st.secrets.get("SFTP_PASSWORD", "").strip() or None
    remote_dir = st.secrets.get(
        "SFTP_MAPPINGS_DIR", _DEFAULT_MAPPINGS_DIR
    ).strip().rstrip("/")

    if not host or not user:
        raise ValueError(
            "SFTP credentials are not configured. "
            "Set SFTP_HOST and SFTP_USERNAME in Streamlit secrets (.streamlit/secrets.toml)."
        )
    if not password:
        raise ValueError(
            "No SFTP authentication method configured. "
            "Set SFTP_PASSWORD in Streamlit secrets."
        )

    return host, port, user, password, remote_dir


@contextlib.contextmanager
def _sftp_session():
    """
    Context manager that yields an open paramiko SFTPClient and the
    configured remote directory path. Guarantees transport closure on exit.

    Usage
    -----
    with _sftp_session() as (sftp, remote_dir):
        sftp.listdir(remote_dir)
    """
    try:
        import paramiko
    except ImportError:
        raise ImportError(
            "paramiko is required for SFTP operations. "
            "Install it with: pip install paramiko"
        )

    host, port, user, password, remote_dir = _sftp_credentials()

    transport = paramiko.Transport((host, port))
    sftp      = None
    try:
        transport.connect()
        transport.auth_password(user, password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        yield sftp, remote_dir
    except paramiko.AuthenticationException:
        raise RuntimeError(
            "SFTP authentication failed. Check SFTP_USERNAME and SFTP_PASSWORD."
        )
    except paramiko.SSHException as exc:
        raise RuntimeError("SFTP SSH error: {}".format(exc))
    except OSError as exc:
        raise RuntimeError("SFTP connection error: {}".format(exc))
    finally:
        if sftp:
            sftp.close()
        if transport.is_active():
            transport.close()


# ─── FETCH (cached) ───────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def fetch_all_raw_configs() -> dict:
    """
    Load every .csv mapping config from the SFTP /mappings/ directory.

    Each file is read as UTF-8 (with BOM tolerance) and stored verbatim
    as a string, keyed by the filename stem (i.e. filename minus ".csv").
    "template.csv" is always skipped.

    Returns
    -------
    dict : {customer_key: csv_string}

    Cached for 5 minutes (ttl=300) to avoid round-tripping to SFTP on
    every Streamlit rerun. Call fetch_all_raw_configs.clear() to bust
    the cache immediately after a write operation.

    Raises
    ------
    RuntimeError on SFTP connection / auth failure.
    """
    raw_configs = {}

    with _sftp_session() as (sftp, remote_dir):
        # Ensure the mappings directory exists
        try:
            entries = sftp.listdir(remote_dir)
        except IOError:
            log.warning(
                "SFTP mappings directory '%s' does not exist — "
                "returning empty config set.",
                remote_dir,
            )
            return {}

        for filename in entries:
            if not filename.endswith(".csv"):
                continue
            if filename.lower() == "template.csv":
                continue

            customer_key = filename[:-4]   # strip .csv extension
            remote_path  = "{}/{}".format(remote_dir, filename)

            try:
                with sftp.open(remote_path, "r") as fh:
                    raw = fh.read()
                    # Tolerate UTF-8 BOM that Excel sometimes writes
                    raw_configs[customer_key] = raw.decode("utf-8-sig")
            except (IOError, UnicodeDecodeError) as exc:
                log.warning(
                    "Could not read config '%s': %s — skipped.", filename, exc
                )

    log.info("Fetched %d config(s) from SFTP.", len(raw_configs))
    return raw_configs


# ─── PARSE ────────────────────────────────────────────────────────────────────

def parse_config_from_csv_string(csv_string: str, customer_key: str) -> dict:
    """
    Parse a CSV mapping config string into the standard config dict.

    Mirrors converter.load_customer_config() but operates on an in-memory
    string rather than a filesystem path, so no local files are needed.

    Expected CSV structure
    ----------------------
    customer_column | wms_field   | customer_name       | date_format
    DocNo           | ORDER_REF   | Gigly Gulp Sdn Bhd  | %d/%m/%Y
    DocDate         | ORDER_DATE  |                     |
    ...

    Required columns : customer_column, wms_field
    Optional columns : customer_name (first non-blank value wins)
                       date_format   (first non-blank value wins)

    Parameters
    ----------
    csv_string   : str   Raw CSV content
    customer_key : str   Used for error messages and name derivation

    Returns
    -------
    dict :
        customer_name : str
        column_map    : dict {customer_column: wms_field}
        date_format   : str or None

    Raises
    ------
    ValueError on any structural problem with the config.
    """
    try:
        df = pd.read_csv(StringIO(csv_string), dtype=str)
    except Exception as exc:
        raise ValueError(
            "Could not parse config for '{}': {}".format(customer_key, exc)
        )

    # Normalise headers
    df.columns = [c.strip().lower() for c in df.columns]

    # Validate required columns
    required = {"customer_column", "wms_field"}
    missing  = required - set(df.columns)
    if missing:
        raise ValueError(
            "Config '{}' is missing required columns: {}. "
            "Required headers: customer_column, wms_field.".format(
                customer_key, sorted(missing)
            )
        )

    # Strip all cell values
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    # Extract optional metadata — first non-blank value in each column
    def _first_value(col_name):
        if col_name not in df.columns:
            return None
        vals = (
            df[col_name]
            .replace({"nan": None, "None": None, "": None})
            .dropna()
        )
        return vals.iloc[0] if not vals.empty else None

    customer_name = (
        _first_value("customer_name")
        or customer_key.replace("_", " ").title()
    )
    date_format = _first_value("date_format")

    # Keep only the two mapping columns
    df = df[["customer_column", "wms_field"]].copy()

    # Drop rows where either column is blank / null-like
    df = df[
        ~df["customer_column"].isin(_NULL_STRINGS) &
        ~df["wms_field"].isin(_NULL_STRINGS)
    ]

    if df.empty:
        raise ValueError(
            "Config '{}' has no valid mapping rows.".format(customer_key)
        )

    # Duplicate customer_column entries are an error — last-write-wins is silent
    dupes = df[df["customer_column"].duplicated()]["customer_column"].tolist()
    if dupes:
        raise ValueError(
            "Config '{}' has duplicate customer_column entries: {}. "
            "Each source column may appear only once.".format(
                customer_key, dupes
            )
        )

    # All wms_field values must be recognised WMS fields
    invalid = [v for v in df["wms_field"] if v not in ALL_WMS_FIELDS]
    if invalid:
        raise ValueError(
            "Config '{}' contains unrecognised WMS fields: {}. "
            "Valid fields: {}.".format(customer_key, invalid, ALL_WMS_FIELDS)
        )

    column_map = dict(zip(df["customer_column"], df["wms_field"]))
    log.info(
        "Parsed config '%s' — %d mapping(s) for %s.",
        customer_key, len(column_map), customer_name,
    )
    return {
        "customer_name": customer_name,
        "column_map":    column_map,
        "date_format":   date_format,
    }


# ─── VALIDATE ─────────────────────────────────────────────────────────────────

def validate_all_sftp_configs(raw_configs: dict) -> dict:
    """
    Validate every SFTP-loaded config and return a health report.

    Mirrors converter.validate_all_customer_configs() but operates on the
    in-memory dict from fetch_all_raw_configs() rather than local files.

    Missing mandatory WMS field mappings are classified as *errors* (not
    warnings) so that config_is_valid stays False when mandatory fields
    are absent — consistent with what validate() would raise at runtime.

    Parameters
    ----------
    raw_configs : dict   {customer_key: csv_string} from fetch_all_raw_configs()

    Returns
    -------
    dict :
        {
          customer_key: {
            "errors":   [str, ...],
            "warnings": [str, ...],
            "config":   dict or None   ← None when errors is non-empty
          }
        }
    """
    reports = {}

    for customer_key, csv_string in raw_configs.items():
        errors   = []
        warnings = []
        config   = None

        try:
            config     = parse_config_from_csv_string(csv_string, customer_key)
            mapped_wms = set(config["column_map"].values())

            for field in MANDATORY_FIELDS:
                if field not in mapped_wms:
                    errors.append(
                        "Mandatory field '{}' is not mapped in this config.".format(field)
                    )

        except ValueError as exc:
            errors.append(str(exc))

        reports[customer_key] = {
            "errors":   errors,
            "warnings": warnings,
            # Only expose the config object when fully valid
            "config":   config if not errors else None,
        }

    return reports


def list_sftp_customers(raw_configs: dict) -> list:
    """Return a sorted list of customer keys from the SFTP-loaded config dict."""
    return sorted(raw_configs.keys())


# ─── WRITE BACK ───────────────────────────────────────────────────────────────

def upload_config_csv(customer_key: str, csv_string: str) -> dict:
    """
    Write (or overwrite) a mapping config on the SFTP server.

    The file is written to: {SFTP_MAPPINGS_DIR}/{customer_key}.csv

    Parameters
    ----------
    customer_key : str   Config key (filename stem)
    csv_string   : str   CSV content to write

    Returns
    -------
    dict : {success: bool, message: str}
    """
    try:
        with _sftp_session() as (sftp, remote_dir):
            remote_path = "{}/{}.csv".format(remote_dir, customer_key)

            # Ensure mappings directory exists
            try:
                sftp.stat(remote_dir)
            except IOError:
                sftp.mkdir(remote_dir)

            with sftp.open(remote_path, "w") as fh:
                fh.write(csv_string)

        log.info("Uploaded config '%s.csv' to SFTP.", customer_key)
        return {
            "success": True,
            "message": "Config '{}' saved to SFTP ({}.csv).".format(
                customer_key, customer_key
            ),
        }

    except (RuntimeError, ImportError, ValueError) as exc:
        return {"success": False, "message": str(exc)}
    except Exception as exc:
        return {
            "success": False,
            "message": "Unexpected error uploading config '{}': {}".format(
                customer_key, exc
            ),
        }


def merge_and_save_mappings(
    customer_key: str,
    existing_csv_string: str,
    new_mappings: dict,
) -> dict:
    """
    Merge UI-defined column mappings into the existing config and save to SFTP.

    Rules
    -----
    - new_mappings entries override existing entries on customer_column conflict.
    - Deduplication is based on customer_column; last occurrence wins after concat.
    - Only validated WMS fields are accepted in new_mappings (caller responsibility).

    Parameters
    ----------
    customer_key        : str    Config key
    existing_csv_string : str    Current raw CSV from SFTP (may be empty string)
    new_mappings        : dict   {customer_column: wms_field} from UI

    Returns
    -------
    dict : {success: bool, message: str, rows_added: int}
    """
    if not new_mappings:
        return {
            "success": False,
            "message": "No new mappings to save.",
            "rows_added": 0,
        }

    # ── Load existing config ──────────────────────────────────────────────────
    if existing_csv_string.strip():
        try:
            df_existing = pd.read_csv(StringIO(existing_csv_string), dtype=str)
            df_existing.columns = [c.strip().lower() for c in df_existing.columns]

            # Ensure the required columns exist after normalisation
            if "customer_column" not in df_existing.columns or \
               "wms_field" not in df_existing.columns:
                df_existing = pd.DataFrame(columns=["customer_column", "wms_field"])
            else:
                df_existing = df_existing[["customer_column", "wms_field"]].copy()

        except Exception:
            df_existing = pd.DataFrame(columns=["customer_column", "wms_field"])
    else:
        df_existing = pd.DataFrame(columns=["customer_column", "wms_field"])

    # ── Build new rows ────────────────────────────────────────────────────────
    df_new = pd.DataFrame(
        [{"customer_column": k, "wms_field": v} for k, v in new_mappings.items()]
    )

    # ── Merge: existing first, new last → drop_duplicates(keep="last") ───────
    df_merged = (
        pd.concat([df_existing, df_new], ignore_index=True)
        .drop_duplicates(subset=["customer_column"], keep="last")
        .reset_index(drop=True)
    )

    rows_added = len(df_merged) - len(df_existing)
    csv_out    = df_merged.to_csv(index=False)

    result = upload_config_csv(customer_key, csv_out)
    result["rows_added"] = max(rows_added, 0)
    return result
