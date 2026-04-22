# sftp_config_store.py
"""
sftp_config_store.py
====================
SFTP-backed persistence layer for WMS customer mapping configs.

Changes
-------
Issue 1 — Connection pooling via @st.cache_resource.
    A single paramiko Transport is opened once per Streamlit server process
    and reused for all subsequent SFTP operations, eliminating the per-call
    SSH handshake overhead (300-800ms per operation).

Issue 2 — Lazy per-config fetch instead of bulk fetch.
    list_sftp_config_keys() fetches only filenames (one SFTP listdir call).
    fetch_raw_config() fetches a single config by key on demand and caches
    it individually. fetch_all_raw_configs() is retained for compatibility
    but now delegates to the per-key cache rather than reading everything
    on every miss.

Issue 6 — NULL_STRINGS imported from converter instead of redefined.
"""

import contextlib
import logging
from io import StringIO

import pandas as pd
import streamlit as st

from converter import ALL_WMS_FIELDS, MANDATORY_FIELDS, NULL_STRINGS

# ─── INTERNALS ───────────────────────────────────────────────────────────────

log = logging.getLogger(__name__)

_DEFAULT_MAPPINGS_DIR = "/mappings"


def is_sftp_configured() -> bool:
    """
    Return True if the minimum SFTP credentials are present in st.secrets.
    Pure key check — no network connection is made.
    """
    try:
        host     = st.secrets.get("SFTP_HOST", "").strip()
        username = st.secrets.get("SFTP_USERNAME", "").strip()
        password = st.secrets.get("SFTP_PASSWORD", "").strip()
        return bool(host and username and password)
    except Exception:
        return False


def _sftp_credentials() -> tuple:
    """
    Read SFTP credentials from st.secrets.

    Returns
    -------
    (host, port, user, password, remote_dir)
    """
    host       = st.secrets.get("SFTP_HOST", "").strip()
    port       = int(st.secrets.get("SFTP_PORT", 22))
    user       = st.secrets.get("SFTP_USERNAME", "").strip()
    password   = st.secrets.get("SFTP_PASSWORD", "").strip() or None
    remote_dir = st.secrets.get(
        "SFTP_MAPPINGS_DIR", _DEFAULT_MAPPINGS_DIR
    ).strip().rstrip("/")

    if not host or not user:
        raise ValueError(
            "SFTP credentials are not configured. "
            "Set SFTP_HOST and SFTP_USERNAME in Streamlit secrets."
        )
    if not password:
        raise ValueError(
            "No SFTP authentication method configured. "
            "Set SFTP_PASSWORD in Streamlit secrets."
        )

    return host, port, user, password, remote_dir


# ─── ISSUE 1: CONNECTION POOL ────────────────────────────────────────────────
# @st.cache_resource persists the transport for the lifetime of the Streamlit
# server process — shared across all reruns and all users on the same instance.
# This means one SSH handshake instead of one per SFTP operation.
#
# get_sftp_transport() returns a live paramiko Transport. Callers must not
# close it — it is owned by the cache. If the transport drops (server restart,
# network interruption), clear_sftp_transport() forces a reconnect.

@st.cache_resource(show_spinner=False)
def _get_sftp_transport():
    """
    Open and cache a single paramiko Transport for the process lifetime.
    Called lazily on first SFTP operation. Never call .close() on the result.
    """
    try:
        import paramiko
    except ImportError:
        raise ImportError(
            "paramiko is required for SFTP operations. "
            "Install it with: pip install paramiko"
        )

    host, port, user, password, _ = _sftp_credentials()

    transport = paramiko.Transport((host, port))
    try:
        transport.connect()
        transport.auth_password(user, password)
    except paramiko.AuthenticationException:
        transport.close()
        raise RuntimeError(
            "SFTP authentication failed. Check SFTP_USERNAME and SFTP_PASSWORD."
        )
    except (paramiko.SSHException, OSError) as exc:
        transport.close()
        raise RuntimeError("SFTP connection error: {}".format(exc))

    log.info("SFTP transport established to %s:%s", host, port)
    return transport


def clear_sftp_transport():
    """
    Force the cached transport to be closed and re-established on the next
    SFTP call. Call this after a connection error to trigger reconnection.
    """
    _get_sftp_transport.clear()


@contextlib.contextmanager
def _sftp_session():
    """
    Context manager that yields an open SFTPClient and the configured
    remote directory path, using the cached transport.

    On transport failure, clears the transport cache so the next call
    reconnects rather than reusing a dead connection.

    Usage
    -----
    with _sftp_session() as (sftp, remote_dir):
        sftp.listdir(remote_dir)
    """
    import paramiko

    _, _, _, _, remote_dir = _sftp_credentials()
    transport = _get_sftp_transport()

    # If the cached transport has gone stale, clear it and raise so the
    # caller can retry or surface the error.
    if not transport.is_active():
        clear_sftp_transport()
        raise RuntimeError(
            "SFTP transport dropped. Reload the page to reconnect."
        )

    sftp = None
    try:
        sftp = paramiko.SFTPClient.from_transport(transport)
        yield sftp, remote_dir
    except paramiko.SSHException as exc:
        clear_sftp_transport()
        raise RuntimeError("SFTP SSH error: {}".format(exc))
    except OSError as exc:
        clear_sftp_transport()
        raise RuntimeError("SFTP connection error: {}".format(exc))
    finally:
        if sftp:
            sftp.close()


# ─── ISSUE 2: LAZY PER-CONFIG FETCH ─────────────────────────────────────────
# Previously fetch_all_raw_configs() read every config file on every cache miss.
# Now:
#   list_sftp_config_keys()  — fetches filenames only (one listdir call), TTL 60s
#   fetch_raw_config()       — fetches a single config by key, TTL 300s per entry
#   fetch_all_raw_configs()  — assembles the full dict from per-key cache entries
#                              (retained for API compatibility with app.py)

@st.cache_data(ttl=60, show_spinner=False)
def list_sftp_config_keys() -> list:
    """
    Return sorted list of customer keys from SFTP by listing filenames only.
    Cached for 60 seconds. Much cheaper than reading every file — one listdir
    call versus N file reads.
    """
    with _sftp_session() as (sftp, remote_dir):
        try:
            entries = sftp.listdir(remote_dir)
        except IOError:
            log.warning("SFTP mappings directory '%s' not found.", remote_dir)
            return []

    keys = [
        f[:-4] for f in entries
        if f.endswith(".csv") and f.lower() != "template.csv"
    ]
    log.info("Listed %d config key(s) from SFTP.", len(keys))
    return sorted(keys)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_raw_config(customer_key: str) -> str:
    """
    Fetch a single config file from SFTP and return its content as a UTF-8 string.
    Cached individually per customer key for 5 minutes.

    Parameters
    ----------
    customer_key : str   Config filename stem, e.g. "gigly_gulp"

    Returns
    -------
    str   Raw CSV content, or empty string if the file cannot be read.
    """
    with _sftp_session() as (sftp, remote_dir):
        remote_path = "{}/{}.csv".format(remote_dir, customer_key)
        try:
            with sftp.open(remote_path, "r") as fh:
                raw = fh.read()
                return raw.decode("utf-8-sig")
        except (IOError, UnicodeDecodeError) as exc:
            log.warning("Could not read config '%s.csv': %s", customer_key, exc)
            return ""


@st.cache_data(ttl=300, show_spinner=False)
def fetch_all_raw_configs() -> dict:
    """
    Load every .csv mapping config from the SFTP /mappings/ directory.

    Delegates to list_sftp_config_keys() + fetch_raw_config() so each
    config is fetched and cached individually. A new config being added
    only invalidates the key listing cache (60s TTL), not all config content.

    Returns
    -------
    dict : {customer_key: csv_string}
    """
    keys        = list_sftp_config_keys()
    raw_configs = {}
    for key in keys:
        content = fetch_raw_config(key)
        if content:
            raw_configs[key] = content
    log.info("Assembled %d config(s) from per-key cache.", len(raw_configs))
    return raw_configs


# ─── PARSE ────────────────────────────────────────────────────────────────────

def parse_config_from_csv_string(csv_string: str, customer_key: str) -> dict:
    """
    Parse a CSV mapping config string into the standard config dict.

    Expected CSV structure
    ----------------------
    customer_column | wms_field   | customer_name       | date_format
    DocNo           | ORDER_REF   | Gigly Gulp Sdn Bhd  | %d/%m/%Y
    DocDate         | ORDER_DATE  |                     |

    Required columns : customer_column, wms_field
    Optional columns : customer_name, date_format
    """
    try:
        df = pd.read_csv(StringIO(csv_string), dtype=str)
    except Exception as exc:
        raise ValueError(
            "Could not parse config for '{}': {}".format(customer_key, exc)
        )

    df.columns = [c.strip().lower() for c in df.columns]

    required = {"customer_column", "wms_field"}
    missing  = required - set(df.columns)
    if missing:
        raise ValueError(
            "Config '{}' is missing required columns: {}. "
            "Required headers: customer_column, wms_field.".format(
                customer_key, sorted(missing)
            )
        )

    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

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

    df = df[["customer_column", "wms_field"]].copy()

    # Issue 6: use NULL_STRINGS imported from converter instead of redefining
    df = df[
        ~df["customer_column"].isin(NULL_STRINGS) &
        ~df["wms_field"].isin(NULL_STRINGS)
    ]

    if df.empty:
        raise ValueError(
            "Config '{}' has no valid mapping rows.".format(customer_key)
        )

    dupes = df[df["customer_column"].duplicated()]["customer_column"].tolist()
    if dupes:
        raise ValueError(
            "Config '{}' has duplicate customer_column entries: {}. "
            "Each source column may appear only once.".format(
                customer_key, dupes
            )
        )

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

    Missing mandatory WMS field mappings are classified as errors so that
    config_is_valid stays False when mandatory fields are absent.

    Returns
    -------
    dict : {customer_key: {"errors": [...], "warnings": [...], "config": dict or None}}
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
    Writes as UTF-8 bytes in binary mode to avoid double line endings on Windows.
    """
    try:
        with _sftp_session() as (sftp, remote_dir):
            remote_path = "{}/{}.csv".format(remote_dir, customer_key)

            try:
                sftp.stat(remote_dir)
            except IOError:
                sftp.mkdir(remote_dir)

            # Binary mode + explicit encoding avoids \r\r\n on Windows
            with sftp.open(remote_path, "wb") as fh:
                fh.write(csv_string.encode("utf-8-sig"))

        # Invalidate the per-key cache for this config so the next read
        # reflects the new content without waiting for TTL expiry.
        fetch_raw_config.clear()
        fetch_all_raw_configs.clear()

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

    new_mappings entries override existing entries on customer_column conflict.
    rows_added reflects genuinely new columns only (not overwrites).
    """
    if not new_mappings:
        return {
            "success": False,
            "message": "No new mappings to save.",
            "rows_added": 0,
        }

    if existing_csv_string.strip():
        try:
            df_existing = pd.read_csv(StringIO(existing_csv_string), dtype=str)
            df_existing.columns = [c.strip().lower() for c in df_existing.columns]

            if "customer_column" not in df_existing.columns or \
               "wms_field" not in df_existing.columns:
                df_existing = pd.DataFrame(columns=["customer_column", "wms_field"])
            else:
                df_existing = df_existing[["customer_column", "wms_field"]].copy()

        except Exception:
            df_existing = pd.DataFrame(columns=["customer_column", "wms_field"])
    else:
        df_existing = pd.DataFrame(columns=["customer_column", "wms_field"])

    df_new = pd.DataFrame(
        [{"customer_column": k, "wms_field": v} for k, v in new_mappings.items()]
    )

    df_merged = (
        pd.concat([df_existing, df_new], ignore_index=True)
        .drop_duplicates(subset=["customer_column"], keep="last")
        .reset_index(drop=True)
    )

    # Count genuinely new columns (not overwrites of existing ones)
    existing_cols = set(df_existing["customer_column"].tolist())
    new_cols      = set(new_mappings.keys())
    rows_added    = len(new_cols - existing_cols)

    csv_out = df_merged.to_csv(index=False)
    result  = upload_config_csv(customer_key, csv_out)
    result["rows_added"] = rows_added
    return result
