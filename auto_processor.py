"""
auto_processor.py
=================
Background hourly sweep for the WMS Order File Converter.

Starts a single daemon thread (per process) that wakes up every
`interval_seconds` (default 3600), scans the FileZilla SFTP inbox, and
automatically converts + routes any pending files it can match to a
customer config.

Customer matching (priority order)
-----------------------------------
1. The file's stem contains a known customer key (case-insensitive).
   e.g.  "acme_orders_2024.xlsx"  →  customer key "acme"
2. The secret AUTO_PROCESSOR_DEFAULT_CUSTOMER is set → use that key.
3. Neither matches → falls back to the first valid customer config so
   that every Excel file dropped in the inbox is always processed.

Scope
-----
Only .xlsx and .xls files are processed automatically — PDFs are left
in the inbox for manual handling via the UI.

Shared state
------------
Module-level `_state` dict is written by the background thread and read
by the Streamlit UI on every re-run via `get_state()`.  A threading.Lock
makes all reads/writes atomic.

Usage (in app.py)
-----------------
    import auto_processor

    auto_processor.start_background_processor(
        get_customers_fn   = lambda: customers,      # fresh list each sweep
        get_raw_configs_fn = lambda: raw_configs,    # fresh dict each sweep
        interval_seconds   = 3600,
        default_customer   = st.secrets.get("AUTO_PROCESSOR_DEFAULT_CUSTOMER"),
    )

    # Sidebar status
    auto_processor.render_sidebar_status()

    # Trigger an immediate sweep from a UI button
    if st.button("▶ Run now"):
        auto_processor.trigger_immediate_sweep(customers, raw_configs, default_customer)
"""

from __future__ import annotations

import logging
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import streamlit as st

log = logging.getLogger(__name__)

# ── Shared state (survives Streamlit re-runs within the same process) ─────────

_lock        = threading.Lock()
_thread: Optional[threading.Thread] = None
_stop_event  = threading.Event()
_wake_event  = threading.Event()   # Signal thread to skip sleep and run now

_state: dict = {
    "last_run":        None,   # datetime | None
    "next_run":        None,   # datetime | None
    "last_results":    [],     # list[dict] — one entry per file attempted
    "running":         False,  # True while a sweep is in progress
    "total_processed": 0,
    "total_errors":    0,
}


def get_state() -> dict:
    """Return a snapshot of the auto-processor state (thread-safe copy)."""
    with _lock:
        snap = dict(_state)
        snap["last_results"] = list(_state["last_results"])
        return snap


# ── Customer matching ─────────────────────────────────────────────────────────

def _match_customer(
    filename: str,
    customers: list[str],
    default: Optional[str],
) -> Optional[str]:
    """
    Return the best customer key for *filename*.

    Priority:
      1. Filename stem contains a customer key (case-insensitive).
      2. *default* is set.
      3. Fall back to the first entry in *customers* so no Excel file
         is ever silently skipped.
    Returns None only when *customers* is empty.
    """
    stem = Path(filename).stem.lower()
    for key in customers:
        if key.lower() in stem:
            return key
    if default and default in customers:
        return default
    # Always process — use first available valid config as last resort
    return customers[0] if customers else None


# ── Core batch sweep (called by the thread AND by the "Run now" button) ───────

def run_sweep(
    customers:       list[str],
    raw_configs:     dict,
    default_customer: Optional[str] = None,
) -> list[dict]:
    """
    Scan the SFTP inbox and process every pending file that can be matched
    to a valid customer config.

    Returns a list of result dicts (one per file), each containing:
        file, customer, status, message, ts
    """
    # Lazy imports — keeps startup fast and avoids circular deps
    from sftp_router import (
        list_pending_files,
        read_source_file,
        archive_source_file,
        write_processed,
        write_error,
    )
    from sftp_config_store import validate_all_sftp_configs
    from converter import (
        read_order_file,
        apply_mapping,
        validate,
        clean_data,
        ordered_wms_columns,
        is_valid_xlsx,
    )

    results: list[dict] = []

    # Validate all configs once up-front; only use the healthy ones
    all_reports     = validate_all_sftp_configs(raw_configs)
    valid_customers = [c for c in customers if not all_reports.get(c, {}).get("errors")]

    if not valid_customers:
        log.warning("Auto-processor: no valid customer configs — sweep aborted.")
        return results

    # Process all supported file types: .xlsx, .xls, .pdf
    pending = list_pending_files()   # already filtered to SUPPORTED_EXTENSIONS by sftp_router

    log.info("Auto-processor sweep: %d file(s) pending.", len(pending))

    if not pending:
        return results

    for file_meta in pending:
        fname = file_meta["name"]
        entry = {
            "file":     fname,
            "customer": None,
            "status":   None,
            "message":  "",
            "ts":       datetime.now().isoformat(timespec="seconds"),
        }

        # ── 1. Resolve customer config ────────────────────────────────────────
        # Always resolves to something — falls back to first valid config if
        # no filename match and no explicit default is set.
        customer_key = _match_customer(fname, valid_customers, default_customer)
        if customer_key is None:
            # Should only happen when valid_customers is empty (caught above)
            entry["status"]  = "skipped"
            entry["message"] = "No valid customer configs available."
            results.append(entry)
            continue

        entry["customer"] = customer_key
        config            = all_reports[customer_key]["config"]
        ts                = datetime.now().strftime("%Y%m%d_%H%M%S")

        # ── 2. Download + validate ────────────────────────────────────────────
        try:
            raw_bytes = read_source_file(fname)
            ext       = Path(fname).suffix.lower()

            if not is_valid_xlsx(raw_bytes):
                raise ValueError("File does not appear to be a valid Excel workbook.")

        except Exception as exc:
            entry["status"]  = "failed"
            entry["message"] = f"Download/validation error: {exc}"
            results.append(entry)
            log.exception("Auto-processor: failed to download %s", fname)
            continue

        # ── 3. Parse + convert ────────────────────────────────────────────────
        try:
            tmp_path: Optional[Path] = None
            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as _f:
                _f.write(raw_bytes)
                tmp_path = Path(_f.name)

            try:
                df_raw = read_order_file(tmp_path)
            finally:
                if tmp_path:
                    tmp_path.unlink(missing_ok=True)

            df_mapped, _warns              = apply_mapping(
                df_raw, config["column_map"], case_insensitive_source=True
            )
            df_valid, df_errors, val_errors = validate(df_mapped)
            df_clean, _cwarns              = clean_data(
                df_valid, date_format=config.get("date_format")
            )

            cols_ordered  = ordered_wms_columns(df_clean)
            csv_bytes_out = (
                df_clean[cols_ordered].to_csv(index=False).encode("utf-8-sig")
                if not df_clean.empty else b""
            )
            err_bytes_out = (
                df_errors.to_csv(index=False).encode("utf-8-sig")
                if not df_errors.empty else b""
            )

            stem = Path(fname).stem

            # ── 4. Route outputs (mirrors the manual flow's strict rule) ──────
            if err_bytes_out:
                write_error(err_bytes_out, stem, ts)
                entry["status"]  = "error_rows"
                entry["message"] = (
                    f"{len(df_errors)} error row(s) written to Error/. "
                    "Blocked from Processed/ until errors are resolved."
                )
            elif csv_bytes_out:
                write_processed(csv_bytes_out, stem, ts)
                entry["status"]  = "ok"
                entry["message"] = f"{len(df_clean)} valid row(s) written to Processed/."
            else:
                entry["status"]  = "empty"
                entry["message"] = "Conversion produced no output rows."

            # Archive source regardless of outcome
            arc_result = archive_source_file(fname, ts)
            if not arc_result["success"]:
                entry["message"] += f"  ⚠ Archive failed: {arc_result['message']}"

        except Exception as exc:
            entry["status"]  = "failed"
            entry["message"] = f"Processing error: {exc}"
            log.exception("Auto-processor: error processing %s", fname)

        results.append(entry)
        log.info(
            "Auto-processor: %s → %s (%s)",
            fname, entry["status"], entry["message"]
        )

    return results


# ── Background thread loop ────────────────────────────────────────────────────

def _thread_loop(
    interval_seconds:    int,
    get_customers_fn:    Callable[[], list[str]],
    get_raw_configs_fn:  Callable[[], dict],
    default_customer:    Optional[str],
) -> None:
    """
    Runs indefinitely.  Sleeps for *interval_seconds*, wakes up, sweeps,
    repeats.  Responds to _wake_event for immediate runs triggered from the UI.
    """
    global _state

    log.info(
        "Auto-processor thread started (interval=%ds, default_customer=%s).",
        interval_seconds, default_customer,
    )

    # Set the first "next_run" time before entering the wait loop
    with _lock:
        _state["next_run"] = datetime.fromtimestamp(time.time() + interval_seconds)

    while not _stop_event.is_set():
        # Sleep in short ticks — respond to _wake_event or _stop_event quickly
        _wake_event.clear()
        slept = 0
        while slept < interval_seconds and not _stop_event.is_set():
            # Wake up early if the UI triggered an immediate sweep
            woken = _wake_event.wait(timeout=min(10, interval_seconds - slept))
            if woken:
                log.info("Auto-processor: woken by UI trigger.")
                break
            slept += 10

        if _stop_event.is_set():
            break

        # ── Run the sweep ─────────────────────────────────────────────────────
        with _lock:
            _state["running"]  = True
            _state["last_run"] = datetime.now()

        try:
            customers   = get_customers_fn()
            raw_configs = get_raw_configs_fn()
            results     = run_sweep(customers, raw_configs, default_customer)
        except Exception as exc:
            log.exception("Auto-processor sweep raised an unhandled error: %s", exc)
            results = []

        ok_count  = sum(1 for r in results if r["status"] == "ok")
        err_count = sum(1 for r in results if r["status"] in ("error_rows", "failed"))

        with _lock:
            _state["running"]          = False
            _state["last_results"]     = results
            _state["next_run"]         = datetime.fromtimestamp(time.time() + interval_seconds)
            _state["total_processed"] += ok_count
            _state["total_errors"]    += err_count

        log.info(
            "Auto-processor sweep complete: %d ok, %d error(s), %d skipped.",
            ok_count, err_count,
            sum(1 for r in results if r["status"] == "skipped"),
        )


# ── Public API ────────────────────────────────────────────────────────────────

def start_background_processor(
    get_customers_fn:   Callable[[], list[str]],
    get_raw_configs_fn: Callable[[], dict],
    interval_seconds:   int = 3600,
    default_customer:   Optional[str] = None,
) -> None:
    """
    Start the background hourly thread.  Idempotent — safe to call on every
    Streamlit re-run; the thread is only created once per process.
    """
    global _thread

    with _lock:
        if _thread is not None and _thread.is_alive():
            return  # Already running — do nothing

        _stop_event.clear()
        _thread = threading.Thread(
            target   = _thread_loop,
            args     = (interval_seconds, get_customers_fn, get_raw_configs_fn, default_customer),
            daemon   = True,
            name     = "sftp-auto-processor",
        )
        _thread.start()
        log.info("Auto-processor background thread launched.")


def trigger_immediate_sweep(
    customers:        list[str],
    raw_configs:      dict,
    default_customer: Optional[str] = None,
) -> list[dict]:
    """
    Run a sweep immediately (blocking, in the Streamlit UI thread).
    Updates shared state so the sidebar status refreshes automatically.
    """
    with _lock:
        _state["running"]  = True
        _state["last_run"] = datetime.now()

    try:
        results = run_sweep(customers, raw_configs, default_customer)
    except Exception as exc:
        log.exception("Immediate sweep failed: %s", exc)
        results = []

    ok_count  = sum(1 for r in results if r["status"] == "ok")
    err_count = sum(1 for r in results if r["status"] in ("error_rows", "failed"))

    with _lock:
        _state["running"]          = False
        _state["last_results"]     = results
        _state["total_processed"] += ok_count
        _state["total_errors"]    += err_count

    # Also wake the background thread so it resets its sleep timer,
    # avoiding a double-sweep shortly after a manual trigger.
    _wake_event.set()

    return results


# ── Streamlit sidebar widget ──────────────────────────────────────────────────

_STATUS_ICON = {
    "ok":         "✅",
    "error_rows": "⚠️",
    "failed":     "❌",
    "skipped":    "⏭",
    "empty":      "⬜",
}


def render_sidebar_status() -> None:
    """
    Render the auto-processor status block inside st.sidebar.
    Call this from the sidebar section of app.py.
    """
    snap = get_state()

    st.subheader("🤖 Auto-processor")

    if snap["running"]:
        st.info("⏳ Sweep in progress…")
    else:
        last = snap["last_run"]
        nxt  = snap["next_run"]
        st.caption(
            f"Last run: **{last.strftime('%H:%M %d %b') if last else 'Never'}**  \n"
            f"Next run: **{nxt.strftime('%H:%M %d %b') if nxt else 'Pending…'}**"
        )

    col1, col2 = st.columns(2)
    col1.metric("✅ Total processed",  snap["total_processed"])
    col2.metric("⚠️ Total w/ errors",  snap["total_errors"])

    if snap["last_results"]:
        with st.expander(f"Last sweep — {len(snap['last_results'])} file(s)"):
            for r in snap["last_results"]:
                icon = _STATUS_ICON.get(r["status"], "❓")
                cust = f"[{r['customer']}]" if r["customer"] else ""
                st.markdown(
                    f"{icon} **{r['file']}** {cust}  \n"
                    f"<span style='font-size:0.85em;color:grey'>{r['message']}</span>",
                    unsafe_allow_html=True,
                )
    else:
        st.caption("No sweep results yet.")
