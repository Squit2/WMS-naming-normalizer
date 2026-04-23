# sftp_router.py
import contextlib
import logging
from io import BytesIO
from datetime import datetime
from pathlib import Path

import streamlit as st

log = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS = {".xlsx", ".xls", ".pdf"}

def _get_workspace() -> str:
    return st.secrets.get("SFTP_WORKSPACE_DIR", "/client_mocks").strip().rstrip("/")

@contextlib.contextmanager
def _sftp_session():
    """Context manager for SFTP connections using st.secrets."""
    import paramiko
    host = st.secrets.get("SFTP_HOST", "").strip()
    port = int(st.secrets.get("SFTP_PORT", 22))
    user = st.secrets.get("SFTP_USERNAME", "").strip()
    pwd  = st.secrets.get("SFTP_PASSWORD", "").strip()

    transport = paramiko.Transport((host, port))
    sftp = None
    try:
        transport.connect()
        transport.auth_password(user, pwd)
        sftp = paramiko.SFTPClient.from_transport(transport)
        yield sftp
    finally:
        if sftp: sftp.close()
        if transport.is_active(): transport.close()

def ensure_workspace_folders() -> None:
    """Create the 4 main workspace folders on FileZilla if missing."""
    ws = _get_workspace()
    folders = [ws, f"{ws}/Archive", f"{ws}/Processed", f"{ws}/Error"]
    
    with _sftp_session() as sftp:
        for folder in folders:
            try:
                sftp.stat(folder)
            except IOError:
                sftp.mkdir(folder)
                log.info("Created remote directory: %s", folder)

def list_pending_files() -> list:
    """Return a list of supported file metadata dicts from the inbox."""
    ws = _get_workspace()
    files = []
    with _sftp_session() as sftp:
        try:
            for attr in sftp.listdir_attr(ws):
                # S_ISREG check (is it a regular file, not a folder)
                if not str(attr).startswith("d"): 
                    ext = Path(attr.filename).suffix.lower()
                    if ext in SUPPORTED_EXTENSIONS:
                        files.append({
                            "name": attr.filename,
                            "size": attr.st_size,
                            "mtime": attr.st_mtime
                        })
        except IOError:
            pass
            
    # Sort oldest first to preserve processing queue order
    return sorted(files, key=lambda f: f["mtime"])

def folder_stats() -> dict:
    """Return file counts and sizes for the sidebar metrics."""
    ws = _get_workspace()
    def _stat_dir(sftp, path, exts=None):
        try:
            attrs = sftp.listdir_attr(path)
            valid = [a for a in attrs if not str(a).startswith("d") and 
                     (exts is None or Path(a.filename).suffix.lower() in exts)]
            total_b = sum(a.st_size for a in valid)
            return {"count": len(valid), "size_kb": round(total_b / 1024, 1)}
        except IOError:
            return {"count": 0, "size_kb": 0.0}

    with _sftp_session() as sftp:
        return {
            "pending":   _stat_dir(sftp, ws, SUPPORTED_EXTENSIONS),
            "archive":   _stat_dir(sftp, f"{ws}/Archive"),
            "processed": _stat_dir(sftp, f"{ws}/Processed", {".csv"}),
            "errors":    _stat_dir(sftp, f"{ws}/Error", {".csv"}),
        }

def read_source_file(filename: str) -> bytes:
    """Download the raw bytes of a pending file into memory."""
    ws = _get_workspace()
    with _sftp_session() as sftp:
        with sftp.open(f"{ws}/{filename}", "rb") as f:
            return f.read()

def make_output_stem(original_name: str, ts: str) -> str:
    stem = Path(original_name).stem
    return f"{stem}_{ts}"

def archive_source_file(filename: str, ts: str) -> dict:
    """Instantly move the file on the server from inbox to Archive/."""
    ws = _get_workspace()
    stem = Path(filename).stem
    ext = Path(filename).suffix
    arc_name = f"{stem}_{ts}{ext}"
    
    src  = f"{ws}/{filename}"
    dest = f"{ws}/Archive/{arc_name}"
    
    try:
        with _sftp_session() as sftp:
            sftp.rename(src, dest)
        return {"success": True, "message": f"Archived to Archive/{arc_name}"}
    except Exception as e:
        return {"success": False, "message": f"Archive failed: {e}"}

def write_processed(csv_bytes: bytes, stem: str, ts: str) -> dict:
    ws = _get_workspace()
    out_name = f"{stem}_{ts}.csv"
    dest = f"{ws}/Processed/{out_name}"
    
    try:
        with _sftp_session() as sftp:
            sftp.putfo(BytesIO(csv_bytes), dest)
        return {"success": True, "message": f"Written to Processed/{out_name}"}
    except Exception as e:
        return {"success": False, "message": f"Failed to write Processed: {e}"}

def write_error(csv_bytes: bytes, stem: str, ts: str) -> dict:
    ws = _get_workspace()
    out_name = f"{stem}_errors_{ts}.csv"
    dest = f"{ws}/Error/{out_name}"
    
    try:
        with _sftp_session() as sftp:
            sftp.putfo(BytesIO(csv_bytes), dest)
        return {"success": True, "message": f"Written to Error/{out_name}"}
    except Exception as e:
        return {"success": False, "message": f"Failed to write Error: {e}"}
