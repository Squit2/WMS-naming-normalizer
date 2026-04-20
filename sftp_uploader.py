# sftp_uploader.py
"""
SFTP uploader — transfers a CSV file to the WMS import folder via SFTP (SSH).
Supports both password authentication and SSH private key authentication.
Credentials are read from environment variables only, never hardcoded.

Requires: pip install paramiko
"""

import os
from io import BytesIO, StringIO


def upload_to_wms(local_file_bytes: bytes, remote_filename: str) -> dict:
    """
    Upload CSV bytes to the WMS SFTP server.

    Automatically uses SSH key auth if SFTP_PRIVATE_KEY is set,
    otherwise falls back to password auth.

    Parameters
    ----------
    local_file_bytes : bytes   The CSV content to upload
    remote_filename  : str     Filename to use on the server

    Returns
    -------
    dict:
        success  : bool
        message  : str
    """
    try:
        import paramiko
    except ImportError:
        return {
            "success": False,
            "message": (
                "paramiko is required for SFTP. "
                "Install it with: pip install paramiko"
            )
        }

    host        = os.environ.get("SFTP_HOST", "").strip()
    port        = int(os.environ.get("SFTP_PORT", 22))
    user        = os.environ.get("SFTP_USER", "").strip()
    password    = os.environ.get("SFTP_PASSWORD", "").strip() or None
    private_key = os.environ.get("SFTP_PRIVATE_KEY", "").strip() or None
    target_dir  = os.environ.get("SFTP_TARGET_DIR", "/").strip()

    # Validate minimum required credentials are present
    if not host or not user:
        return {
            "success": False,
            "message": (
                "SFTP credentials are not configured. "
                "Set SFTP_HOST and SFTP_USER in environment or Streamlit secrets."
            )
        }

    if not password and not private_key:
        return {
            "success": False,
            "message": (
                "No authentication method configured. "
                "Set either SFTP_PASSWORD or SFTP_PRIVATE_KEY."
            )
        }

    transport = None
    try:
        # Establish transport layer
        transport = paramiko.Transport((host, port))
        transport.connect()

        if private_key:
            # SSH key authentication — parse the key from string
            pkey = _load_private_key(private_key)
            transport.auth_publickey(user, pkey)
        else:
            # Password authentication
            transport.auth_password(user, password)

        # Open SFTP session and upload
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            remote_path = "{}/{}".format(
                target_dir.rstrip("/"), remote_filename
            )
            sftp.putfo(BytesIO(local_file_bytes), remote_path)

        return {
            "success": True,
            "message": "Uploaded '{}' to {}:{}".format(
                remote_filename, host, target_dir
            )
        }

    except paramiko.AuthenticationException:
        return {
            "success": False,
            "message": (
                "SFTP authentication failed. "
                "Check your username and password/key."
            )
        }
    except paramiko.SSHException as e:
        return {
            "success": False,
            "message": "SFTP SSH error: {}".format(str(e))
        }
    except OSError as e:
        return {
            "success": False,
            "message": "SFTP connection error: {}".format(str(e))
        }
    finally:
        # Always close the transport, even if an exception occurred
        if transport and transport.is_active():
            transport.close()


def _load_private_key(key_string: str):
    """
    Parse an SSH private key from a string.
    Supports RSA, Ed25519, ECDSA, and DSS key types.

    Parameters
    ----------
    key_string : str   PEM-formatted private key content

    Returns
    -------
    paramiko.PKey subclass instance
    """
    import paramiko

    key_types = [
        paramiko.RSAKey,
        paramiko.Ed25519Key,
        paramiko.ECDSAKey,
        paramiko.DSSKey,
    ]

    last_error = None
    for key_type in key_types:
        try:
            return key_type.from_private_key(StringIO(key_string))
        except paramiko.SSHException as e:
            last_error = e
            continue

    raise paramiko.SSHException(
        "Could not load private key — unsupported key type or malformed key. "
        "Last error: {}".format(last_error)
    )
