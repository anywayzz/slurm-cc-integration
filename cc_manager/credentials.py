"""Secure credential storage for the autoscaler using the system keyring.

On headless servers (no desktop session), falls back to keyrings.cryptfile
which stores credentials in an AES-256 encrypted file at
~/.local/share/keyrings/cryptfile_pass (protected by a passphrase you set
once at store time).

Usage (one-time setup, interactive):
    python -m cc_manager.credentials store

Usage (runtime, headless):
    Autoscaler reads password automatically — no TTY needed.
"""

from __future__ import annotations

import logging
import os
import sys

KEYRING_SERVICE = "chameleon-autoscaler"
KEYRING_USERNAME = "os_password"

log = logging.getLogger(__name__)


def _ensure_cryptfile_backend() -> None:
    """Force keyrings.cryptfile when no desktop keyring is available."""
    try:
        import keyring
        backend = keyring.get_keyring()
        # If the default backend is fail/null/plaintext, switch to cryptfile
        backend_name = type(backend).__name__.lower()
        if any(x in backend_name for x in ("fail", "null", "plaintext", "chainer")):
            raise RuntimeError("no usable default keyring")
    except Exception:
        try:
            from keyrings.cryptfile.cryptfile import CryptFileKeyring
            kr = CryptFileKeyring()
            # In headless mode the passphrase must come from env var
            # KEYRING_CRYPTFILE_PASSWORD to avoid a TTY prompt.
            passphrase = os.environ.get("KEYRING_CRYPTFILE_PASSWORD", "")
            if not passphrase:
                raise RuntimeError(
                    "Set KEYRING_CRYPTFILE_PASSWORD env var to unlock the keyring "
                    "in headless mode, or use password_file in the config instead."
                )
            kr.keyring_key = passphrase
            import keyring
            keyring.set_keyring(kr)
        except ImportError:
            pass  # keyrings.cryptfile not installed — caller will handle


def get_password() -> str:
    """Retrieve the OpenStack password from the keyring. Returns '' if not set."""
    _ensure_cryptfile_backend()
    try:
        import keyring
        pwd = keyring.get_password(KEYRING_SERVICE, KEYRING_USERNAME)
        return pwd or ""
    except Exception as exc:
        log.warning("keyring.get_password failed: %s", exc)
        return ""


def store_password(password: str) -> None:
    """Store the OpenStack password in the keyring (interactive one-time setup)."""
    import keyring
    keyring.set_password(KEYRING_SERVICE, KEYRING_USERNAME, password)
    log.info("Password stored in keyring (service=%s, user=%s)", KEYRING_SERVICE, KEYRING_USERNAME)


def delete_password() -> None:
    """Remove the stored password from the keyring."""
    import keyring
    keyring.delete_password(KEYRING_SERVICE, KEYRING_USERNAME)


# ── CLI for one-time interactive setup ───────────────────────────────────────

def _cli():
    import getpass
    if len(sys.argv) < 2 or sys.argv[1] not in ("store", "get", "delete"):
        print("Usage: python -m cc_manager.credentials [store|get|delete]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "store":
        pwd = getpass.getpass("OpenStack/Chameleon password: ")
        store_password(pwd)
        print("Stored.")
    elif cmd == "get":
        print(get_password() or "(not set)")
    elif cmd == "delete":
        delete_password()
        print("Deleted.")


if __name__ == "__main__":
    _cli()
