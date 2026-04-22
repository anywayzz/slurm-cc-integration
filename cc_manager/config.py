"""YAML-based configuration management for cc-vm-manager."""

from __future__ import annotations

import base64
import os
from pathlib import Path
from typing import Optional

import yaml

_KEYRING_SERVICE = "cc-vm-manager"

CONFIG_DIR = Path(__file__).parent.parent / "configs"
CONFIG_FILE = CONFIG_DIR / "config.yaml"


def _try_keyring(action: str, profile: str, password: str = "") -> Optional[str]:
    """Keyring helper — returns None silently if keyring backend is unavailable."""
    try:
        import keyring
        if action == "set":
            keyring.set_password(_KEYRING_SERVICE, profile, password)
        elif action == "get":
            return keyring.get_password(_KEYRING_SERVICE, profile)
        elif action == "del":
            try:
                keyring.delete_password(_KEYRING_SERVICE, profile)
            except Exception:
                pass
    except Exception:
        pass
    return None


def save_password(profile_name: str, password: str, cfg: dict) -> None:
    """Store the OpenRC password.

    Tries OS keyring first. Falls back to base64-obfuscated value inside
    the YAML config (with a warning comment in the file).
    """
    ok = _try_keyring("set", profile_name, password)
    # If keyring succeeded, ensure no stale fallback in cfg
    for p in cfg.get("profiles", []):
        if p["name"] == profile_name:
            p.pop("_password_b64", None)
            p["_password_stored"] = "keyring"
            return
    # Keyring not available — store obfuscated in profile dict
    encoded = base64.b64encode(password.encode()).decode()
    for p in cfg.get("profiles", []):
        if p["name"] == profile_name:
            p["_password_b64"] = encoded
            p["_password_stored"] = "config_b64"
            return


def get_password(profile_name: str, cfg: dict) -> str:
    """Retrieve the stored password for a profile. Returns '' if not found."""
    profile = next((p for p in cfg.get("profiles", []) if p["name"] == profile_name), {})
    storage = profile.get("_password_stored", "")

    if storage == "keyring":
        pwd = _try_keyring("get", profile_name)
        return pwd or ""

    if storage == "config_b64":
        encoded = profile.get("_password_b64", "")
        if encoded:
            try:
                return base64.b64decode(encoded.encode()).decode()
            except Exception:
                pass
    return ""



def _default_config() -> dict:
    return {"profiles": [], "active_profile": None, "work_volume": None}


def load_config() -> dict:
    """Load config from disk; returns default structure if not found."""
    if not CONFIG_FILE.exists():
        return _default_config()
    with CONFIG_FILE.open("r") as f:
        data = yaml.safe_load(f) or {}
    cfg = _default_config()
    cfg.update(data)
    return cfg


def save_config(cfg: dict) -> None:
    """Persist config to disk (creates directory if needed)."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with CONFIG_FILE.open("w") as f:
        yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)


def get_active_profile(cfg: dict) -> Optional[dict]:
    """Return the active profile dict or None."""
    name = cfg.get("active_profile")
    for p in cfg.get("profiles", []):
        if p.get("name") == name:
            return p
    return None


def add_or_update_profile(cfg: dict, name: str, openrc: str, key_file: str) -> None:
    """Upsert a profile by name."""
    profiles: list = cfg.setdefault("profiles", [])
    for p in profiles:
        if p["name"] == name:
            p["openrc"] = openrc
            p["key_file"] = key_file
            return
    profiles.append({"name": name, "openrc": openrc, "key_file": key_file})


def set_work_volume(cfg: dict, name: str, size_gb: int, mount_point: str = "/mnt/data") -> None:
    cfg["work_volume"] = {"name": name, "size_gb": size_gb, "mount_point": mount_point}


def is_config_complete(cfg: dict) -> tuple[bool, str]:
    """Return (ok, reason). ok is True when the config is ready to use."""
    profile = get_active_profile(cfg)
    if not profile:
        return False, "No active profile configured."
    openrc = profile.get("openrc", "")
    key_file = profile.get("key_file", "")
    if not openrc or not Path(openrc).exists():
        return False, f"OpenRC file not found: {openrc or '(not set)'}"
    if not key_file or not Path(key_file).exists():
        return False, f"SSH key file not found: {key_file or '(not set)'}"
    if not cfg.get("work_volume"):
        return False, "No work volume configured."
    return True, ""
