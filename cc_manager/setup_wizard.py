"""First-run setup wizard screens for cc-vm-manager."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from textual.app import ComposeResult
from textual.containers import Vertical, Horizontal
from textual.screen import Screen
from textual.widgets import (
    Button, Input, Label, ListItem, ListView, RichLog, Rule, Select, Static,
)
from textual import work

from .config import (
    add_or_update_profile, get_active_profile, get_password,
    load_config, save_config, save_password, set_work_volume,
)
from .kvm_backend import ChiKVMManager


# ── helpers ───────────────────────────────────────────────────────────────────

def _volume_presets() -> list[tuple[str, str, int]]:
    """Return [(label, key, size_gb)]."""
    return [
        ("Minimal — 20 GB", "minimal", 20),
        ("Data-driven — 100 GB", "data", 100),
        ("ML / AI — 500 GB", "ml", 500),
        ("Custom", "custom", 0),
    ]


# ── WelcomeScreen ─────────────────────────────────────────────────────────────

OPENRC_GUIDE = """\
[bold cyan]► How to get your OpenRC file[/bold cyan]

1. Log in to https://chameleoncloud.org
2. Go to your project dashboard → Identity → Application Credentials
   [dim]or use the direct URL:[/dim] https://chi.tacc.utexas.edu/auth/
3. Click [bold]Create Application Credential[/bold] (or download the
   KVM@TACC OpenRC file from the "API Access" page).
4. Save the downloaded .sh file into this project folder, e.g.:
   [green]  ./CHI-project-openrc.sh[/green]
"""

KEY_GUIDE = """\
[bold cyan]► How to create and download your SSH key[/bold cyan]

1. Log in to https://chameleoncloud.org → your project
2. Go to Compute → Key Pairs → [bold]Create Key Pair[/bold]
3. Name it (e.g. [green]kvmtacc[/green]) — the private key downloads automatically
4. Move it to the [green]keys/[/green] subfolder of this project:
   [green]  mkdir -p keys && mv ~/Downloads/kvmtacc.pem keys/kvmtacc.pvt[/green]
5. The key must have restrictive permissions:
   [green]  chmod 600 keys/kvmtacc.pvt[/green]
"""


class WelcomeScreen(Screen[None]):
    """Shown first-time or when config is incomplete."""

    CSS = """
    WelcomeScreen {
        align: center middle;
    }
    #welcome-box {
        width: 80;
        height: auto;
        border: solid #44475a;
        padding: 1 2;
        background: #1e1e2e;
    }
    #welcome-title {
        text-style: bold;
        color: #89b4fa;
        text-align: center;
    }
    #guide-log {
        height: 18;
        background: #181825;
    }
    .btn-row {
        height: 3;
        margin-top: 1;
        align: center middle;
    }
    """

    def __init__(self, reason: str = ""):
        super().__init__()
        self._reason = reason

    def compose(self) -> ComposeResult:
        with Vertical(id="welcome-box"):
            yield Label("cc-vm-manager — First Time Setup", id="welcome-title")
            yield Rule()
            if self._reason:
                yield Label(f"[yellow]{self._reason}[/yellow]")
            yield RichLog(id="guide-log", markup=True, highlight=False)
            yield Rule()
            with Horizontal(classes="btn-row"):
                yield Button("Continue to Profile Setup →", variant="primary", id="btn-continue")

    def on_mount(self) -> None:
        log = self.query_one("#guide-log", RichLog)
        log.write(OPENRC_GUIDE)
        log.write(KEY_GUIDE)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-continue":
            self.app.switch_screen(ProfileScreen())


# ── ProfileScreen ─────────────────────────────────────────────────────────────

class ProfileScreen(Screen[None]):
    """Create or select a profile (openrc + key_file)."""

    CSS = """
    ProfileScreen {
        align: center middle;
    }
    #profile-box {
        width: 80;
        height: auto;
        border: solid #44475a;
        padding: 1 2;
        background: #1e1e2e;
    }
    #profile-title {
        text-style: bold;
        color: #89b4fa;
    }
    #test-log {
        height: 6;
        background: #181825;
    }
    .field-label { color: #cdd6f4; }
    .btn-row {
        height: 3;
        margin-top: 1;
        align: left middle;
    }
    """

    def compose(self) -> ComposeResult:
        cfg = load_config()
        active = get_active_profile(cfg) or {}
        saved_pwd = get_password(active.get("name", ""), cfg)
        with Vertical(id="profile-box"):
            yield Label("Configure Profile", id="profile-title")
            yield Rule()
            yield Label("Profile name", classes="field-label")
            yield Input(placeholder="my-project", id="prof-name",
                        value=active.get("name", ""))
            yield Label("OpenRC file path", classes="field-label")
            yield Input(placeholder="CHI-project-openrc.sh", id="prof-openrc",
                        value=active.get("openrc", ""))
            yield Label("SSH private key path", classes="field-label")
            yield Input(placeholder="keys/kvmtacc.pvt", id="prof-key",
                        value=active.get("key_file", ""))
            yield Label(
                "OpenRC password [dim](leave blank if not required)[/dim]",
                classes="field-label",
            )
            yield Input(
                placeholder="••••••••",
                password=True,
                id="prof-password",
                value=saved_pwd,
            )
            yield Label(
                "[dim]Configs are stored in ./configs/config.yaml in the project root. "
                "Password is requested ad-hoc at each startup and NEVER saved.[/dim]",
                classes="field-label",
            )
            yield Rule()
            with Horizontal(classes="btn-row"):
                yield Button("Test Connection", id="btn-test")
                yield Button("Save & Continue →", variant="primary", id="btn-save")
            yield RichLog(id="test-log", markup=True, highlight=False)

    def _log(self, msg: str, error: bool = False) -> None:
        w = self.query_one("#test-log", RichLog)
        if error:
            w.write(f"[red]{msg}[/red]")
        else:
            w.write(msg)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-test":
            self._do_test()
        elif event.button.id == "btn-save":
            self._do_save()

    @work(thread=True)
    def _do_test(self) -> None:
        openrc = self.query_one("#prof-openrc", Input).value.strip()
        key = self.query_one("#prof-key", Input).value.strip()
        password = self.query_one("#prof-password", Input).value
        self.app.call_from_thread(self._log, "Testing connection…")
        if not Path(openrc).exists():
            self.app.call_from_thread(self._log, f"OpenRC not found: {openrc}", error=True)
            return
        if not Path(key).exists():
            self.app.call_from_thread(self._log, f"Key file not found: {key}", error=True)
            return
        try:
            mgr = ChiKVMManager(openrc, key_file=key, password=password)
            flavors = mgr.get_flavors()
            self.app.call_from_thread(
                self._log, f"✓ Connection OK — {len(flavors)} flavors found"
            )
        except Exception as e:
            self.app.call_from_thread(self._log, f"✗ Connection failed: {e}", error=True)

    def _do_save(self) -> None:
        name = self.query_one("#prof-name", Input).value.strip()
        openrc = self.query_one("#prof-openrc", Input).value.strip()
        key = self.query_one("#prof-key", Input).value.strip()
        password = self.query_one("#prof-password", Input).value
        if not name or not openrc or not key:
            self._log("Fill profile name, OpenRC path and key path.", error=True)
            return
        cfg = load_config()
        add_or_update_profile(cfg, name, openrc, key)
        cfg["active_profile"] = name
        # Password is not saved to config as per user request
        # if password:
        #     save_password(name, password, cfg)
        save_config(cfg)
        storage = next(
            (p.get("_password_stored", "not saved")
             for p in cfg["profiles"] if p["name"] == name),
            "not saved",
        )
        self._log(f"Profile '{name}' saved. Password storage: {storage}")
        self.app.switch_screen(VolumeScreen())


# ── VolumeScreen ──────────────────────────────────────────────────────────────

class VolumeScreen(Screen[None]):
    """Select an existing volume or create a new one."""

    CSS = """
    VolumeScreen {
        align: center middle;
    }
    #vol-box {
        width: 84;
        height: auto;
        border: solid #44475a;
        padding: 1 2;
        background: #1e1e2e;
    }
    #vol-title { text-style: bold; color: #89b4fa; }
    #vol-list { height: 8; border: solid #313244; }
    #vol-log { height: 6; background: #181825; }
    .field-label { color: #cdd6f4; }
    .btn-row {
        height: 3;
        margin-top: 1;
        align: left middle;
    }
    """

    def __init__(self):
        super().__init__()
        self._mgr: Optional[ChiKVMManager] = None
        self._existing_vols: list[dict] = []

    def compose(self) -> ComposeResult:
        presets = _volume_presets()
        with Vertical(id="vol-box"):
            yield Label("Configure Work Volume", id="vol-title")
            yield Rule()
            yield Label("Existing volumes (select to reuse)", classes="field-label")
            yield ListView(id="vol-list")
            yield Rule()
            yield Label("— OR create a new volume —", classes="field-label")
            yield Label("Volume name", classes="field-label")
            yield Input(placeholder="my-data-vol", id="vol-name")
            yield Label("Size preset", classes="field-label")
            yield Select(
                [(lbl, key) for lbl, key, _ in presets],
                value="data",
                id="vol-preset",
            )
            yield Label("Custom size in GB (only for Custom preset)", classes="field-label")
            yield Input(placeholder="200", id="vol-custom-gb")
            yield Rule()
            with Horizontal(classes="btn-row"):
                yield Button("Use Selected Existing", id="btn-use-existing")
                yield Button("Create New Volume", id="btn-create")
                yield Button("Finish Setup ✓", variant="primary", id="btn-finish")
            yield RichLog(id="vol-log", markup=True, highlight=False)

    def on_mount(self) -> None:
        self._load_manager()

    def _log(self, msg: str, error: bool = False) -> None:
        w = self.query_one("#vol-log", RichLog)
        if error:
            w.write(f"[red]{msg}[/red]")
        else:
            w.write(msg)

    @work(thread=True, group="vol-load")
    def _load_manager(self) -> None:
        cfg = load_config()
        profile = get_active_profile(cfg)
        if not profile:
            self.app.call_from_thread(self._log, "No active profile found.", error=True)
            return
        try:
            password = get_password(profile["name"], cfg)
            self._mgr = ChiKVMManager(
                profile["openrc"], key_file=profile["key_file"], password=password
            )
            vols = self._mgr.get_volumes()
            self._existing_vols = vols
            self.app.call_from_thread(self._populate_list, vols)
        except Exception as e:
            self.app.call_from_thread(self._log, f"Could not list volumes: {e}", error=True)

    def _populate_list(self, vols: list[dict]) -> None:
        lv = self.query_one("#vol-list", ListView)
        lv.clear()
        if not vols:
            lv.append(ListItem(Label("(no existing volumes)")))
        else:
            for v in vols:
                lv.append(ListItem(Label(
                    f"{v['name']}  {v['size']} GB  [{v['status']}]  {v['attached_to'] or ''}",
                    id=f"vol-item-{v['id']}",
                )))

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-use-existing":
            self._use_existing()
        elif event.button.id == "btn-create":
            self._create_volume()
        elif event.button.id == "btn-finish":
            self._finish()

    def _use_existing(self) -> None:
        lv = self.query_one("#vol-list", ListView)
        if lv.index is None or not self._existing_vols:
            self._log("Select a volume from the list first.", error=True)
            return
        idx = lv.index
        if idx >= len(self._existing_vols):
            return
        vol = self._existing_vols[idx]
        cfg = load_config()
        set_work_volume(cfg, vol["name"], int(vol["size"]))
        save_config(cfg)
        self._log(f"✓ Work volume set to '{vol['name']}' ({vol['size']} GB)")

    @work(thread=True)
    def _create_volume(self) -> None:
        if not self._mgr:
            self.app.call_from_thread(self._log, "Manager not ready yet.", error=True)
            return
        name = self.query_one("#vol-name", Input).value.strip()
        preset_key = self.query_one("#vol-preset", Select).value
        presets = {key: size for _, key, size in _volume_presets()}
        size_gb = presets.get(str(preset_key), 0)
        if size_gb == 0:  # custom
            raw = self.query_one("#vol-custom-gb", Input).value.strip()
            if not raw.isdigit() or int(raw) <= 0:
                self.app.call_from_thread(self._log, "Enter a valid custom size in GB.", error=True)
                return
            size_gb = int(raw)
        if not name:
            self.app.call_from_thread(self._log, "Enter a volume name.", error=True)
            return
        self.app.call_from_thread(self._log, f"Creating volume '{name}' ({size_gb} GB)…")
        try:
            vol_id = self._mgr.create_volume(name, size_gb)
            cfg = load_config()
            set_work_volume(cfg, name, size_gb)
            save_config(cfg)
            self.app.call_from_thread(self._log,
                f"✓ Volume '{name}' created ({vol_id[:8]}…) and saved to config")
            # Refresh list
            vols = self._mgr.get_volumes()
            self._existing_vols = vols
            self.app.call_from_thread(self._populate_list, vols)
        except Exception as e:
            self.app.call_from_thread(self._log, f"✗ {e}", error=True)

    def _finish(self) -> None:
        from .config import is_config_complete, load_config
        cfg = load_config()
        ok, reason = is_config_complete(cfg)
        if not ok:
            self._log(f"Setup incomplete: {reason}", error=True)
            return

        # Notify the parent app to reload and boot
        if hasattr(self.app, "_boot_manager"):
            # Update the app's local config reference
            setattr(self.app, "_cfg", cfg)
            self.app._boot_manager()

        self.app.pop_screen()


class PasswordPromptScreen(Screen[str]):
    """Prompts for Chameleon password at runtime."""

    CSS = """
    PasswordPromptScreen {
        align: center middle;
        background: rgba(0, 0, 0, 0.5);
    }
    #prompt-box {
        width: 50;
        height: auto;
        border: solid #89b4fa;
        padding: 1 2;
        background: #1e1e2e;
    }
    #prompt-title {
        text-style: bold;
        color: #89b4fa;
        text-align: center;
        margin-bottom: 1;
    }
    .btn-row {
        height: 3;
        margin-top: 1;
        align: center middle;
    }
    """

    def compose(self) -> ComposeResult:
        with Vertical(id="prompt-box"):
            yield Label("Chameleon CLI Password Required", id="prompt-title")
            yield Label("Please enter your password for KVM@TACC:")
            yield Input(placeholder="Password", password=True, id="pwd-input")
            with Horizontal(classes="btn-row"):
                yield Button("Submit", variant="primary", id="btn-submit")
                yield Button("Quit", variant="error", id="btn-quit")

    def on_mount(self) -> None:
        self.query_one("#pwd-input", Input).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-submit":
            pwd = self.query_one("#pwd-input", Input).value
            if pwd:
                self.dismiss(pwd)
        elif event.button.id == "btn-quit":
            self.app.exit()

    def on_input_submitted(self) -> None:
        self.query_one("#btn-submit", Button).press()
