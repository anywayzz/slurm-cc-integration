"""Textual TUI for cc-vm-manager — KVM@TACC only."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger(__name__)


def _clean_error(exc: Exception) -> str:
    msg = str(exc)
    if "Unauthorized" in msg or "401" in msg:
        return "Authentication failed (401). Check password or OIDC session."
    if "Client Error for url:" in msg:
        idx = msg.find(", ", msg.find("Client Error for url:"))
        if idx >= 0:
            return msg[idx + 2:].rstrip(".")
    if msg.startswith("OpenStack error: "):
        return msg[len("OpenStack error: "):]
    return msg


from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import (
    Button, DataTable, Footer, Header, Input, Label,
    RichLog, Rule, Select, SelectionList, Static, Switch,
    TabbedContent, TabPane,
)
from textual import work

from .config import get_active_profile, get_password, is_config_complete, load_config, save_config
from .kvm_backend import ChiKVMManager
from .scripts import BUILT_IN_SCRIPTS, make_volume_setup_script
from .setup_wizard import WelcomeScreen, ProfileScreen, VolumeScreen, PasswordPromptScreen

_DURATION_TIMEDELTAS = {
    "1h":  timedelta(hours=1),
    "6h":  timedelta(hours=6),
    "12h": timedelta(hours=12),
    "1d":  timedelta(days=1),
    "3d":  timedelta(days=3),
    "7d":  timedelta(days=7),
}


# ── Config Selection Screen ───────────────────────────────────────────────────

class ConfigSelectScreen(Static):
    """Bar shown above the main TUI that displays active profile + volume."""

    def __init__(self, cfg: dict, **kwargs):
        super().__init__(**kwargs)
        self._cfg = cfg

    def compose(self) -> ComposeResult:
        profile = get_active_profile(self._cfg)
        vol = self._cfg.get("work_volume") or {}
        pname = profile["name"] if profile else "—"
        vname = vol.get("name", "—")
        vsize = vol.get("size_gb", "?")
        vmount = vol.get("mount_point", "/mnt/data")
        yield Label(
            f"[bold cyan]Profile:[/bold cyan] {pname}  "
            f"[bold cyan]Work Volume:[/bold cyan] {vname} ({vsize} GB → {vmount})",
            id="config-bar",
        )
        with Horizontal(id="config-bar-btns"):
            yield Button("Change Profile", id="btn-change-profile", variant="default")
            yield Button("Change Volume", id="btn-change-volume", variant="default")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-change-profile":
            self.app.push_screen(ProfileScreen())
        elif event.button.id == "btn-change-volume":
            self.app.push_screen(VolumeScreen())


# ── KVM Pane ──────────────────────────────────────────────────────────────────

class KVMPane(Static):
    def __init__(self, manager: ChiKVMManager, cfg: dict, **kwargs):
        super().__init__(**kwargs)
        self.manager = manager
        self._cfg = cfg
        self._images: dict[str, dict] = {}
        self._flavors: dict[str, dict] = {}
        self._keypairs: list[str] = []
        self._secgroups: dict[str, str] = {}
        self._networks: dict[str, str] = {}
        self._servers: dict[str, dict] = {}
        self._volumes: list[dict] = []

    def compose(self) -> ComposeResult:
        with Horizontal(id="kvm-layout"):
            with Vertical(id="kvm-form-panel"):
                yield Label("Create VM", classes="panel-title")
                yield Rule()
                yield Label("Name")
                yield Input(placeholder="my-vm", id="kvm-name")
                yield Label("Flavor")
                yield Select([], id="kvm-flavor", prompt="Select…")
                yield Label("Image")
                yield Select([], id="kvm-image", prompt="Select…")
                yield Label("Keypair")
                yield Select([], id="kvm-keypair", prompt="Select…")
                yield Label("Security Group")
                yield Select([], id="kvm-secgroup", prompt="Select…")
                yield Label("Network")
                yield Select([], id="kvm-network", prompt="Select…")
                with Horizontal(classes="switch-row"):
                    yield Label("Floating IP")
                    yield Switch(id="kvm-float", value=True)
                with Horizontal(classes="switch-row"):
                    yield Label("Auto-attach work volume")
                    yield Switch(id="kvm-auto-vol", value=True)
                yield Label("Init Scripts")
                yield SelectionList(
                    *[(v["label"], k) for k, v in BUILT_IN_SCRIPTS.items()],
                    id="kvm-scripts",
                )
                yield Rule()
                with Horizontal(classes="switch-row"):
                    yield Label("Create Lease")
                    yield Switch(id="kvm-lease-enabled", value=True)
                yield Label("Lease Duration")
                yield Select(
                    [("1 hour", "1h"), ("6 hours", "6h"), ("12 hours", "12h"),
                     ("1 day", "1d"), ("3 days", "3d"), ("1 week", "7d"), ("Custom", "custom")],
                    value="1d", id="kvm-duration",
                )
                yield Label("End Date (YYYY-MM-DD)")
                yield Input(id="kvm-end-date")
                yield Label("End Time (HH:MM)")
                yield Input(id="kvm-end-time")
                yield Button("Create VM", variant="primary", id="kvm-create")

            with Vertical(id="kvm-right-col"):
                with TabbedContent(id="kvm-right-tabs"):
                    with TabPane("Servers", id="kvm-servers-tab"):
                        yield DataTable(id="kvm-table", cursor_type="row")
                        with Horizontal(classes="action-row"):
                            yield Button("Refresh", id="kvm-refresh")
                            yield Button("Copy SSH", variant="success", id="kvm-ssh")
                            yield Button("Delete", variant="error", id="kvm-delete")
                    with TabPane("Volumes", id="kvm-volumes-tab"):
                        yield DataTable(id="kvm-vol-table", cursor_type="row")
                        with Horizontal(classes="vol-form-row"):
                            yield Button("Refresh Volumes", id="kvm-vol-refresh")
                            yield Button("Detach Selected", id="kvm-vol-detach", variant="error")
                with Vertical(id="kvm-log-panel"):
                    yield Label("Events", classes="panel-title")
                    yield RichLog(id="kvm-log", highlight=False, markup=True)

    def on_mount(self) -> None:
        self.query_one("#kvm-table", DataTable).add_columns(
            "Name", "Image", "Keypair", "Flavor", "Volumes", "IP", "SSH Command"
        )
        self.query_one("#kvm-vol-table", DataTable).add_columns(
            "Name", "GB", "Status", "Attached To"
        )
        end = datetime.now() + timedelta(days=1)
        self.query_one("#kvm-end-date", Input).value = end.strftime("%Y-%m-%d")
        self.query_one("#kvm-end-time", Input).value = end.strftime("%H:%M")
        if hasattr(self.manager, "_shell") and self.manager._shell is not None:
            self.manager._shell._log_callback = self._shell_log_callback
        self._load_resources()

    def _shell_log_callback(self, kind: str, text: str) -> None:
        if kind == "CMD":
            self.app.call_from_thread(self._log, f"[bold cyan]>[/bold cyan] {text}")
        else:
            preview = text[:200].rstrip()
            if preview:
                self.app.call_from_thread(self._log, f"[dim]{preview}[/dim]")

    def on_select_changed(self, event) -> None:
        if event.select.id == "kvm-duration":
            delta = _DURATION_TIMEDELTAS.get(str(event.value))
            if delta:
                end = datetime.now() + delta
                self.query_one("#kvm-end-date", Input).value = end.strftime("%Y-%m-%d")
                self.query_one("#kvm-end-time", Input).value = end.strftime("%H:%M")

    # ── workers ───────────────────────────────────────────────────────────────

    @work(thread=True, exclusive=True, group="kvm-load")
    def _load_resources(self) -> None:
        self.app.call_from_thread(self._log, "Loading resources…")
        images: dict = {}
        for label, fetch_fn in [
            ("images",    self.manager.get_images),
            ("flavors",   self.manager.get_flavors),
            ("keypairs",  self.manager.get_keypairs),
            ("secgroups", self.manager.get_secgroups),
            ("networks",  self.manager.get_networks),
        ]:
            try:
                data = fetch_fn()
                if label == "images":
                    images = data
                self.app.call_from_thread(self._apply_resources, {label: data})
                self.app.call_from_thread(self._log, f"Loaded {label} ({len(data)})")
            except Exception as e:
                log.exception("KVM load %s failed", label)
                error_msg = _clean_error(e)
                self.app.call_from_thread(self._log, f"Error loading {label}: {error_msg}", error=True)
                if "401" in error_msg or "Unauthorized" in error_msg:
                    self.app.call_from_thread(self._log, "Tip: Run setup again to update password.", error=True)
                return
        try:
            servers = self.manager.get_servers()
            self.app.call_from_thread(self._update_servers, servers)
        except Exception:
            log.exception("KVM load servers failed")
        try:
            volumes = self.manager.get_volumes()
            self.app.call_from_thread(self._update_volumes, volumes)
            self.app.call_from_thread(self._log, f"Loaded volumes ({len(volumes)})")
        except Exception:
            log.exception("KVM load volumes failed")
        if not images:
            self.app.call_from_thread(self._log, "Warning: no images found", error=True)
        else:
            self.app.call_from_thread(self._log, "Ready")

    @work(thread=True, exclusive=True, group="kvm-refresh")
    def _refresh_servers(self) -> None:
        self.app.call_from_thread(self._log, "Refreshing…")
        try:
            servers = self.manager.get_servers()
            self.app.call_from_thread(self._update_servers, servers)
            self.app.call_from_thread(self._log, f"{len(servers)} active servers")
        except Exception as e:
            self.app.call_from_thread(self._log, _clean_error(e), error=True)

    @work(thread=True)
    def _refresh_volumes(self) -> None:
        try:
            volumes = self.manager.get_volumes()
            self.app.call_from_thread(self._update_volumes, volumes)
        except Exception as e:
            self.app.call_from_thread(self._log, _clean_error(e), error=True)

    @work(thread=True)
    def _create_vm(
        self,
        name: str,
        flavor: str,
        image: str,
        keypair: str,
        secgroup: str,
        network: str,
        float_ip: bool,
        auto_vol: bool,
        init_scripts: list[str],
        end_date: str,
        end_time: str,
    ) -> None:
        reservation_id: Optional[str] = None
        resource_type = "flavor:instance"

        if end_date and end_time:
            lease_name = f"{name}-lease"
            self.app.call_from_thread(self._log, f"Creating lease '{lease_name}'…")
            try:
                reservation_id, lease_id = self.manager.create_reservation(
                    name=lease_name,
                    end_date=end_date,
                    end_time=end_time,
                    resource_type=resource_type,
                    flavor_id=self._flavors[flavor]["id"],
                )
                if not reservation_id:
                    self.app.call_from_thread(self._log, "No resources available", error=True)
                    return
                self.app.call_from_thread(self._log, "Waiting for lease activation…")
                self.manager.wait_for_lease_active(lease_id)
            except Exception as e:
                err = _clean_error(e)
                if "reservation" in err.lower() or "not an openstack command" in err:
                    self.app.call_from_thread(self._log, "Falling back to on-demand instance")
                    reservation_id = None
                else:
                    self.app.call_from_thread(self._log, f"Lease failed: {err}", error=True)
                    return

        server_id: Optional[str] = None
        for attempt in range(2):
            prefix = f"[retry {attempt}/1] " if attempt > 0 else ""
            self.app.call_from_thread(self._log, f"{prefix}Creating VM '{name}'…")
            try:
                active_flavor_id = self._flavors[flavor]["id"]
                active_res_id = reservation_id
                if reservation_id and resource_type == "flavor:instance":
                    active_flavor_id = self.manager.get_reservation_flavor(reservation_id)
                    active_res_id = None

                server_id = self.manager.create_vm(
                    name=name,
                    flavor_id=active_flavor_id,
                    image_id=self._images[image]["id"],
                    keypair=keypair,
                    secgroup_id=self._secgroups.get(secgroup, secgroup),
                    network_id=self._networks.get(network, network),
                    reservation_id=active_res_id,
                )
                ssh_cmd = f"ssh cc@<pending> -i {self.manager._key_file or keypair}"
                ip_addr: Optional[str] = None

                if float_ip:
                    self.app.call_from_thread(self._log, "Waiting for ACTIVE…")
                    self.manager.wait_for_active(server_id)
                    self.app.call_from_thread(self._log, "Assigning floating IP…")
                    ip_id, ip_addr = self.manager.create_floating_ip(name)
                    self.manager.attach_floating_ip(server_id, ip_id)
                    ssh_cmd = f"ssh cc@{ip_addr} -i {self.manager._key_file or keypair}"

                if ip_addr:
                    self.app.call_from_thread(self._log, f"Waiting for SSH on {ip_addr}…")
                    try:
                        self.manager.wait_for_ssh(ip_addr, keypair)
                    except Exception as he:
                        self.app.call_from_thread(self._log, f"SSH wait failed: {_clean_error(he)}", error=True)
                        return

                    # Run selected init scripts
                    for script_key in init_scripts:
                        info = BUILT_IN_SCRIPTS.get(script_key, {})
                        label = info.get("label", script_key)
                        self.app.call_from_thread(self._log, f"Running '{label}'…")
                        try:
                            self.manager.run_init_script(ip_addr, keypair, info["body"])
                            self.app.call_from_thread(self._log, f"'{label}' done")
                        except Exception as se:
                            self.app.call_from_thread(self._log, f"'{label}' failed: {_clean_error(se)}", error=True)

                    # Auto-attach and mount work volume
                    if auto_vol:
                        vol_cfg = self._cfg.get("work_volume") or {}
                        vol_name = vol_cfg.get("name")
                        mount_point = vol_cfg.get("mount_point", "/mnt/data")
                        if vol_name:
                            self.app.call_from_thread(
                                self._log, f"Attaching work volume '{vol_name}'…"
                            )
                            try:
                                # Find vol ID
                                vols = self.manager.get_volumes()
                                vol = next((v for v in vols if v["name"] == vol_name), None)
                                if vol:
                                    self.manager.attach_volume(server_id, vol["id"])
                                    device = self.manager.find_volume_device(server_id, vol["id"])
                                    self.app.call_from_thread(
                                        self._log, f"Volume attached at {device}, mounting…"
                                    )
                                    script = make_volume_setup_script(device, mount_point)
                                    self.manager.run_init_script(ip_addr, keypair, script)
                                    self.app.call_from_thread(
                                        self._log,
                                        f"✓ Volume '{vol_name}' mounted at {mount_point} "
                                        f"(Docker → {mount_point}/docker)",
                                    )
                                else:
                                    self.app.call_from_thread(
                                        self._log, f"Volume '{vol_name}' not found", error=True
                                    )
                            except Exception as ve:
                                self.app.call_from_thread(
                                    self._log, f"Volume attach failed: {_clean_error(ve)}", error=True
                                )

                self.app.call_from_thread(self._log, f"Done! {ssh_cmd}")
                servers = self.manager.get_servers()
                self.app.call_from_thread(self._update_servers, servers)
                return

            except RuntimeError as e:
                if "entered ERROR state" in str(e):
                    self.app.call_from_thread(self._log, f"VM '{name}' ERROR — cleaning up…", error=True)
                    if server_id:
                        try:
                            self.manager.delete_server(server_id)
                        except Exception:
                            pass
                    if attempt == 0:
                        self.app.call_from_thread(self._log, "Retrying in 5s…")
                        time.sleep(5)
                        server_id = None
                        continue
                    self.app.call_from_thread(self._log, "VM failed on retry — giving up.", error=True)
                    return
                self.app.call_from_thread(self._log, _clean_error(e), error=True)
                return
            except Exception as e:
                self.app.call_from_thread(self._log, _clean_error(e), error=True)
                return

    @work(thread=True)
    def _delete_server(self, name: str) -> None:
        info = self._servers.get(name, {})
        server_id = info.get("id", name)
        ip = info.get("ip")
        self.app.call_from_thread(self._log, f"Deleting '{name}'…")
        try:
            if ip:
                self.manager.delete_floating_ip(ip)
            self.manager.delete_server(server_id)
            servers = self.manager.get_servers()
            self.app.call_from_thread(self._update_servers, servers)
            self.app.call_from_thread(self._log, f"Deleted '{name}'")
        except Exception as e:
            self.app.call_from_thread(self._log, _clean_error(e), error=True)
        try:
            self.manager.delete_lease(f"{name}-lease")
            self.app.call_from_thread(self._log, f"Deleted lease '{name}-lease'")
        except Exception:
            pass

    # ── UI updates ────────────────────────────────────────────────────────────

    def _log(self, msg: str, error: bool = False) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        w = self.query_one("#kvm-log", RichLog)
        if error:
            w.write(f"[bold red]\\[{ts}][/bold red] [red]{msg}[/red]")
        else:
            w.write(f"[dim]\\[{ts}][/dim] {msg}")

    def _apply_resources(self, part: dict) -> None:
        if "images" in part:
            self._images = part["images"]
            self.query_one("#kvm-image", Select).set_options([(n, n) for n in part["images"]])
        if "flavors" in part:
            self._flavors = part["flavors"]
            self.query_one("#kvm-flavor", Select).set_options([(n, n) for n in part["flavors"]])
        if "keypairs" in part:
            self._keypairs = part["keypairs"]
            self.query_one("#kvm-keypair", Select).set_options([(n, n) for n in part["keypairs"]])
        if "secgroups" in part:
            self._secgroups = part["secgroups"]
            self.query_one("#kvm-secgroup", Select).set_options([(n, n) for n in part["secgroups"]])
        if "networks" in part:
            self._networks = part["networks"]
            self.query_one("#kvm-network", Select).set_options([(n, n) for n in part["networks"]])

    def _update_servers(self, servers: dict) -> None:
        self._servers = servers
        table = self.query_one("#kvm-table", DataTable)
        table.clear()
        for sname, info in servers.items():
            ip = info.get("ip") or "—"
            key_name = info.get("key_name") or "—"
            ssh = f"ssh cc@{ip} -i {self.manager._key_file or key_name}" if info.get("ip") else "—"
            vols = ", ".join([v[:8] for v in info.get("volumes", [])]) or "—"
            table.add_row(sname, info.get("image", "—"), key_name,
                          info.get("flavor", "—"), vols, ip, ssh, key=sname)

    def _update_volumes(self, volumes: list[dict]) -> None:
        self._volumes = volumes
        table = self.query_one("#kvm-vol-table", DataTable)
        table.clear()
        for v in volumes:
            table.add_row(v["name"], v["size"], v["status"], v["attached_to"] or "—", key=v["id"])

    # ── events ────────────────────────────────────────────────────────────────

    def on_button_pressed(self, event: Button.Pressed) -> None:
        btn = event.button.id
        if btn == "kvm-create":
            self._on_create()
        elif btn == "kvm-refresh":
            self._refresh_servers()
        elif btn == "kvm-delete":
            self._on_delete()
        elif btn == "kvm-ssh":
            self._on_copy_ssh()
        elif btn == "kvm-vol-refresh":
            self._refresh_volumes()
        elif btn == "kvm-vol-detach":
            self._on_detach_volume()

    def _on_detach_volume(self) -> None:
        table = self.query_one("#kvm-vol-table", DataTable)
        if table.row_count == 0 or table.cursor_row is None:
            return
        # DataTable row keys or data retrieval
        try:
            row = table.get_row_at(table.cursor_row)
            # Find the volume dict in self._volumes
            vol_name = str(row[0])
            vol = next((v for v in self._volumes if v["name"] == vol_name), None)
            if vol and vol["attached_server_id"]:
                self._detach_volume(vol["attached_server_id"], vol["id"])
            else:
                self.notify("Volume not attached or not found", severity="warning")
        except Exception as e:
             self.notify(f"Could not get row info: {e}", severity="error")

    @work(thread=True)
    def _detach_volume(self, server_id: str, volume_id: str) -> None:
        self.app.call_from_thread(self._log, f"Detaching volume {volume_id[:8]}…")
        try:
            self.manager.detach_volume(server_id, volume_id)
            self.app.call_from_thread(self._log, "Volume detached")
            volumes = self.manager.get_volumes()
            self.app.call_from_thread(self._update_volumes, volumes)
            servers = self.manager.get_servers()
            self.app.call_from_thread(self._update_servers, servers)
        except Exception as e:
            self.app.call_from_thread(self._log, f"Detach failed: {_clean_error(e)}", error=True)

    def _on_create(self) -> None:
        name = self.query_one("#kvm-name", Input).value.strip()
        flavor = self.query_one("#kvm-flavor", Select).value
        image = self.query_one("#kvm-image", Select).value
        keypair = self.query_one("#kvm-keypair", Select).value
        secgroup = self.query_one("#kvm-secgroup", Select).value
        network = self.query_one("#kvm-network", Select).value
        float_ip = self.query_one("#kvm-float", Switch).value
        auto_vol = self.query_one("#kvm-auto-vol", Switch).value
        lease_enabled = self.query_one("#kvm-lease-enabled", Switch).value
        init_scripts = list(self.query_one("#kvm-scripts", SelectionList).selected)
        end_date = self.query_one("#kvm-end-date", Input).value.strip()
        end_time = self.query_one("#kvm-end-time", Input).value.strip()

        if not name or any(not isinstance(v, str) for v in [flavor, image, keypair, secgroup, network]):
            self._log("Please fill all required fields", error=True)
            return
        self._create_vm(
            name, flavor, image, keypair, secgroup, network,
            float_ip, auto_vol, init_scripts,
            end_date if lease_enabled else "",
            end_time if lease_enabled else "",
        )

    def _on_delete(self) -> None:
        table = self.query_one("#kvm-table", DataTable)
        if table.row_count == 0:
            return
        row = table.get_row_at(table.cursor_row)
        if row:
            self._delete_server(str(row[0]))

    def _on_copy_ssh(self) -> None:
        table = self.query_one("#kvm-table", DataTable)
        if table.row_count == 0:
            return
        row = table.get_row_at(table.cursor_row)
        if row and len(row) >= 6:
            ssh_cmd = str(row[5])
            self.app.copy_to_clipboard(ssh_cmd)
            self.app.notify(f"Copied: {ssh_cmd}", title="Clipboard")

    def refresh_servers(self) -> None:
        self._refresh_servers()


# ── Main App ──────────────────────────────────────────────────────────────────

class CCManagerApp(App[None]):
    CSS_PATH = "app.tcss"
    TITLE = "cc-vm-manager"
    SUB_TITLE = "KVM@TACC"

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
        Binding("?", "help", "Help"),
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._cfg: dict = {}
        self._manager: Optional[ChiKVMManager] = None

    def on_mount(self) -> None:
        self._cfg = load_config()
        ok, reason = is_config_complete(self._cfg)
        if not ok:
            self.push_screen(WelcomeScreen(reason=reason))
        else:
            self._boot_manager()

    def _boot_manager(self, password: Optional[str] = None) -> None:
        """Initialise the KVM manager and mount the main TUI."""
        import os
        profile = get_active_profile(self._cfg)
        if not profile:
            return

        # If no password provided, check environment or keyring
        pwd = password or os.environ.get("OS_PASSWORD")
        if not pwd:
            pwd = get_password(profile["name"], self._cfg)

        # Still no password? Prompt the user
        if not pwd:
            self.push_screen(PasswordPromptScreen(), callback=self._boot_manager)
            return

        try:
            self._manager = ChiKVMManager(
                profile["openrc"],
                key_file=profile["key_file"],
                password=pwd
            )
        except ValueError as e:
            # Likely missing password or invalid credential file
            self.push_screen(WelcomeScreen(reason=str(e)))
            return
        except Exception as e:
            self.notify(f"Failed to init manager: {e}", severity="error")
            return
        self._mount_kvm_pane()

    def _mount_kvm_pane(self) -> None:
        if self._manager is None:
            return
        main = self.query_one("#main-content")
        main.remove_children()
        main.mount(ConfigSelectScreen(self._cfg))
        main.mount(KVMPane(self._manager, self._cfg))

    def compose(self) -> ComposeResult:
        yield Header()
        yield Vertical(id="main-content")
        yield Footer()

    def on_screen_resume(self) -> None:
        # Called when a wizard screen is popped (user returns)
        self._cfg = load_config()
        ok, reason = is_config_complete(self._cfg)
        if ok and self._manager is None:
            self._boot_manager()
        elif ok:
            # Config may have changed (e.g. new volume); refresh bar
            self._mount_kvm_pane()

    def action_refresh(self) -> None:
        for pane in self.query(KVMPane):
            pane.refresh_servers()

    def action_help(self) -> None:
        self.notify(
            "r: Refresh  q: Quit  Arrows: navigate table  Space: toggle script",
            title="Keybindings",
        )
