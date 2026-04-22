"""ChiKVMManager — python-chi backend for KVM@TACC (KVM-only release)."""

from __future__ import annotations

import datetime
import getpass
import logging
import os
import re
import sys
import threading
import time
from typing import Any, Optional

log = logging.getLogger(__name__)


def _load_openrc(path: str, password: str = "") -> None:
    """Parse an OpenRC file and export OS_* env vars.

    Args:
        path:     Path to the .sh OpenRC file.
        password: If the OpenRC requires OS_PASSWORD and this is non-empty,
                  it is used directly instead of prompting via getpass.
    """
    with open(path) as f:
        content = f.read()

    env_vars: dict[str, str] = {}
    needs_password = False

    for line in content.splitlines():
        line = line.strip()
        if re.search(r"\bread\b.*OS_PASSWORD", line):
            needs_password = True
            continue
        m = re.match(r"export\s+(\w+)=[\"']?(.*?)[\"']?\s*$", line)
        if m:
            key, val = m.group(1), m.group(2)
            if val.startswith("$"):
                if key == "OS_PASSWORD":
                    needs_password = True
                continue
            env_vars[key] = val

    if needs_password and not os.environ.get("OS_PASSWORD"):
        if password:
            env_vars["OS_PASSWORD"] = password
        elif sys.stdin.isatty():
            import getpass
            env_vars["OS_PASSWORD"] = getpass.getpass("Enter OpenID-Connect password: ")
        else:
            raise ValueError("OpenID-Connect password required but not found in environment and no TTY available for prompt.")

    for key, value in env_vars.items():
        os.environ[key] = value


class _NullShell:
    """Dummy shell object so TUI code that accesses manager._shell doesn't crash."""
    _log_callback: Any = None


class ChiKVMManager:
    """python-chi backend for KVM@TACC. KVM VMs + Cinder volumes only."""

    def __init__(self, openrc_path: str, key_file: str = "", password: str = ""):
        _load_openrc(openrc_path, password=password)

        import chi  # noqa: F401  — must import after env vars are set

        project_id = os.environ.get("OS_PROJECT_ID")
        region = os.environ.get("OS_REGION_NAME", "KVM@TACC")
        auth_type = os.environ.get("OS_AUTH_TYPE", "")
        chi.set("region_name", region)

        if project_id and "applicationcredential" not in auth_type:
            # Password/OIDC auth: set project explicitly so chi knows the scope.
            chi.set("project_id", project_id)
        elif "applicationcredential" in auth_type:
            # App credentials are already scoped to a project inside the token.
            # Calling chi.set("project_id", ...) would add an explicit scope to
            # the auth request, which Keystone rejects with HTTP 401
            # "Application credentials cannot request a scope."
            pass

        self._key_file = key_file
        if key_file and os.path.isfile(key_file):
            os.chmod(key_file, 0o600)

        self._lock = threading.Lock()
        self._leases: dict[str, Any] = {}
        self._servers: dict[str, Any] = {}
        self._res_to_lease: dict[str, str] = {}
        self._shell = _NullShell()

        log.info("ChiKVMManager ready (region=%s, project=%s)", region, project_id)

    @property
    def conn(self):
        import chi
        return chi.connection()


    # ── helpers ───────────────────────────────────────────────────────────────

    def _is_uuid(self, value: str) -> bool:
        return bool(re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            value, re.I,
        ))

    def _resolve_image(self, image_id_or_name: str) -> str:
        if not self._is_uuid(image_id_or_name):
            return image_id_or_name
        import chi.image
        try:
            return chi.image.get_image_name(image_id_or_name)
        except Exception:
            return image_id_or_name

    def _resolve_flavor(self, flavor_id_or_name: str) -> str:
        if self._is_uuid(flavor_id_or_name):
            return flavor_id_or_name
        import chi.server as chi_server
        return chi_server.get_flavor_id(flavor_id_or_name)

    # ── resource listing ──────────────────────────────────────────────────────

    def get_flavors(self) -> dict[str, dict]:
        import chi.server as chi_server
        flavors = chi_server.list_flavors()
        return {self._get_res_attr(f, "name"): {"id": self._get_res_attr(f, "name"), "disk": self._get_res_attr(f, "disk") or 0}
                for f in sorted(flavors, key=lambda x: self._get_res_attr(x, "name") or "")}

    def get_images(self) -> dict[str, dict]:
        import chi.image
        images = {}
        for img in chi.image.list_images():
            name = self._get_res_attr(img, "name")
            if name and name.startswith("CC-"):
                images[name] = {"id": name, "min_disk": 0}
        return images

    def get_keypairs(self) -> list[str]:
        import chi.server
        return [self._get_res_attr(kp, "name") for kp in chi.server.nova().keypairs.list()]

    def get_secgroups(self) -> dict[str, str]:
        import chi.network
        sgs = chi.network.list_security_groups()
        return {self._get_res_attr(sg, "name"): self._get_res_attr(sg, "id") for sg in sgs}

    def get_networks(self) -> dict[str, str]:
        import chi.network
        return {self._get_res_attr(n, "name"): self._get_res_attr(n, "id") for n in chi.network.list_networks()
                if not self._get_res_attr(n, "is_router_external")}

    def _get_res_attr(self, res, attr):
        """Helper to handle both object attributes and dictionary keys from chi/openstack."""
        if isinstance(res, dict):
            if attr == "is_router_external":
                return res.get("router:external", False)
            return res.get(attr)
        return getattr(res, attr, None)

    def get_servers(self) -> dict[str, dict]:
        import chi.server
        servers: dict[str, dict] = {}
        for s in chi.server.list_servers():
            if s.status != "ACTIVE":
                continue
            ip = s.get_floating_ip() or next(iter(s.addresses.values()), [{"addr": None}])[0]["addr"]
            vols = getattr(s, "os-extended-volumes:volumes_attached", [])
            servers[s.name] = {
                "id": s.id,
                "ip": ip,
                "image": (s.image_name or "—")[:20],
                "flavor": s.flavor_name or "—",
                "key_name": (s.keypair.name if s.keypair else "—") or "—",
                "volumes": [v["id"] for v in vols],
            }
        return servers

    # ── volume management (Cinder) ────────────────────────────────────────────

    def get_volumes(self) -> list[dict]:
        """List Cinder volumes for this project."""
        try:
            import chi.storage
            vols = []
            for v in chi.storage.list_volumes():
                att_info = ""
                attached_server_id = None
                # Volume objects from chi.storage might have different attribute names
                # depending on the version of python-chi.
                attachments = getattr(v, "attachments", [])
                if attachments:
                    att = attachments[0]
                    # Handle both dict and object (cinderclient variations)
                    if isinstance(att, dict):
                        attached_server_id = att.get("server_id") or att.get("instance_id")
                        device = att.get("device", "")
                    else:
                        attached_server_id = getattr(att, "server_id", getattr(att, "instance_id", None))
                        device = getattr(att, "device", "")
                    
                    if attached_server_id:
                        att_info = f"Attached to {attached_server_id[:8]} on {device}" if device else f"Attached to {attached_server_id[:8]}"
                vols.append({
                    "id": v.id,
                    "name": v.name or f"<{v.id[:8]}>",
                    "status": v.status,
                    "size": str(v.size),
                    "attached_to": att_info,
                    "attached_server_id": attached_server_id,
                })
            return vols
        except Exception as e:
            log.warning("get_volumes failed: %s", e)
            return []

    def create_volume(self, name: str, size_gb: int) -> str:
        """Create a Cinder volume. Returns volume ID."""
        import chi.storage
        log.info("Creating volume '%s' (%d GB)", name, size_gb)
        vol = chi.storage.Volume(name=name, size=size_gb)
        vol.submit()
        return vol.id

    def _wait_for_volume_status(self, volume_id: str, target: str, timeout: int = 30) -> None:
        """Wait for volume to reach a target status (e.g. 'available')."""
        import chi.clients
        import time
        cinder = chi.clients.cinder()
        start = time.time()
        while time.time() - start < timeout:
            vol = cinder.volumes.get(volume_id)
            if vol.status == target:
                return
            if vol.status == "error":
                raise RuntimeError(f"Volume {volume_id} entered ERROR state")
            time.sleep(2)
        raise TimeoutError(f"Volume {volume_id} did not reach {target} within {timeout}s")

    def attach_volume(self, server_id: str, volume_id: str) -> None:
        """Attach volume to server. Smarter: detaches if already attached elsewhere."""
        import chi.clients
        import chi.server
        nova = chi.clients.nova()
        cinder = chi.clients.cinder()

        vol = cinder.volumes.get(volume_id)
        attachments = getattr(vol, "attachments", [])
        
        if attachments:
            curr_server = attachments[0].get("server_id")
            if curr_server == server_id:
                log.info("Volume already attached to this server")
                return
            
            log.warning("Volume %s already attached to %s, detaching first...", volume_id, curr_server)
            try:
                nova.volumes.delete_server_volume(curr_server, volume_id)
                self._wait_for_volume_status(volume_id, "available")
            except Exception as e:
                log.error("Failed to detach from old server: %s", e)

        log.info("Attaching volume %s to %s", volume_id, server_id)
        # Use novaclient directly for volume attachment as it's most reliable in KVM@TACC
        nova.volumes.create_server_volume(server_id, volume_id)

    def detach_volume(self, server_id: str, volume_id: str) -> None:
        """Detach volume from server."""
        import chi.clients
        chi.clients.nova().volumes.delete_server_volume(server_id, volume_id)

    def find_volume_device(self, server_id: str, vol_id: str, timeout: int = 15) -> str:
        """Return the device path (e.g. /dev/vdb) for an attached volume. Retries if not yet ready."""
        import chi.clients
        import time
        nova = chi.clients.nova()
        start = time.time()
        while time.time() - start < timeout:
            # key in novaclient for server volumes is get_server_volumes
            for att in nova.volumes.get_server_volumes(server_id):
                if getattr(att, "volumeId", None) == vol_id:
                    dev = getattr(att, "device", "/dev/vdb")
                    if dev:
                        return dev
            time.sleep(2)
        return "/dev/vdb"

    # ── lease management ──────────────────────────────────────────────────────

    def create_reservation(
        self,
        name: str,
        end_date: str,
        end_time: str,
        resource_type: str = "flavor:instance",
        flavor_id: Optional[str] = None,
        node_type: Optional[str] = None,
    ) -> tuple[str, str]:
        """Create a Blazar lease. Returns (reservation_id, lease_id)."""
        from chi import lease as chi_lease

        end_dt = datetime.datetime.strptime(f"{end_date} {end_time}", "%Y-%m-%d %H:%M")
        duration = end_dt - datetime.datetime.now()
        if duration.total_seconds() <= 0:
            raise RuntimeError(f"Lease end {end_date} {end_time} is in the past")

        l = chi_lease.Lease(name, duration=duration)
        if resource_type == "flavor:instance" and flavor_id:
            actual_uuid = self._resolve_flavor(flavor_id)
            l.add_flavor_reservation(id=actual_uuid, amount=1)
        l.submit(idempotent=True)

        lease_id = l.id
        res_id = ""
        if l.flavor_reservations:
            res_id = l.flavor_reservations[0].get("id", "")

        self._leases[lease_id] = l
        if res_id:
            self._res_to_lease[res_id] = lease_id
        return res_id, lease_id

    def wait_for_lease_active(self, lease_id: str) -> None:
        l = self._leases.get(lease_id)
        if l is None:
            from chi import lease as chi_lease
            l = chi_lease.get_lease(lease_id)
        l.wait(status="active")

    def get_reservation_flavor(self, reservation_id: str) -> str:
        lease_id = self._res_to_lease.get(reservation_id)
        if lease_id:
            l = self._leases.get(lease_id)
            if l:
                flavors = l.get_reserved_flavors()
                if flavors:
                    return flavors[0].name
        raise RuntimeError(f"No reserved flavor for reservation '{reservation_id}'")

    def delete_lease(self, lease_id_or_name: str) -> None:
        l = self._leases.pop(lease_id_or_name, None)
        if l:
            try:
                l.delete()
            except Exception as exc:
                log.warning("Error deleting lease %s: %s", lease_id_or_name, exc)
            return
        try:
            from chi import lease as chi_lease
            l = chi_lease.get_lease(lease_id_or_name)
            l.delete()
        except Exception as exc:
            log.warning("Could not delete lease '%s': %s", lease_id_or_name, exc)

    # ── server lifecycle ──────────────────────────────────────────────────────

    def create_vm(
        self,
        name: str,
        flavor_id: str,
        image_id: str,
        keypair: str,
        secgroup_id: str,
        network_id: str,
        reservation_id: Optional[str] = None,
    ) -> str:
        """Create a KVM VM. Returns server UUID."""
        from chi import server as chi_server

        image_name = self._resolve_image(image_id)
        actual_flavor = flavor_id
        if reservation_id:
            try:
                actual_flavor = self.get_reservation_flavor(reservation_id)
            except RuntimeError:
                pass

        s = chi_server.Server(
            name=name,
            image_name=image_name,
            flavor_name=actual_flavor,
            key_name=keypair,
            network_name=network_id,
        )
        s.submit(idempotent=True, show="text")
        if secgroup_id:
            try:
                s.add_security_group(secgroup_id)
            except Exception as exc:
                log.warning("Could not add security group '%s': %s", secgroup_id, exc)
        self._servers[s.id] = s
        return s.id

    def wait_for_active(self, server_id: str, timeout: int = 1200, interval: int = 10) -> None:
        s = self._servers.get(server_id)
        if s:
            s.wait()
            # chi's Server.wait() does not raise for ERROR — check status explicitly
            try:
                import chi
                srv = chi.connection().compute.get_server(server_id)
                if srv.status == "ERROR":
                    raise RuntimeError(f"Server {server_id} entered ERROR state")
            except RuntimeError:
                raise
            except Exception:
                pass
            return
        import chi
        conn = chi.connection()
        elapsed = 0
        while elapsed < timeout:
            srv = conn.compute.get_server(server_id)
            if srv.status == "ACTIVE":
                return
            if srv.status == "ERROR":
                raise RuntimeError(f"Server {server_id} entered ERROR state")
            time.sleep(interval)
            elapsed += interval
        raise RuntimeError(f"Server {server_id} did not reach ACTIVE in {timeout}s")

    def create_floating_ip(self, description: str) -> tuple[str, str]:
        s = None
        for srv in self._servers.values():
            if hasattr(srv, "name") and srv.name == description:
                s = srv
                break
        if s is None:
            raise RuntimeError(f"Server '{description}' not found")
        s.associate_floating_ip()
        # get_floating_ip() may return None immediately after association — retry
        for _ in range(12):
            ip = s.get_floating_ip()
            if ip:
                return ip, ip
            time.sleep(5)
        raise RuntimeError("Could not obtain floating IP after association")

    def attach_floating_ip(self, server_id: str, ip_id: str) -> None:
        pass  # already done in create_floating_ip

    def delete_floating_ip(self, ip_addr: str) -> None:
        try:
            import chi.network
            fip = chi.network.get_floating_ip(ip_addr)
            if fip:
                chi.network.delete_floating_ip(fip["id"])
        except Exception as exc:
            log.warning("Error releasing floating IP %s: %s", ip_addr, exc)

    def delete_server(self, server_id: str) -> None:
        s = self._servers.pop(server_id, None)
        if s:
            try:
                s.delete()
                return
            except Exception as exc:
                log.warning("Error deleting server via Server object: %s", exc)
        try:
            import chi
            conn = chi.connection()
            conn.compute.delete_server(server_id, ignore_missing=True)
        except Exception as exc:
            log.warning("Error deleting server %s: %s", server_id, exc)

    # ── SSH ───────────────────────────────────────────────────────────────────

    def wait_for_ssh(self, ip: str, keypair: str, timeout: int = 600, interval: int = 10) -> None:
        import subprocess
        key_path = self._key_file or os.path.expanduser(f"~/.ssh/{keypair}")
        log.info("wait_for_ssh: ip=%s key=%s timeout=%ds", ip, key_path, timeout)
        if not os.path.exists(key_path):
            log.error("wait_for_ssh: key file NOT FOUND: %s", key_path)
        elapsed = 0
        while elapsed < timeout:
            proc = subprocess.run(
                ["ssh", "-o", "StrictHostKeyChecking=no",
                 "-o", "ConnectTimeout=5", "-o", "BatchMode=yes",
                 "-i", key_path, f"cc@{ip}", "echo ready"],
                capture_output=True, text=True,
            )
            if proc.returncode == 0 and "ready" in proc.stdout:
                log.info("wait_for_ssh: connected after %ds", elapsed)
                return
            if elapsed % 30 == 0:
                log.debug(
                    "wait_for_ssh: attempt at %ds rc=%d stdout=%r stderr=%r",
                    elapsed, proc.returncode,
                    proc.stdout.strip()[:120], proc.stderr.strip()[:120],
                )
            time.sleep(interval)
            elapsed += interval
        raise RuntimeError(f"SSH to {ip} not ready after {timeout}s")

    def run_init_script(self, ip: str, keypair: str, script: str) -> str:
        """Run a bash script on the VM via SSH. Returns stdout."""
        import paramiko
        key_path = self._key_file or os.path.expanduser(f"~/.ssh/{keypair}")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(ip, username="cc", key_filename=key_path,
                           timeout=30, banner_timeout=60)
            stdin, stdout, stderr = client.exec_command("bash -s", timeout=1800)
            stdin.write(script.encode())
            stdin.channel.shutdown_write()
            exit_status = stdout.channel.recv_exit_status()
            out = stdout.read().decode()
            err = stderr.read().decode()
            if err:
                log.debug("run_init_script stderr: %s", err[:500])
            if exit_status != 0:
                raise RuntimeError(f"Script failed (exit {exit_status}): {(err or out)[-500:]}")
            return out
        finally:
            client.close()

    def close(self) -> None:
        pass


def add_virtual_instance_reservation(lease, flavor_name: str, amount: int = 1) -> None:
    """Add a flavor:instance reservation to a chi Lease by flavor name.

    KVM@TACC Blazar uses resource_type=flavor:instance (not virtual:instance).
    chi's add_flavor_reservation() expects a UUID, so we resolve the name first.
    """
    from chi.server import get_flavor

    flavor = get_flavor(flavor_name)
    log.debug("Adding flavor:instance reservation: flavor=%s id=%s amount=%d", flavor_name, flavor.id, amount)
    lease.add_flavor_reservation(id=flavor.id, amount=amount)
