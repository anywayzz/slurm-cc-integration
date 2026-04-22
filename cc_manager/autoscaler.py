"""SlurmAutoscaler — core logic for Slurm integration with Chameleon Cloud."""

from __future__ import annotations

import datetime
import logging
import os
import time
from pathlib import Path
import yaml
from typing import List, Optional

from .kvm_backend import ChiKVMManager, add_virtual_instance_reservation

log = logging.getLogger(__name__)

class SlurmAutoscaler:
    """Manages Slurm nodes on Chameleon Cloud."""

    def __init__(self, config_path: str = "configs/slurm_autoscaler.yaml"):
        p = Path(config_path)
        if not p.is_absolute():
            p = Path(__file__).parent.parent / p
        self.config_path = p
        self.config = self._load_config()
        
        # Initialize the backend
        cloud_cfg = self.config.get("cloud", {})
        openrc = cloud_cfg.get("openrc_path", "openrc.sh")
        key_file = cloud_cfg.get("key_file", "keys/kvmtacc.pvt")
        password_file = cloud_cfg.get("password_file", "")

        # Ensure paths are absolute if relative to project root
        project_root = Path(__file__).parent.parent
        if not Path(openrc).is_absolute():
            openrc = str(project_root / openrc)
        if not Path(key_file).is_absolute():
            key_file = str(project_root / key_file)
        if password_file and not Path(password_file).is_absolute():
            password_file = str(project_root / password_file)

        password = ""

        # 1. Try keyring first (most secure — encrypted, OS-managed)
        try:
            from .credentials import get_password as _kr_get
            password = _kr_get()
            if password:
                log.info("Using password from keyring.")
        except Exception as exc:
            log.debug("Keyring unavailable: %s", exc)

        # 2. Fall back to password_file (chmod 600 plain-text file)
        if not password and password_file:
            pf = Path(password_file)
            if not pf.exists():
                raise FileNotFoundError(f"password_file not found: {pf}")
            mode = pf.stat().st_mode & 0o777
            if mode & 0o077:
                log.warning("password_file %s has loose permissions (%o) — should be 600", pf, mode)
            password = pf.read_text().strip()
            log.info("Using password from password_file.")

        self.manager = ChiKVMManager(openrc_path=openrc, key_file=key_file, password=password)
        self.nodes_cfg = self.config.get("nodes", {})

    def _load_config(self) -> dict:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        with open(self.config_path, "r") as f:
            return yaml.safe_load(f)

    def resume_nodes(self, node_list: List[str]) -> None:
        """Create VMs for the requested Slurm nodes.

        KVM@TACC requires a Blazar flavor reservation before VM creation.
        We create one lease per resume batch, then create all VMs under it.
        """
        log.info("Resuming nodes: %s", node_list)

        existing = self.manager.get_servers()
        nodes_to_create = [n for n in node_list if n not in existing]
        if not nodes_to_create:
            log.info("All nodes already exist. Nothing to do.")
            return

        # ── Create Blazar lease ───────────────────────────────────────────────
        from chi import server as chi_server
        from chi import lease as chi_lease

        flavor_name = self.nodes_cfg.get("flavor", "m1.medium")
        lease_name = f"autoscaler-{'_'.join(nodes_to_create[:2])}"
        lease_hours = self.config.get("timeouts", {}).get("lease_hours", 2)

        lease = None
        lease_retries = self.config.get("timeouts", {}).get("lease_retries", 3)
        lease_retry_delay = self.config.get("timeouts", {}).get("lease_retry_delay", 60)
        for attempt in range(1, lease_retries + 1):
            try:
                log.info("Creating lease '%s' for %d node(s) (flavor=%s, attempt %d/%d)...",
                         lease_name, len(nodes_to_create), flavor_name, attempt, lease_retries)
                lease = chi_lease.Lease(lease_name,
                                        duration=datetime.timedelta(hours=lease_hours))
                add_virtual_instance_reservation(lease, flavor_name, amount=len(nodes_to_create))
                lease.submit(idempotent=True)
                log.info("Lease submitted (id=%s). Waiting for ACTIVE...", lease.id)
                lease.wait(status="active")
                log.info("Lease is ACTIVE.")
                break
            except Exception as exc:
                log.error("Lease creation attempt %d/%d failed: %s", attempt, lease_retries, exc)
                if attempt < lease_retries:
                    log.info("Retrying in %ds...", lease_retry_delay)
                    time.sleep(lease_retry_delay)
                else:
                    raise RuntimeError(f"Blazar lease creation failed after {lease_retries} attempts: {exc}") from exc

        reserved_flavors = lease.get_reserved_flavors()
        if not reserved_flavors:
            raise RuntimeError("No reserved flavors in lease after becoming ACTIVE.")
        reserved_flavor = reserved_flavors[0].name
        log.info("Reserved flavor: %s", reserved_flavor)

        # ── Create VMs ────────────────────────────────────────────────────────
        timeout = self.config.get("timeouts", {}).get("server_active", 600)
        ssh_timeout = self.config.get("timeouts", {}).get("ssh_ready", 300)
        for node_name in nodes_to_create:
            try:
                log.info("Creating VM for node %s", node_name)
                server_id = self.manager.create_vm(
                    name=node_name,
                    flavor_id=reserved_flavor,
                    image_id=self.nodes_cfg.get("image"),
                    keypair=self.nodes_cfg.get("keypair"),
                    secgroup_id=self.nodes_cfg.get("security_group", "allow-ssh"),
                    network_id=self.nodes_cfg.get("network"),
                )
                log.info("Node %s created (UUID: %s). Waiting for ACTIVE...", node_name, server_id)
                self.manager.wait_for_active(server_id, timeout=timeout)
                log.info("Node %s is ACTIVE. Setting up slurmd...", node_name)

                node_ip = self._get_node_ip(server_id)
                if node_ip:
                    self.manager.wait_for_ssh(node_ip, self.nodes_cfg.get("keypair", ""),
                                              timeout=ssh_timeout)
                    self._setup_compute_node(node_ip)
                    log.info("Node %s slurmd configured at %s.", node_name, node_ip)
                else:
                    log.warning("Node %s has no IP yet — slurmd setup skipped.", node_name)
            except Exception as e:
                log.error("Failed to resume node %s: %s", node_name, e)

    def _get_node_ip(self, server_id: str) -> str | None:
        """Return the fixed (internal) IP of a server from its network addresses."""
        try:
            import chi
            srv = chi.connection().compute.get_server(server_id)
            for net_addrs in srv.addresses.values():
                for addr in net_addrs:
                    if addr.get("OS-EXT-IPS:type") == "fixed":
                        return addr["addr"]
        except Exception as exc:
            log.debug("Could not get IP for %s: %s", server_id, exc)
        return None

    def _setup_compute_node(self, ip: str) -> None:
        """Copy munge.key + slurm.conf and run 03_slurm_node.sh on a compute node."""
        import paramiko
        import subprocess
        import tempfile

        key_file = self.manager._key_file
        log.info("SSH setup for compute node at %s", ip)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(ip, username="cc", key_filename=key_file, timeout=30, banner_timeout=60)

        # Read munge.key via sudo (cc user cannot read /etc/munge/munge.key directly)
        munge_key_data = subprocess.check_output(["sudo", "cat", "/etc/munge/munge.key"])

        sftp = client.open_sftp()
        with sftp.open("/tmp/munge.key", "wb") as remote_f:
            remote_f.write(munge_key_data)
        sftp.put("/etc/slurm/slurm.conf", "/tmp/slurm.conf")

        project_root = Path(__file__).parent.parent
        sftp.put(str(project_root / "scripts" / "03_slurm_node.sh"), "/tmp/03_slurm_node.sh")
        sftp.close()

        failed = False
        for cmd in ["chmod +x /tmp/03_slurm_node.sh", "bash /tmp/03_slurm_node.sh"]:
            _, stdout, stderr = client.exec_command(f"bash -l -c '{cmd}'")
            exit_code = stdout.channel.recv_exit_status()
            if exit_code != 0:
                err = stderr.read().decode()
                client.close()
                raise RuntimeError(f"compute setup cmd failed ({cmd}): {err}")
            out = stdout.read().decode().strip()
            if out:
                log.info(out)
        client.close()

    def suspend_nodes(self, node_list: List[str]) -> None:
        """Delete VMs for the requested Slurm nodes."""
        log.info("Suspending nodes: %s", node_list)
        
        existing = self.manager.get_servers()
        for node_name in node_list:
            if node_name in existing:
                try:
                    server_id = existing[node_name]["id"]
                    log.info("Deleting VM for node %s (UUID: %s)", node_name, server_id)
                    self.manager.delete_server(server_id)
                    
                    # Cleanup floating IP if any
                    ip = existing[node_name].get("ip")
                    if ip:
                        log.info("Releasing IP %s for node %s", ip, node_name)
                        self.manager.delete_floating_ip(ip)
                        
                except Exception as e:
                    log.error("Failed to suspend node %s: %s", node_name, e)
            else:
                log.warning("Node %s not found in active servers. Skipping.", node_name)
