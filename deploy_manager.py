"""deploy_manager.py — Automate deployment of Slurm Manager to Chameleon Cloud.

Flow:
  1. Check for existing ACTIVE VM and reuse it if present
  2. Create Blazar lease with flavor reservation (required on KVM@TACC)
     -- OR skip lease with --no-lease (use flavor name directly, fallback)
  3. Wait for lease ACTIVE
  4. Ensure 'allow-ssh' security group exists
  5. Create Manager VM using the reserved flavor
  6. Wait for ACTIVE (raises on ERROR)
  7. Assign floating IP
  8. Wait for SSH (20 min timeout for cloud-init), upload project, run setup
"""

import argparse
import datetime
import logging
import os
import tarfile
import tempfile
from pathlib import Path
from cc_manager.kvm_backend import ChiKVMManager, add_virtual_instance_reservation

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("deploy_manager")

# ── Configuration ────────────────────────────────────────────────────────────
OPENRC      = "openrc.sh"
KEY_FILE    = "keys/kvmtacc.pvt"
VM_NAME     = "slurm-manager"
KEYPAIR     = "kvmtacckey"
FLAVOR      = "m1.small"
IMAGE       = "CC-Ubuntu24.04"
NETWORK     = "sharednet1"
SG_NAME     = "allow-ssh"
LEASE_NAME  = f"{VM_NAME}-lease"
LEASE_HOURS = 8      # default lease duration; override with --lease-hours
SSH_TIMEOUT = 1200   # 20 minutes — CC-Ubuntu24.04 cloud-init can be slow


def create_project_tarball() -> str:
    """Create a tarball of the current project directory."""
    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
        with tarfile.open(fileobj=tmp, mode="w:gz") as tar:
            project_root = Path(__file__).parent
            for item in project_root.iterdir():
                if item.name in ["venv", ".git", "__pycache__", ".gemini", "cc-vm-manager.log"]:
                    continue
                tar.add(item, arcname=item.name)
        return tmp.name


def ensure_allow_ssh_sg() -> None:
    """Create 'allow-ssh' security group with TCP/22 ingress if it doesn't exist."""
    import chi.network as chi_network
    sg_list = chi_network.list_security_groups(name_filter=SG_NAME)
    if sg_list:
        log.info("Security group '%s' already exists.", SG_NAME)
        return
    log.info("Creating security group '%s' with TCP/22 ingress...", SG_NAME)
    sg = chi_network.SecurityGroup({
        "name": SG_NAME,
        "description": "Enable SSH traffic on TCP port 22",
    })
    sg.add_rule("ingress", "tcp", 22)
    sg.submit()
    log.info("Security group '%s' created.", SG_NAME)


def _run_setup(manager: ChiKVMManager, ip: str) -> None:
    """Upload project files and run setup script on the manager VM."""
    tarball = create_project_tarball()
    log.info("Uploading project files to %s...", ip)

    import paramiko
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(ip, username="cc", key_filename=KEY_FILE)

    sftp = ssh_client.open_sftp()
    sftp.put(tarball, "/home/cc/project.tar.gz")
    os.unlink(tarball)

    log.info("Running setup script on Manager...")
    commands = [
        "mkdir -p cc-slurm-autoscaler",
        "tar -xzf project.tar.gz -C cc-slurm-autoscaler",
        "cd cc-slurm-autoscaler && chmod +x scripts/02_slurm_manager.sh",
        "cd cc-slurm-autoscaler && ./scripts/02_slurm_manager.sh",
    ]
    for cmd in commands:
        log.info("Running: %s", cmd)
        stdin, stdout, stderr = ssh_client.exec_command(f"bash -l -c '{cmd}'")
        exit_code = stdout.channel.recv_exit_status()
        out = stdout.read().decode()
        err = stderr.read().decode()
        if exit_code != 0:
            log.error("Command failed (exit=%d):\nSTDOUT: %s\nSTDERR: %s", exit_code, out, err)
            raise RuntimeError(f"Remote command failed: {cmd}")
        if out.strip():
            log.info(out.strip())

    ssh_client.close()
    log.info("Deployment complete! Manager is ready at %s", ip)
    log.info("SSH: ssh -i %s cc@%s", KEY_FILE, ip)


def _get_floating_ip_for_server(server_id: str) -> str | None:
    """Return the floating IP of an existing server, or None."""
    import chi
    try:
        conn = chi.connection()
        for fip in conn.network.ips():
            if fip.fixed_ip_address and fip.port_id:
                port = conn.network.get_port(fip.port_id)
                if port and port.device_id == server_id:
                    return fip.floating_ip_address
    except Exception as exc:
        log.debug("Could not enumerate floating IPs: %s", exc)
    return None


def _finish_deployment(manager: ChiKVMManager, server_id: str) -> None:
    """Wait for ACTIVE, assign floating IP, wait for SSH, run setup."""
    log.info("Waiting for VM ACTIVE...")
    manager.wait_for_active(server_id)
    log.info("Server is ACTIVE.")

    ip, _ = manager.create_floating_ip(VM_NAME)
    log.info("Manager IP: %s. Waiting for SSH (up to %d min for cloud-init)...",
             ip, SSH_TIMEOUT // 60)

    try:
        manager.wait_for_ssh(ip, KEYPAIR, timeout=SSH_TIMEOUT)
    except RuntimeError:
        log.error(
            "SSH timeout after %d min. VM is still running.\n"
            "  Try manually: ssh -i %s -o StrictHostKeyChecking=no cc@%s\n"
            "  Then re-run deploy_manager.py — the running VM will be reused.",
            SSH_TIMEOUT // 60, KEY_FILE, ip,
        )
        raise

    _run_setup(manager, ip)


def deploy(no_lease: bool = False, lease_hours: int = LEASE_HOURS) -> None:
    manager = ChiKVMManager(openrc_path=OPENRC, key_file=KEY_FILE)

    import chi
    import chi.server as chi_server

    # ── Step 1: Check for existing ACTIVE VM ──────────────────────────────────
    existing_id = None
    try:
        existing_id = chi_server.get_server_id(VM_NAME)
    except Exception:
        pass

    if existing_id:
        srv = chi.connection().compute.get_server(existing_id)
        if srv.status == "ACTIVE":
            log.info("Found existing ACTIVE VM (%s). Reusing it.", existing_id)
            existing_ip = _get_floating_ip_for_server(existing_id)
            if existing_ip:
                log.info("Manager IP: %s. Waiting for SSH...", existing_ip)
                manager.wait_for_ssh(existing_ip, KEYPAIR, timeout=SSH_TIMEOUT)
                _run_setup(manager, existing_ip)
                return
            log.info("No floating IP on existing VM — will assign one.")
            _finish_deployment(manager, existing_id)
            return
        else:
            log.info("Existing VM (%s) in status=%s — deleting for a clean deploy.",
                     existing_id, srv.status)
            manager.delete_server(existing_id)
            import time; time.sleep(5)

    if no_lease:
        # ── Step 2b (fallback): Skip Blazar, use flavor name directly ─────────
        log.warning("--no-lease: skipping Blazar reservation, using flavor '%s' directly.", FLAVOR)
        log.warning("This may fail if the project quota requires a reservation.")
        ensure_allow_ssh_sg()
        log.info("Creating Slurm Manager VM: %s (flavor=%s, image=%s)", VM_NAME, FLAVOR, IMAGE)
        server_id = manager.create_vm(
            name=VM_NAME,
            flavor_id=FLAVOR,
            image_id=IMAGE,
            keypair=KEYPAIR,
            secgroup_id=SG_NAME,
            network_id=NETWORK,
        )
        _finish_deployment(manager, server_id)
        return

    # ── Step 2: Create Blazar lease with flavor reservation ───────────────────
    log.info("Creating lease '%s' (flavor=%s, duration=%dh)", LEASE_NAME, FLAVOR, lease_hours)

    from chi import lease as chi_lease
    duration = datetime.timedelta(hours=lease_hours)
    lease = chi_lease.Lease(LEASE_NAME, duration=duration)
    add_virtual_instance_reservation(lease, FLAVOR, amount=1)
    lease.submit(idempotent=True)
    log.info("Lease submitted (id=%s). Waiting for ACTIVE...", lease.id)

    server_id = None
    try:
        # ── Step 3: Wait for lease ACTIVE ─────────────────────────────────────
        lease.wait(status="active")
        log.info("Lease is ACTIVE.")

        reserved_flavors = lease.get_reserved_flavors()
        if not reserved_flavors:
            raise RuntimeError("No reserved flavors in lease after becoming ACTIVE.")
        reserved_flavor_name = reserved_flavors[0].name
        log.info("Reserved flavor: %s", reserved_flavor_name)

        # ── Step 4: Ensure allow-ssh security group ────────────────────────────
        ensure_allow_ssh_sg()

        # ── Step 5: Create Manager VM ─────────────────────────────────────────
        log.info("Creating Slurm Manager VM: %s (flavor=%s, image=%s)",
                 VM_NAME, reserved_flavor_name, IMAGE)
        server_id = manager.create_vm(
            name=VM_NAME,
            flavor_id=reserved_flavor_name,
            image_id=IMAGE,
            keypair=KEYPAIR,
            secgroup_id=SG_NAME,
            network_id=NETWORK,
        )

        # ── Steps 6-8: Wait ACTIVE, floating IP, SSH, setup ───────────────────
        _finish_deployment(manager, server_id)

    except Exception:
        if server_id is None:
            log.exception("Failed before VM creation. Cleaning up lease...")
            try:
                lease.delete()
                log.info("Lease deleted.")
            except Exception as exc:
                log.warning("Could not delete lease: %s", exc)
        else:
            log.error(
                "VM '%s' (id=%s) may still be running. Lease id=%s is kept.",
                VM_NAME, server_id, lease.id,
            )
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deploy Slurm Manager to Chameleon Cloud")
    parser.add_argument(
        "--no-lease",
        action="store_true",
        help="Skip Blazar flavor reservation and use the flavor name directly. "
             "Use when Blazar is unavailable (may fail if quota requires a lease).",
    )
    parser.add_argument(
        "--lease-hours",
        type=int,
        default=LEASE_HOURS,
        metavar="N",
        help=f"Duration of the Blazar manager lease in hours (default: {LEASE_HOURS}).",
    )
    args = parser.parse_args()
    deploy(no_lease=args.no_lease, lease_hours=args.lease_hours)
