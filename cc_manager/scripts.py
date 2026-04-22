import os
import logging
from pathlib import Path

log = logging.getLogger(__name__)

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"

def load_scripts() -> dict[str, dict]:
    """Load script metadata from the scripts/ folder.
    
    Expected format in the .sh file:
    # LABEL: My Script Label
    """
    scripts = {}
    if not SCRIPTS_DIR.exists():
        SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
    
    for p in sorted(SCRIPTS_DIR.glob("*.sh")):
        try:
            body = p.read_text(encoding="utf-8")
            label = p.name
            # Parse label from comment if present
            for line in body.splitlines():
                if "# LABEL:" in line:
                    label = line.split("# LABEL:", 1)[1].strip()
                    break
            scripts[p.stem] = {"label": label, "body": body}
        except Exception as e:
            log.error("Failed to load script %s: %s", p.name, e)
    return scripts

# Keep BUILT_IN_SCRIPTS for backward compatibility or as a fallback
BUILT_IN_SCRIPTS = load_scripts()


def make_volume_setup_script(device: str = "/dev/vdb", mount_point: str = "/mnt/data") -> str:
    """Return a bash script that mounts the volume and moves Docker data onto it."""
    return f"""\
#!/bin/bash
set -e

DEVICE="{device}"
MOUNT="{mount_point}"

echo "Waiting for device $DEVICE to appear..."
# Wait up to 60s for the kernel to create the device node
for i in {{1..60}}; do
    if [ -b "$DEVICE" ]; then
        echo "Device $DEVICE found."
        break
    fi
    sleep 1
done

if [ ! -b "$DEVICE" ]; then
    echo "Error: Device $DEVICE not found after 60 seconds."
    exit 1
fi

# Format ONLY if blank (safe check)
if sudo wipefs -n "$DEVICE" | grep -q "offset"; then
    echo "Existing filesystem or partition table detected on $DEVICE. Skipping format."
else
    echo "No filesystem detected on $DEVICE. Formatting as ext4..."
    # -F to force if it's a raw block device but we already checked wipefs
    sudo mkfs.ext4 -F "$DEVICE"
fi

# Mount
sudo mkdir -p "$MOUNT"
if ! mountpoint -q "$MOUNT"; then
    sudo mount "$DEVICE" "$MOUNT"
fi

# Persist across reboots
FSTAB_LINE="$DEVICE $MOUNT ext4 defaults,nofail 0 2"
if ! grep -q "$DEVICE" /etc/fstab; then
    echo "Adding $DEVICE to /etc/fstab"
    echo "$FSTAB_LINE" | sudo tee -a /etc/fstab
fi

# Move Docker data directory to the volume (if docker is installed)
if command -v docker &>/dev/null; then
    sudo systemctl stop docker 2>/dev/null || true
    sudo mkdir -p "$MOUNT/docker"
    if [ -d /var/lib/docker ] && [ ! -L /var/lib/docker ]; then
        echo "Moving existing Docker data to volume..."
        sudo rsync -aP /var/lib/docker/ "$MOUNT/docker/" 2>/dev/null || true
        sudo rm -rf /var/lib/docker
        sudo ln -s "$MOUNT/docker" /var/lib/docker
    fi
    sudo chown root:root "$MOUNT/docker"
    sudo chmod 711 "$MOUNT/docker"
    sudo systemctl start docker 2>/dev/null || true
else
    # Pre-create docker dir so it's ready when docker is installed later
    sudo mkdir -p "$MOUNT/docker"
    sudo chown root:root "$MOUNT/docker"
    sudo chmod 711 "$MOUNT/docker"
fi

echo "Volume setup complete: $DEVICE mounted at $MOUNT"
"""
