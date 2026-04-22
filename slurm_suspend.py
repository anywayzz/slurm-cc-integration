#!/usr/bin/env python3
"""SuspendProgram script for Slurm."""

import sys
import logging
import re
from pathlib import Path

# Ensure the project root is on sys.path (Slurm may call this from a different CWD)
_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from cc_manager.autoscaler import SlurmAutoscaler

# Use absolute log path so it's always written to the project directory
_LOG_FILE = _SCRIPT_DIR / "slurm_autoscaler.log"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(str(_LOG_FILE), mode="a")]
)
log = logging.getLogger("slurm_suspend")

def expand_hostlist(hostlist: str) -> list[str]:
    """Simple hostlist expander for formats like compute-[1-3,5] or compute-1 compute-2."""
    if " " in hostlist:
        return hostlist.split()
    
    match = re.match(r"([^\[]+)\[([^\]]+)\]", hostlist)
    if not match:
        return [hostlist]
    
    prefix = match.group(1)
    ranges = match.group(2).split(",")
    nodes = []
    for r in ranges:
        if "-" in r:
            start, end = map(int, r.split("-"))
            for i in range(start, end + 1):
                nodes.append(f"{prefix}{i}")
        else:
            nodes.append(f"{prefix}{r}")
    return nodes

def main():
    if len(sys.argv) < 2:
        log.error("Usage: %s <hostlist>", sys.argv[0])
        sys.exit(1)

    hostlist = sys.argv[1]
    log.info("Suspend request for: %s", hostlist)
    
    try:
        nodes = expand_hostlist(hostlist)
        scaler = SlurmAutoscaler()
        scaler.suspend_nodes(nodes)
        log.info("Suspend completed for: %s", nodes)
    except Exception as e:
        log.error("Failed to suspend: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
