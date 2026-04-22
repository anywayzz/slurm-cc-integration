# Slurm Autoscaler for Chameleon Cloud (KVM@TACC)

Automatically provisions and tears down KVM compute nodes on Chameleon Cloud in response to Slurm job queue demand.

## How it works

1. You submit jobs with `sbatch` on the manager VM.
2. Slurm calls `slurm_resume.py` when idle nodes need to be powered on.
3. The autoscaler creates a Blazar lease, boots a VM, installs `slurmd`, and registers the node.
4. When the node is idle long enough, Slurm calls `slurm_suspend.py` to delete the VM and release the lease.

## Quick start

```bash
# 1. Clone this repo on your local machine
git clone <this-repo>
cd slurm-cc-integration

# 2. Install dependencies
pip install -r requirements.txt

# 3. Source your Chameleon Application Credential (must be "unrestricted" type)
source <your-app-cred-openrc.sh>

# 4. Edit configs/slurm_autoscaler.yaml with your project/key/network details

# 5. Deploy the manager VM (default 8h lease; override with --lease-hours N)
python3 deploy_manager.py --lease-hours 24
```

## Full tutorial

Step-by-step guide including prerequisites, Chameleon setup, troubleshooting common errors, and node reset procedures:

**[https://docs.google.com/document/d/1-Q1PmtpXQl3KRFOUYaBYp5hwZdZiNU2tsuLex8-s5qM/edit?usp=sharing](https://docs.google.com/document/d/1-Q1PmtpXQl3KRFOUYaBYp5hwZdZiNU2tsuLex8-s5qM/edit?usp=sharing)**

## Repository structure

```
deploy_manager.py        # Deploy the Slurm manager VM on Chameleon
slurm_resume.py          # Called by Slurm to power on compute nodes
slurm_suspend.py         # Called by Slurm to power off compute nodes
cc_manager/              # Autoscaler core (autoscaler, kvm_backend, config)
configs/
  slurm_autoscaler.yaml  # Main configuration (edit this)
  slurm.conf.template    # Slurm config template (auto-deployed)
  config.yaml            # VM manager config
scripts/
  02_slurm_manager.sh    # Manager node setup script
  03_slurm_node.sh       # Compute node setup script
```

## Requirements

- Python 3.10+
- Chameleon Cloud account with KVM@TACC access
- An **unrestricted** Application Credential (required for Blazar lease creation)
- SSH key pair registered in Chameleon
