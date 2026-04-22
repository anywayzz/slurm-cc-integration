
from cc_manager.kvm_backend import ChiKVMManager
import logging

logging.basicConfig(level=logging.INFO)

def discover():
    openrc = "openrc.sh"
    key_file = "keys/kvmtacc.pvt"
    if not open(openrc).read():
        print("openrc.sh is empty or missing")
        return

    manager = ChiKVMManager(openrc_path=openrc, key_file=key_file)
    
    print("\n--- Networks ---")
    try:
        networks = manager.conn.network.networks()
        for net in networks:
            print(f"Name: {net.name}, ID: {net.id}, Shared: {net.is_shared}, External: {net.is_router_external}")
    except Exception as e:
        print(f"Error listing networks: {e}")

    print("\n--- Flavors ---")
    try:
        flavors = manager.conn.compute.flavors()
        for f in flavors:
            print(f"Name: {f.name}, ID: {f.id}, VCPUs: {f.vcpus}, RAM: {f.ram}")
    except Exception as e:
        print(f"Error listing flavors: {e}")

    print("\n--- Images ---")
    try:
        images = manager.conn.compute.images()
        for i in images:
            if "Ubuntu" in i.name:
                print(f"Name: {i.name}, ID: {i.id}")
    except Exception as e:
        print(f"Error listing images: {e}")

if __name__ == "__main__":
    discover()
