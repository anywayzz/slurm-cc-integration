#!/usr/bin/env python3
import sys
import os
import subprocess
import logging
import chi
from dotenv import load_dotenv
from typing import Dict

# Carica configurazione dal file .env
load_dotenv()

# Configurazione logging
logging.basicConfig(
    filename='/var/log/slurm/suspend_nodes.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_env_config() -> Dict:
    """Carica la configurazione dal file .env"""
    return {
        'site': os.getenv('CHAMELEON_SITE', 'CHI@UC'),
        'credentials_file': os.getenv('CREDENTIALS_FILE', '/etc/slurm/chameleon-creds.sh')
    }

def source_credentials(credentials_file: str) -> bool:
    """Carica le credenziali ChameleonCloud"""
    try:
        with open(credentials_file, 'r') as f:
            for line in f:
                if line.startswith('export '):
                    line = line[7:].strip()
                    if '=' in line:
                        key, value = line.split('=', 1)
                        value = value.strip('"\'')
                        os.environ[key] = value
        return True
    except Exception as e:
        logging.error(f"Errore nel caricamento delle credenziali: {e}")
        return False

def delete_chameleon_node(node_name: str, config: Dict) -> bool:
    """Elimina un nodo da ChameleonCloud"""
    try:
        # Configura python-chi
        chi.set('project_name', os.environ.get('OS_PROJECT_NAME'))
        chi.use_site(config['site'])
        
        # Trova il server
        servers = chi.server.list_servers()
        target_server = None
        
        for server in servers:
            if server.name == node_name:
                target_server = server
                break
        
        if not target_server:
            logging.warning(f"Server {node_name} non trovato")
            return True
        
        # Trova il lease associato
        leases = chi.lease.list_leases()
        target_lease = None
        
        for lease in leases:
            if 'slurm' in lease['name'].lower() and node_name in lease.get('reservations', []):
                target_lease = lease
                break
        
        # Elimina il server
        chi.server.delete_server(target_server.id)
        logging.info(f"Server {node_name} eliminato")
        
        # Elimina il lease se trovato
        if target_lease:
            chi.lease.delete_lease(target_lease['uuid'])
            logging.info(f"Lease {target_lease['name']} eliminato")
        
        logging.info(f"Nodo {node_name} eliminato completamente")
        return True
        
    except Exception as e:
        logging.error(f"Errore nell'eliminazione del nodo {node_name}: {e}")
        return False

def main():
    if len(sys.argv) < 2:
        logging.error("Nessun nodo specificato")
        sys.exit(1)
    
    try:
        # Carica configurazione
        config = load_env_config()
        
        # Carica credenziali
        if not source_credentials(config['credentials_file']):
            sys.exit(1)
        
        # Processa ogni nodo
        nodes = sys.argv[1].split(',')
        
        for node in nodes:
            node = node.strip()
            logging.info(f"Spegnimento del nodo: {node}")
            
            # Rimuovi il nodo da SLURM
            subprocess.run([
                'scontrol', 'update', 
                f'NodeName={node}', 
                'State=POWER_DOWN'
            ])
            
            if delete_chameleon_node(node, config):
                logging.info(f"Nodo {node} spento con successo")
            else:
                logging.error(f"Fallimento nello spegnimento del nodo {node}")
                
    except Exception as e:
        logging.error(f"Errore fatale: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
