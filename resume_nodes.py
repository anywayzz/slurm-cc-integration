#!/usr/bin/env python3
import sys
import os
import time
import subprocess
import logging
import chi
from chi import hardware
from dotenv import load_dotenv
from collections import defaultdict
from typing import List, Dict, Tuple

# Carica configurazione dal file .env
load_dotenv()

# Configurazione logging
logging.basicConfig(
    filename='/var/log/slurm/resume_nodes.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_env_config() -> Dict:
    """Carica la configurazione dal file .env"""
    config = {
        'site': os.getenv('CHAMELEON_SITE', 'CHI@UC'),
        'key_name': os.getenv('CHAMELEON_KEY_NAME'),
        'credentials_file': os.getenv('CREDENTIALS_FILE', '/etc/slurm/chameleon-creds.sh'),
        'lease_hours': int(os.getenv('DEFAULT_LEASE_HOURS', '4')),
        'min_cores': int(os.getenv('MIN_CORES_PER_NODE', '8')),
        'target_cores': int(os.getenv('PREFERRED_CORES_TARGET', '24')),
        'max_nodes': int(os.getenv('MAX_NODES_PER_REQUEST', '5')),
        'head_node_ip': os.getenv('SLURM_HEAD_NODE_IP'),
        'preferred_types': os.getenv('PREFERRED_NODE_TYPES', 'compute_skylake').split(',')
    }
    
    if not config['key_name']:
        raise ValueError("CHAMELEON_KEY_NAME deve essere specificato nel file .env")
    
    return config

def source_credentials(credentials_file: str) -> bool:
    """Carica le credenziali ChameleonCloud"""
    try:
        # Carica le variabili d'ambiente dal file delle credenziali
        with open(credentials_file, 'r') as f:
            for line in f:
                if line.startswith('export '):
                    # Rimuovi 'export ' e splitta su '='
                    line = line[7:].strip()
                    if '=' in line:
                        key, value = line.split('=', 1)
                        # Rimuovi virgolette se presenti
                        value = value.strip('"\'')
                        os.environ[key] = value
        return True
    except Exception as e:
        logging.error(f"Errore nel caricamento delle credenziali: {e}")
        return False

def get_available_nodes_by_type() -> Dict[str, List]:
    """Ottieni nodi disponibili raggruppati per tipo"""[1][18][20]
    available_nodes = defaultdict(list)
    
    try:
        # Ottieni tutti i tipi di nodo disponibili
        node_types = chi.hardware.get_node_types()
        
        for node_type in node_types:
            # Filtra solo nodi disponibili
            nodes = hardware.get_nodes(node_type=node_type, filter_reserved=True)
            if nodes:
                available_nodes[node_type] = nodes
                logging.info(f"Trovati {len(nodes)} nodi {node_type} disponibili")
    
    except Exception as e:
        logging.error(f"Errore nel recupero dei nodi disponibili: {e}")
    
    return available_nodes

def calculate_node_cores(node) -> int:
    """Calcola il numero totale di core per un nodo"""[12][24]
    try:
        if hasattr(node, 'cpu') and node.cpu:
            # Estrai informazioni CPU dal nodo
            cores_per_socket = node.cpu.get('cores', 1)
            sockets = node.cpu.get('sockets', 1)
            threads_per_core = node.cpu.get('threads_per_core', 1)
            
            # Calcola core totali (senza hyperthreading)
            total_cores = cores_per_socket * sockets
            return total_cores
        else:
            # Fallback: usa valori predefiniti per tipo di nodo
            node_core_map = {
                'compute_skylake': 24,
                'compute_cascadelake_r': 48,
                'compute_haswell': 24,
                'compute_arm64': 64,
                'gpu_v100': 20
            }
            return node_core_map.get(node.type, 8)
    except Exception as e:
        logging.warning(f"Errore nel calcolo dei core per {node.name}: {e}")
        return 8  # Valore di fallback

def select_optimal_nodes(available_nodes: Dict, target_cores: int, max_nodes: int, preferred_types: List[str]) -> List[Tuple[str, str, int]]:
    """Seleziona la combinazione ottimale di nodi per raggiungere il target di core"""
    selected_nodes = []
    total_cores = 0
    
    # Ordina i tipi di nodo per preferenza
    ordered_types = []
    for pref_type in preferred_types:
        if pref_type in available_nodes:
            ordered_types.append(pref_type)
    
    # Aggiungi eventuali tipi rimanenti
    for node_type in available_nodes:
        if node_type not in ordered_types:
            ordered_types.append(node_type)
    
    logging.info(f"Tipi di nodo ordinati per preferenza: {ordered_types}")
    
    for node_type in ordered_types:
        if len(selected_nodes) >= max_nodes or total_cores >= target_cores:
            break
            
        nodes_of_type = available_nodes[node_type]
        
        for node in nodes_of_type:
            if len(selected_nodes) >= max_nodes or total_cores >= target_cores:
                break
                
            node_cores = calculate_node_cores(node)
            
            if node_cores >= 8:  # Solo nodi con almeno 8 core
                selected_nodes.append((node_type, node.uid, node_cores))
                total_cores += node_cores
                logging.info(f"Selezionato nodo {node.name} ({node_type}) con {node_cores} core")
    
    logging.info(f"Combinazione finale: {len(selected_nodes)} nodi con {total_cores} core totali")
    return selected_nodes

def create_intelligent_reservation(node_count: int, config: Dict) -> Tuple[str, List]:
    """Crea una reservation intelligente basata sulla disponibilità"""[8][18]
    try:
        # Configura python-chi
        chi.set('project_name', os.environ.get('OS_PROJECT_NAME'))
        chi.use_site(config['site'])
        
        # Ottieni nodi disponibili
        available_nodes = get_available_nodes_by_type()
        
        if not available_nodes:
            raise Exception("Nessun nodo disponibile")
        
        # Seleziona combinazione ottimale
        optimal_nodes = select_optimal_nodes(
            available_nodes, 
            config['target_cores'] * node_count,
            min(node_count, config['max_nodes']),
            config['preferred_types']
        )
        
        if not optimal_nodes:
            raise Exception("Impossibile trovare una combinazione soddisfacente di nodi")
        
        # Crea reservations per i nodi selezionati
        reservations = []
        node_details = []
        
        for node_type, node_uid, cores in optimal_nodes:
            chi.lease.add_node_reservation(
                reservations,
                node_type=node_type,
                count=1
            )
            node_details.append({
                'type': node_type,
                'uid': node_uid,
                'cores': cores
            })
        
        # Calcola durata ottimale del lease
        start_date, end_date = chi.lease.lease_duration(hours=config['lease_hours'])
        
        # Crea il lease
        lease_name = f"slurm-auto-{int(time.time())}"
        lease = chi.lease.create_lease(
            lease_name,
            reservations,
            start_date=start_date,
            end_date=end_date
        )
        
        logging.info(f"Lease {lease_name} creato con {len(optimal_nodes)} nodi")
        return lease['uuid'], node_details
        
    except Exception as e:
        logging.error(f"Errore nella creazione della reservation intelligente: {e}")
        raise

def create_chameleon_node(node_name: str, config: Dict) -> bool:
    """Crea un nodo su ChameleonCloud con selezione intelligente"""
    try:
        # Crea reservation intelligente
        lease_uuid, node_details = create_intelligent_reservation(1, config)
        
        # Attendi che il lease sia attivo
        chi.lease.wait_for_active(lease_uuid)
        
        # Crea l'istanza sul primo nodo disponibile
        node_detail = node_details[0]
        
        server = chi.server.create_server(
            server_name=node_name,
            image_name='CC-Ubuntu22.04',
            flavor_name='baremetal',
            key_name=config['key_name'],
            reservation_id=lease_uuid
        )
        
        # Attendi che l'istanza sia attiva
        chi.server.wait_for_active(server.id)
        
        # Configura il nodo per SLURM
        setup_slurm_node(server, node_name, node_detail['cores'], config)
        
        logging.info(f"Nodo {node_name} creato con {node_detail['cores']} core")
        return True
        
    except Exception as e:
        logging.error(f"Errore nella creazione del nodo {node_name}: {e}")
        return False

def setup_slurm_node(server, node_name: str, cores: int, config: Dict):
    """Configura il nodo per SLURM con informazioni sui core"""
    try:
        # Ottieni l'IP del server
        server_ip = server.accessIPv4 or server.networks['sharednet1'][0]
        
        # Script di setup remoto
        setup_script = f"""#!/bin/bash
        # Installa SLURM
        apt update
        apt install -y slurmd
        
        # Copia la configurazione SLURM
        scp -o StrictHostKeyChecking=no slurm@{config['head_node_ip']}:/etc/slurm-llnl/slurm.conf /etc/slurm-llnl/
        scp -o StrictHostKeyChecking=no slurm@{config['head_node_ip']}:/etc/munge/munge.key /etc/munge/
        
        # Avvia i servizi
        systemctl start munge
        systemctl start slurmd
        systemctl enable munge
        systemctl enable slurmd
        """
        
        # Esegui il setup
        subprocess.run([
            'ssh', '-o', 'StrictHostKeyChecking=no',
            f'cc@{server_ip}', setup_script
        ], check=True)
        
        # Aggiungi il nodo a SLURM con informazioni sui core
        subprocess.run([
            'scontrol', 'update', 
            f'NodeName={node_name}', 
            f'NodeAddr={server_ip}',
            f'CPUs={cores}',
            'State=RESUME'
        ], check=True)
        
        logging.info(f"Setup completato per {node_name} con {cores} core")
        
    except Exception as e:
        logging.error(f"Errore nel setup di {node_name}: {e}")

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
            logging.info(f"Avvio intelligente del nodo: {node}")
            
            if create_chameleon_node(node, config):
                logging.info(f"Nodo {node} avviato con successo")
            else:
                logging.error(f"Fallimento nell'avvio del nodo {node}")
                
    except Exception as e:
        logging.error(f"Errore fatale: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
