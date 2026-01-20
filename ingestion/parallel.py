import logging
import multiprocessing
import os
import random
import time
import sys
from typing import List, Callable, Any, Dict, Tuple

from ingestion.config import (
    LOG_FORMAT, LOG_DATE_FORMAT, 
    WORKER_STAGGER_MIN, WORKER_STAGGER_MAX
)
from ingestion.utils import FatalIngestionError
from db.utils.logging_handler import SQLAlchemyHandler

logger = logging.getLogger(__name__)

def setup_worker_logging(worker_name: str):
    """Configura el logging para un worker en la base de datos."""
    # Limpiar handlers existentes
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)
        
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
        handlers=[
            SQLAlchemyHandler(),
            # No usamos StreamHandler en workers para no saturar la consola principal si hay muchos
        ]
    )
    
    # Silenciar otros loggers ruidosos si es necesario
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('nba_api').setLevel(logging.WARNING)

def run_worker_with_stagger(worker_func: Callable, name: str, *args, **kwargs):
    """Ejecuta una función de worker con un retraso inicial aleatorio."""
    setup_worker_logging(name)
    worker_logger = logging.getLogger(__name__)
    
    # Jitter inicial desde configuración
    delay = random.uniform(WORKER_STAGGER_MIN, WORKER_STAGGER_MAX)
    worker_logger.info(f"Worker {name} iniciando con delay de {delay:.2f}s...")
    time.sleep(delay)
    
    try:
        worker_func(*args, **kwargs)
        sys.exit(0)
    except FatalIngestionError as e:
        worker_logger.error(f"🔴 ERROR FATAL en worker {name}: {e}")
        sys.exit(42) # Código especial para FatalIngestionError
    except Exception as e:
        worker_logger.error(f"❌ Error inesperado en worker {name}: {e}", exc_info=True)
        sys.exit(1)

def run_parallel_task(
    task_func: Callable, 
    items: List[Any], 
    num_workers: int, 
    prefix: str,
    worker_name_func: Callable[[int], str]
):
    """Ejecuta una tarea en paralelo dividiendo los items entre los workers.
    
    Si num_workers es 1, se ejecuta de forma secuencial para ahorrar memoria.
    """
    if not items:
        return

    # Si solo hay 1 worker, ejecutamos secuencialmente para evitar overhead de procesos y RAM
    if num_workers <= 1:
        logger.info(f"Ejecutando tarea {prefix} de forma secuencial (1 worker)...")
        try:
            task_func(1, items)
            return
        except Exception as e:
            logger.error(f"Error en tarea secuencial {prefix}: {e}")
            raise

    chunk_size = max(1, len(items) // num_workers)
    chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
    
    processes = {}
    for i, chunk in enumerate(chunks):
        batch_id = i + 1
        name = worker_name_func(batch_id)
        p = multiprocessing.Process(
            target=run_worker_with_stagger,
            args=(task_func, f"{prefix}_{batch_id}", batch_id, chunk),
            name=name
        )
        p.start()
        processes[str(batch_id)] = p
        
    # Supervise
    active_processes = processes.copy()
    while active_processes:
        for bid_str, p in list(active_processes.items()):
            if not p.is_alive():
                if p.exitcode == 0:
                    active_processes.pop(bid_str)
                else:
                    if p.exitcode == 42:
                        logger.error(f"Worker {p.name} falló con ERROR FATAL. Relanzando...")
                    else:
                        logger.warning(f"Batch {prefix} {bid_str} falló (Code {p.exitcode}). Relanzando...")
                    
                    bid = int(bid_str)
                    new_p = multiprocessing.Process(
                        target=run_worker_with_stagger,
                        args=(task_func, f"{prefix}_{bid}", bid, chunks[bid-1]),
                        name=p.name
                    )
                    new_p.start()
                    active_processes[bid_str] = new_p
        time.sleep(5)
