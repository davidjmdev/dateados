import logging
import multiprocessing
import os
import random
import time
import sys
from typing import List, Callable, Any, Dict, Tuple

from db.logging import setup_logging
from ingestion.config import (
    WORKER_STAGGER_MIN, WORKER_STAGGER_MAX
)
from ingestion.api_common import FatalIngestionError

logger = logging.getLogger(__name__)

def setup_worker_logging(worker_name: str):
    """Configura el logging para un worker en la base de datos."""
    # Usar el setup centralizado
    setup_logging(context="worker")

def run_worker_with_stagger(worker_func: Callable, name: str, *args, **kwargs):
    """Ejecuta una función de worker con un retraso inicial aleatorio."""
    import signal
    # Ignorar Ctrl+C en los procesos hijos, el padre se encargará de matarlos
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    setup_worker_logging(name)
    worker_logger = logging.getLogger(__name__)
    
    # Jitter inicial desde configuración
    delay = random.uniform(WORKER_STAGGER_MIN, WORKER_STAGGER_MAX)
    # CAMBIO: Usar DEBUG para el delay (reduce spam en logs)
    worker_logger.debug(f"Worker {name} delay: {delay:.2f}s")
    time.sleep(delay)
    
    # Log de inicio conciso
    worker_logger.info(f"✓ Worker {name} activo")
    
    try:
        worker_func(*args, **kwargs)
        # Log de finalización exitosa
        worker_logger.info(f"✓ Worker {name} completado")
        sys.exit(0)
    except FatalIngestionError as e:
        worker_logger.error(f"ERROR FATAL en {name}: {e}")
        sys.exit(42) # Código especial para FatalIngestionError
    except Exception as e:
        worker_logger.error(f"Error en {name}: {e}", exc_info=True)
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
        name = worker_name_func(1)
        
        # Registrar worker en system_status
        try:
            from ingestion.utils import ProgressReporter
            from db.connection import get_session
            reporter = ProgressReporter(name, session_factory=get_session)
            reporter.update(0, "Iniciando tarea secuencial...", status="running")
        except Exception as e:
            logger.debug(f"No se pudo registrar estado de tarea secuencial: {e}")
            reporter = None

        try:
            task_func(1, items, task_name=name, checkpoint_prefix=prefix)
            if reporter:
                reporter.complete("Tarea secuencial finalizada")
            return
        except Exception as e:
            logger.error(f"Error en tarea secuencial {prefix}: {e}")
            if reporter:
                reporter.fail(str(e))
            raise

    chunk_size = max(1, len(items) // num_workers)
    chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
    
    processes = {}
    for i, chunk in enumerate(chunks):
        batch_id = i + 1
        name = worker_name_func(batch_id)
        
        # Registrar worker en system_status si es posible
        try:
            from ingestion.utils import ProgressReporter
            from db.connection import get_session
            reporter = ProgressReporter(name, session_factory=get_session)
            reporter.update(0, "Inicializando...", status="running")
        except:
            pass

        p = multiprocessing.Process(
            target=run_worker_with_stagger,
            args=(task_func, f"{prefix}_{batch_id}", batch_id, chunk), kwargs={"task_name": name, "checkpoint_prefix": prefix},
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
                        args=(task_func, f"{prefix}_{bid}", bid, chunks[bid-1]), kwargs={"task_name": p.name, "checkpoint_prefix": prefix},
                        name=p.name
                    )
                    new_p.start()
                    active_processes[bid_str] = new_p
        time.sleep(5)
