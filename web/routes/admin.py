from fastapi import APIRouter, Request, Depends, BackgroundTasks, Header, HTTPException
from web.templates import templates
from sqlalchemy.orm import Session
from pathlib import Path
from typing import Optional, List
import os
from datetime import datetime
import logging
import subprocess
import sys
import signal
import time

from db.connection import get_session, get_engine
from db.models import SystemStatus, Base, LogEntry
from ingestion.utils import ProgressReporter
from db.logging import cleanup_for_new_ingestion
from ingestion.checkpoints import CheckpointManager

router = APIRouter(prefix="/admin")

# Lista global para rastrear procesos de ingesta activos
active_processes = []

def get_db():
    db = get_session()
    try:
        yield db
    finally:
        db.close()

@router.get("/ingest/logs")
async def get_ingestion_logs(limit: int = 50, db: Session = Depends(get_db)):
    """Retorna los últimos logs de la base de datos."""
    logs = db.query(LogEntry).order_by(LogEntry.timestamp.desc()).limit(limit).all()
    return [
        {
            "id": log.id,
            "timestamp": log.timestamp.isoformat(),
            "level": log.level,
            "module": log.module,
            "message": log.message
        } for log in reversed(logs)
    ]

@router.get("/ingest")
async def ingest_page(request: Request, db: Session = Depends(get_db)):
    status = db.query(SystemStatus).filter_by(task_name="smart_ingestion").first()

    return templates.TemplateResponse("admin/ingest.html", {
        "request": request,
        "active_page": "admin",
        "status": status
    })

def stop_all_ingestions():
    """Detiene todos los procesos de ingesta a nivel de sistema operativo."""
    global active_processes
    
    logger = logging.getLogger("web.admin")
    logger.info("Iniciando parada forzosa de procesos de ingesta...")
    
    # 1. Intentar parada limpia de los procesos rastreados en esta instancia
    for process in active_processes:
        try:
            process.send_signal(signal.SIGINT)
        except Exception:
            pass
    
    # 2. Limpieza agresiva a nivel de Sistema Operativo (Crucial para Render/Multi-worker)
    # Buscamos cualquier proceso que esté ejecutando el módulo de ingesta
    try:
        # Enviamos SIGINT (señal 2) a todos los procesos que coincidan con el patrón
        subprocess.run(["pkill", "-2", "-f", "ingestion.cli"], capture_output=True)
        time.sleep(2)
        # Forzamos con SIGKILL (señal 9) para asegurar que no queden procesos colgados
        subprocess.run(["pkill", "-9", "-f", "ingestion.cli"], capture_output=True)
        logger.info("Comandos pkill ejecutados con éxito.")
    except Exception as e:
        logger.error(f"Error ejecutando pkill: {e}")
    
    active_processes = []

def run_ingestion_task(extra_args: Optional[List[str]] = None):
    """Ejecuta la ingesta inteligente llamando al CLI como subproceso.
    
    El CLI ahora usa una lógica centralizada que siempre escribe en la DB
    y en STDOUT, asegurando visibilidad tanto en la web como en Render.
    """
    global active_processes
    python_exec = sys.executable
    import os
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    
    cmd = [python_exec, "-m", "ingestion.cli"]
    if extra_args:
        cmd.extend(extra_args)
    
    logger = logging.getLogger("web.admin")
    process = None
    try:
        # Ejecutar el CLI. Su salida STDOUT irá directamente a los logs del servidor (Render).
        process = subprocess.Popen(
            cmd,
            env=env
        )
        
        active_processes.append(process)
        
        # Esperar a que el proceso termine
        process.wait()
        
        if process in active_processes:
            active_processes.remove(process)
        
        if process.returncode != 0 and process.returncode != -signal.SIGINT:
            reporter = ProgressReporter("smart_ingestion", session_factory=get_session)
            reporter.fail(f"El CLI terminó con código {process.returncode}")
            
    except Exception as e:
        if process and process in active_processes:
            active_processes.remove(process)
        logger.error(f"Error fatal en tarea de ingesta: {e}")
        reporter = ProgressReporter("smart_ingestion", session_factory=get_session)
        reporter.fail(str(e))

def run_awards_update_task():
    """Ejecuta la actualización de premios."""
    global active_processes
    python_exec = sys.executable
    import os
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    
    logger = logging.getLogger("web.admin")
    task_name = "awards_sync"
    
    cmd = [python_exec, "-m", "ingestion.cli", "--mode", "awards"]
    logger.info(f"Iniciando actualización de premios: {' '.join(cmd)}")
    
    reporter = ProgressReporter(task_name, session_factory=get_session)
    reporter.update(0, "Sincronizando premios...")
    
    process = None
    try:
        process = subprocess.Popen(cmd, env=env)
        active_processes.append(process)
        process.wait()
        
        if process in active_processes:
            active_processes.remove(process)
        
        if process.returncode == 0:
            logger.info("Actualización de premios completada con éxito.")
            reporter.complete("Premios actualizados correctamente")
        else:
            logger.error(f"Actualización de premios falló con código {process.returncode}")
            reporter.fail(f"Falló con código {process.returncode}")
            
    except Exception as e:
        if process and process in active_processes:
            active_processes.remove(process)
        logger.error(f"Error en actualización de premios: {e}")
        reporter.fail(str(e))

def run_outliers_update_task():
    """Ejecuta la actualización de outliers."""
    global active_processes
    python_exec = sys.executable
    import os
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    
    logger = logging.getLogger("web.admin")
    task_name = "outliers_backfill"
    
    cmd = [python_exec, "-m", "outliers.cli", "backfill"]
    logger.info(f"Iniciando actualización de outliers: {' '.join(cmd)}")
    
    reporter = ProgressReporter(task_name, session_factory=get_session)
    reporter.update(0, "Actualizando outliers...")
    
    process = None
    try:
        process = subprocess.Popen(cmd, env=env)
        active_processes.append(process)
        process.wait()
        
        if process in active_processes:
            active_processes.remove(process)
        
        if process.returncode == 0:
            logger.info("Actualización de outliers completada con éxito.")
            reporter.complete("Outliers actualizados correctamente")
        else:
            logger.error(f"Actualización de outliers falló con código {process.returncode}")
            reporter.fail(f"Falló con código {process.returncode}")
            
    except Exception as e:
        if process and process in active_processes:
            active_processes.remove(process)
        logger.error(f"Error en actualización de outliers: {e}")
        reporter.fail(str(e))

@router.post("/ingest/run")
async def start_ingestion(background_tasks: BackgroundTasks, clean: bool = False, db: Session = Depends(get_db)):
    # 1. Si es un reinicio, detener procesos anteriores PRIMERO (A nivel de sistema)
    if clean:
        stop_all_ingestions()
        # Dar un margen generoso para que el SO libere recursos y los hijos mueran
        time.sleep(3)

    # 2. Verificar si ya está corriendo (para el modo normal)
    # Si acabamos de hacer clean, confiamos en que ya no hay nada corriendo
    if not clean:
        status = db.query(SystemStatus).filter_by(task_name="smart_ingestion").first()
        if status and status.status == "running":
            return {"status": "error", "message": "Ya hay una ingesta en curso."}
    
    if clean:
        # 3. Limpiar logs y resetear estados en la base de datos
        cleanup_for_new_ingestion(db, clear_status=True)
        # 4. Borrar checkpoints
        CheckpointManager().clear()
        db.commit()
        
    # Asegurar que tenemos un registro de estado limpio
    status = db.query(SystemStatus).filter_by(task_name="smart_ingestion").first()
    if not status:
        status = SystemStatus(task_name="smart_ingestion")
        db.add(status)
    
    status.status = "running"
    status.progress = 0
    status.message = "Iniciando proceso inteligente..."
    status.last_run = datetime.now()
    db.commit()
    
    background_tasks.add_task(run_ingestion_task)
    return {"status": "success", "message": "Ingesta inteligente iniciada en segundo plano."}

def get_auth_token():
    """Obtiene el token de seguridad desde las variables de entorno."""
    return os.getenv("SECURE_TOKEN") or os.getenv("CRON_API_KEY")

@router.post("/ingest/cron")
async def cron_ingestion(
    db: Session = Depends(get_db),
    x_secure_token: Optional[str] = Header(None, alias="X-Secure-Token"),
    x_cron_key: Optional[str] = Header(None, alias="X-Cron-Key")
):
    """Endpoint para disparar la ingesta desde un cron externo (GitHub Actions).
    
    Este endpoint envía un comando al Background Worker en lugar de ejecutar
    la tarea directamente, evitando el problema de spin-down del Web Service.
    """
    secure_token = get_auth_token()
    
    if not secure_token:
        raise HTTPException(status_code=500, detail="SECURE_TOKEN no configurada en el servidor")
        
    provided_token = x_secure_token or x_cron_key
    if provided_token != secure_token:
        raise HTTPException(status_code=403, detail="Token de seguridad inválido")

    # Verificar si ya hay un comando pendiente o una ingesta corriendo
    existing_cmd = db.query(SystemStatus).filter_by(task_name="worker_command").first()
    if existing_cmd and existing_cmd.status == "pending":
        return {"status": "ignored", "message": "Ya hay un comando pendiente para el worker."}
    
    status = db.query(SystemStatus).filter_by(task_name="smart_ingestion").first()
    if status and status.status == "running":
        return {"status": "ignored", "message": "Ya hay una ingesta en curso."}

    # Enviar comando al worker
    cmd = db.query(SystemStatus).filter_by(task_name="worker_command").first()
    if not cmd:
        cmd = SystemStatus(task_name="worker_command")
        db.add(cmd)
    
    cmd.status = "pending"
    cmd.message = "RUN_INGESTION"
    cmd.progress = 0
    cmd.last_run = datetime.now()
    db.commit()
    
    logger = logging.getLogger("web.admin")
    logger.info("Comando RUN_INGESTION enviado al background worker")
    
    return {"status": "success", "message": "Comando enviado al background worker. La ingesta iniciará en breve."}

@router.post("/update/awards")
async def update_awards(
    db: Session = Depends(get_db),
    x_secure_token: Optional[str] = Header(None, alias="X-Secure-Token"),
    x_cron_key: Optional[str] = Header(None, alias="X-Cron-Key")
):
    """Endpoint para forzar la actualización de premios.
    
    Envía comando al Background Worker para ejecutar la sincronización.
    """
    secure_token = get_auth_token()
    
    if not secure_token:
        raise HTTPException(status_code=500, detail="SECURE_TOKEN no configurada en el servidor")
        
    provided_token = x_secure_token or x_cron_key
    if provided_token != secure_token:
        raise HTTPException(status_code=403, detail="Token de seguridad inválido")

    # Verificar si ya hay un comando pendiente
    existing_cmd = db.query(SystemStatus).filter_by(task_name="worker_command").first()
    if existing_cmd and existing_cmd.status == "pending":
        return {"status": "ignored", "message": "Ya hay un comando pendiente para el worker."}

    # Enviar comando al worker
    cmd = db.query(SystemStatus).filter_by(task_name="worker_command").first()
    if not cmd:
        cmd = SystemStatus(task_name="worker_command")
        db.add(cmd)
    
    cmd.status = "pending"
    cmd.message = "RUN_AWARDS_SYNC"
    cmd.progress = 0
    cmd.last_run = datetime.now()
    db.commit()
    
    logger = logging.getLogger("web.admin")
    logger.info("Comando RUN_AWARDS_SYNC enviado al background worker")
    
    return {
        "status": "success", 
        "message": "Comando de actualización de premios enviado al worker."
    }

@router.post("/update/outliers")
async def update_outliers(
    db: Session = Depends(get_db),
    x_secure_token: Optional[str] = Header(None, alias="X-Secure-Token"),
    x_cron_key: Optional[str] = Header(None, alias="X-Cron-Key")
):
    """Endpoint para forzar la actualización de outliers.
    
    Envía comando al Background Worker para ejecutar el backfill.
    """
    secure_token = get_auth_token()
    
    if not secure_token:
        raise HTTPException(status_code=500, detail="SECURE_TOKEN no configurada en el servidor")
        
    provided_token = x_secure_token or x_cron_key
    if provided_token != secure_token:
        raise HTTPException(status_code=403, detail="Token de seguridad inválido")

    # Verificar si ya hay un comando pendiente
    existing_cmd = db.query(SystemStatus).filter_by(task_name="worker_command").first()
    if existing_cmd and existing_cmd.status == "pending":
        return {"status": "ignored", "message": "Ya hay un comando pendiente para el worker."}

    # Enviar comando al worker
    cmd = db.query(SystemStatus).filter_by(task_name="worker_command").first()
    if not cmd:
        cmd = SystemStatus(task_name="worker_command")
        db.add(cmd)
    
    cmd.status = "pending"
    cmd.message = "RUN_OUTLIERS_BACKFILL"
    cmd.progress = 0
    cmd.last_run = datetime.now()
    db.commit()
    
    logger = logging.getLogger("web.admin")
    logger.info("Comando RUN_OUTLIERS_BACKFILL enviado al background worker")
    
    return {
        "status": "success", 
        "message": "Comando de actualización de outliers enviado al worker."
    }

@router.post("/ingest/reset")
async def reset_ingestion(
    db: Session = Depends(get_db),
    x_secure_token: Optional[str] = Header(None, alias="X-Secure-Token"),
    x_cron_key: Optional[str] = Header(None, alias="X-Cron-Key")
):
    """Endpoint para forzar la parada de ingestas y limpiar el estado."""
    secure_token = get_auth_token()
    
    if not secure_token:
        raise HTTPException(status_code=500, detail="SECURE_TOKEN no configurada en el servidor")
        
    provided_token = x_secure_token or x_cron_key
    if provided_token != secure_token:
        raise HTTPException(status_code=403, detail="Token de seguridad inválido")

    # 1. Parar procesos
    stop_all_ingestions()
    
    # 2. Limpiar base de datos (logs y estados)
    cleanup_for_new_ingestion(db, clear_status=True)
    
    # 3. Limpiar checkpoints
    CheckpointManager().clear()
    
    db.commit()
    
    return {"status": "success", "message": "Sistema de ingesta reseteado y procesos detenidos."}

@router.get("/ingest/status")
async def get_ingestion_status(db: Session = Depends(get_db)):
    status = db.query(SystemStatus).filter_by(task_name="smart_ingestion").first()
    if not status:
        return {"status": "idle", "progress": 0, "message": "No hay tareas registradas."}
    
    return {
        "status": status.status,
        "progress": status.progress,
        "message": status.message,
        "updated_at": status.updated_at.isoformat() if status.updated_at else None
    }
