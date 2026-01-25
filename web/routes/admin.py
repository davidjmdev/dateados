from fastapi import APIRouter, Request, Depends, BackgroundTasks
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from pathlib import Path
from typing import Optional
from datetime import datetime
import logging
import subprocess
import sys
import signal
import time

from db.connection import get_session, get_engine
from db.models import SystemStatus, Base, LogEntry
from ingestion.utils import ProgressReporter
from db.utils.log_cleanup import cleanup_for_new_ingestion
from ingestion.checkpoints import CheckpointManager

router = APIRouter(prefix="/admin")

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

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
    status = db.query(SystemStatus).filter_by(task_name="incremental_ingestion").first()

    return templates.TemplateResponse("admin/ingest.html", {
        "request": request,
        "active_page": "admin",
        "status": status
    })

def stop_all_ingestions():
    """Detiene todos los procesos de ingesta que se hayan lanzado desde la web."""
    global active_processes
    if not active_processes:
        return
    
    # Informar en logs
    logger = logging.getLogger("web.admin")
    logger.info(f"Deteniendo {len(active_processes)} procesos de ingesta activos...")
    
    for process in active_processes:
        try:
            # Enviar SIGINT (equivalente a Ctrl+C)
            # El CLI tiene un manejador que matará a los hijos y limpiará la BD
            process.send_signal(signal.SIGINT)
            
            # Esperar hasta 5 segundos a que cierre de forma limpia
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # Si no cierra, forzar muerte
                logger.warning(f"Proceso {process.pid} no cerró a tiempo. Forzando kill...")
                process.kill()
        except Exception as e:
            logger.error(f"Error al detener proceso {getattr(process, 'pid', 'unknown')}: {e}")
    
    active_processes = []

def run_ingestion_task():
    """Ejecuta la ingesta incremental llamando al CLI como subproceso.
    
    Esto permite reutilizar la lógica de reinicio automático ante errores fatales
    que ya tiene el CLI, sin riesgo de tirar abajo el servidor web.
    El progreso se sigue viendo en la web porque el CLI actualiza SystemStatus.
    """
    global active_processes
    python_exec = sys.executable
    cmd = [python_exec, "-m", "ingestion.cli", "--mode", "incremental"]
    
    process = None
    try:
        # Ejecutar el CLI
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Registrar proceso
        active_processes.append(process)
        
        # Esperar a que termine (incluyendo sus propios reinicios internos)
        stdout, stderr = process.communicate()
        
        # Eliminar de la lista al terminar
        if process in active_processes:
            active_processes.remove(process)
        
        if process.returncode != 0 and process.returncode != -signal.SIGINT:
            # Si el proceso termina con error (y no fue por una interrupción manual)
            reporter = ProgressReporter("incremental_ingestion", session_factory=get_session)
            reporter.fail(f"El CLI terminó con código {process.returncode}. Error: {stderr}")
            
    except Exception as e:
        if process and process in active_processes:
            active_processes.remove(process)
        reporter = ProgressReporter("incremental_ingestion", session_factory=get_session)
        reporter.fail(str(e))

@router.post("/ingest/run")
async def start_ingestion(background_tasks: BackgroundTasks, clean: bool = False, db: Session = Depends(get_db)):
    # 1. Si es un reinicio, detener procesos anteriores PRIMERO
    if clean:
        stop_all_ingestions()
        # Dar un pequeño margen para que el SO libere recursos y el signal_handler limpie la BD
        time.sleep(1)

    # 2. Verificar si ya está corriendo (para el modo normal)
    status = db.query(SystemStatus).filter_by(task_name="incremental_ingestion").first()
    if not clean and status and status.status == "running":
        return {"status": "error", "message": "Ya hay una ingesta en curso."}
    
    if clean:
        # 3. Limpiar logs y status
        cleanup_for_new_ingestion(db, clear_status=True)
        # 4. Borrar checkpoints incrementales
        CheckpointManager().clear()
        db.commit()
        
        # Recargar status tras limpieza
        status = None
    
    # Reiniciar estado
    if not status:
        status = SystemStatus(task_name="incremental_ingestion")
        db.add(status)
    
    status.status = "running"
    status.progress = 0
    status.message = "Iniciando proceso..."
    status.last_run = datetime.now()
    db.commit()
    
    background_tasks.add_task(run_ingestion_task)
    return {"status": "success", "message": "Ingesta iniciada en segundo plano."}

@router.get("/ingest/status")
async def get_ingestion_status(db: Session = Depends(get_db)):
    status = db.query(SystemStatus).filter_by(task_name="incremental_ingestion").first()
    if not status:
        return {"status": "idle", "progress": 0, "message": "No hay tareas registradas."}
    
    return {
        "status": status.status,
        "progress": status.progress,
        "message": status.message,
        "updated_at": status.updated_at.isoformat() if status.updated_at else None
    }
