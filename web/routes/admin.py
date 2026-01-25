from fastapi import APIRouter, Request, Depends, BackgroundTasks
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from pathlib import Path
from typing import Optional
from datetime import datetime

from db.connection import get_session, get_engine
from db.models import SystemStatus, Base
from ingestion.core import IncrementalIngestion
from ingestion.api_client import NBAApiClient
from ingestion.utils import ProgressReporter

router = APIRouter(prefix="/admin")

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

def get_db():
    db = get_session()
    try:
        yield db
    finally:
        db.close()

# Asegurar que la tabla system_status existe (Migración segura) - YA SE HACE EN APP.PY VIA INIT_DB

@router.get("/ingest")
async def ingest_page(request: Request, db: Session = Depends(get_db)):
    status = db.query(SystemStatus).filter_by(task_name="incremental_ingestion").first()

    return templates.TemplateResponse("admin/ingest.html", {
        "request": request,
        "active_page": "admin",
        "status": status
    })

import subprocess
import sys

def run_ingestion_task():
    """Ejecuta la ingesta incremental llamando al CLI como subproceso.
    
    Esto permite reutilizar la lógica de reinicio automático ante errores fatales
    que ya tiene el CLI, sin riesgo de tirar abajo el servidor web.
    El progreso se sigue viendo en la web porque el CLI actualiza SystemStatus.
    """
    python_exec = sys.executable
    cmd = [python_exec, "-m", "ingestion.cli", "--mode", "incremental"]
    
    try:
        # Ejecutar el CLI y esperar a que termine (incluyendo sus propios reinicios internos)
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            # Si el proceso termina con error a pesar de sus reintentos
            reporter = ProgressReporter("incremental_ingestion", session_factory=get_session)
            reporter.fail(f"El CLI terminó con código {process.returncode}. Error: {stderr}")
            
    except Exception as e:
        reporter = ProgressReporter("incremental_ingestion", session_factory=get_session)
        reporter.fail(str(e))

@router.post("/ingest/run")
async def start_ingestion(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    # Verificar si ya está corriendo
    status = db.query(SystemStatus).filter_by(task_name="incremental_ingestion").first()
    if status and status.status == "running":
        return {"status": "error", "message": "Ya hay una ingesta en curso."}
    
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
