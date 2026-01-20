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

# Asegurar que la tabla system_status existe (Migración segura)
def ensure_status_table():
    engine = get_engine()
    from sqlalchemy import inspect
    inspector = inspect(engine)
    if not inspector.has_table("system_status"):
        SystemStatus.__table__.create(bind=engine)

@router.get("/ingest")
async def ingest_page(request: Request, db: Session = Depends(get_db)):
    ensure_status_table()
    status = db.query(SystemStatus).filter_by(task_name="incremental_ingestion").first()

    return templates.TemplateResponse("admin/ingest.html", {
        "request": request,
        "active_page": "admin",
        "status": status
    })

def run_ingestion_task():
    """Ejecuta la ingesta incremental en segundo plano."""
    api_client = NBAApiClient()
    # Usamos get_session directamente aquí ya que estamos fuera del ciclo de vida de FastAPI request
    reporter = ProgressReporter("incremental_ingestion", session_factory=get_session)
    
    try:
        ingestor = IncrementalIngestion(api_client)
        # Sin limites, la ingesta incremental se detiene al encontrar partidos ya procesados
        ingestor.run(reporter=reporter)
    except Exception as e:
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
    ensure_status_table()
    status = db.query(SystemStatus).filter_by(task_name="incremental_ingestion").first()
    if not status:
        return {"status": "idle", "progress": 0, "message": "No hay tareas registradas."}
    
    return {
        "status": status.status,
        "progress": status.progress,
        "message": status.message,
        "updated_at": status.updated_at.isoformat() if status.updated_at else None
    }

# Ejecutar al importar para asegurar que la tabla existe al iniciar la app
try:
    ensure_status_table()
except:
    pass
