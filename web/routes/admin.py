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
import asyncio
import httpx

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

async def keep_alive_during_task(task_name: str, max_hours: int = 0):
    """Evita el spin-down de Render haciendo ping local cada 5 min mientras la tarea corre.
    
    Args:
        task_name: Nombre de la tarea en la tabla system_status
        max_hours: Si > 0, tiempo máximo de ejecución. Si es 0 (default), corre indefinidamente
                  mientras la tarea esté activa en la base de datos.
    """
    # Solo actuar si estamos en la nube (Render o similar)
    if os.getenv("RENDER") != "true" and os.getenv("CLOUD_MODE") != "true":
        return

    logger = logging.getLogger("web.admin.keepalive")
    port = os.getenv("PORT", "8000")
    start_time = time.time()
    
    logger.info(f"🔄 Anti-spin-down ACTIVO para: {task_name}")
    
    while True:
        # Esperar 5 minutos entre pings
        await asyncio.sleep(300)
        
        # Timeout de seguridad (opcional, solo si max_hours > 0)
        if max_hours > 0 and (time.time() - start_time) > (max_hours * 3600):
            logger.warning(f"⏱️ Keep-alive timeout tras {max_hours}h. Deteniendo.")
            break
            
        # Verificar estado en BD
        session = get_session()
        try:
            status = session.query(SystemStatus).filter_by(task_name=task_name).first()
            
            # Si la tarea ya no está "running" o "pending", terminamos el keep-alive
            if not status or status.status not in ["running", "pending"]:
                logger.info(f"✅ Tarea '{task_name}' finalizada (status={status.status if status else 'None'}). Deteniendo keep-alive.")
                break
                
            # Ping local al endpoint de status para mantener despierto el servicio
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    await client.get(f"http://localhost:{port}/admin/ingest/status")
                logger.debug(f"💓 Keep-alive ping enviado (tarea: {task_name})")
            except Exception as e:
                logger.debug(f"⚠️ Fallo en ping keep-alive: {e}")
                
        except Exception as e:
            logger.error(f"❌ Error en bucle keep-alive: {e}")
        finally:
            session.close()

    logger.info(f"🛑 Anti-spin-down FINALIZADO para: {task_name}")

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
    # Activar keep-alive condicional
    background_tasks.add_task(keep_alive_during_task, "smart_ingestion")
    
    return {"status": "success", "message": "Ingesta inteligente iniciada en segundo plano con protección anti-spin-down."}

def get_auth_token():
    """Obtiene el token de seguridad desde las variables de entorno."""
    return os.getenv("SECURE_TOKEN") or os.getenv("CRON_API_KEY")

@router.post("/ingest/cron")
async def cron_ingestion(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    x_secure_token: Optional[str] = Header(None, alias="X-Secure-Token"),
    x_cron_key: Optional[str] = Header(None, alias="X-Cron-Key")
):
    """Endpoint para disparar la ingesta desde un cron externo (GitHub Actions)."""
    secure_token = get_auth_token()
    
    if not secure_token:
        raise HTTPException(status_code=500, detail="SECURE_TOKEN no configurada en el servidor")
        
    provided_token = x_secure_token or x_cron_key
    if provided_token != secure_token:
        raise HTTPException(status_code=403, detail="Token de seguridad inválido")

    # Verificar si ya está corriendo
    status = db.query(SystemStatus).filter_by(task_name="smart_ingestion").first()
    if status and status.status == "running":
        return {"status": "ignored", "message": "Ya hay una ingesta en curso."}

    # Asegurar que tenemos un registro de estado limpio
    if not status:
        status = SystemStatus(task_name="smart_ingestion")
        db.add(status)
    
    status.status = "running"
    status.progress = 0
    status.message = "Iniciando ingesta automática (Cron)..."
    status.last_run = datetime.now()
    db.commit()
    
    # Lanzamos la ingesta inteligente normal
    background_tasks.add_task(run_ingestion_task)
    # Activar keep-alive condicional
    background_tasks.add_task(keep_alive_during_task, "smart_ingestion")
    
    return {"status": "success", "message": "Ingesta automática iniciada con protección anti-spin-down."}

@router.post("/update/awards")
async def update_awards(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    x_secure_token: Optional[str] = Header(None, alias="X-Secure-Token"),
    x_cron_key: Optional[str] = Header(None, alias="X-Cron-Key")
):
    """Endpoint para forzar la actualización de premios."""
    secure_token = get_auth_token()
    
    if not secure_token:
        raise HTTPException(status_code=500, detail="SECURE_TOKEN no configurada en el servidor")
        
    provided_token = x_secure_token or x_cron_key
    if provided_token != secure_token:
        raise HTTPException(status_code=403, detail="Token de seguridad inválido")

    task_name = "awards_sync"
    background_tasks.add_task(run_awards_update_task)
    # Activar keep-alive condicional
    background_tasks.add_task(keep_alive_during_task, task_name)
    
    return {
        "status": "success", 
        "message": "Actualización forzada de premios iniciada en segundo plano con protección anti-spin-down."
    }

@router.post("/update/outliers")
async def update_outliers(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    x_secure_token: Optional[str] = Header(None, alias="X-Secure-Token"),
    x_cron_key: Optional[str] = Header(None, alias="X-Cron-Key")
):
    """Endpoint para forzar la actualización de outliers."""
    secure_token = get_auth_token()
    
    if not secure_token:
        raise HTTPException(status_code=500, detail="SECURE_TOKEN no configurada en el servidor")
        
    provided_token = x_secure_token or x_cron_key
    if provided_token != secure_token:
        raise HTTPException(status_code=403, detail="Token de seguridad inválido")

    task_name = "outliers_backfill"
    background_tasks.add_task(run_outliers_update_task)
    # Activar keep-alive condicional
    background_tasks.add_task(keep_alive_during_task, task_name)
    
    return {
        "status": "success", 
        "message": "Actualización forzada de outliers iniciada en segundo plano con protección anti-spin-down."
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
