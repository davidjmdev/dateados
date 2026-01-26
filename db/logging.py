"""Sistema centralizado de Logging para Dateados.

Este módulo unifica toda la lógica de registro, persistencia y configuración de logs
para todos los componentes del sistema (CLI, Workers, Web).
"""
import os
import sys
import logging
import traceback
from datetime import datetime, timezone
from typing import Optional, Dict

from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import delete

from db.connection import get_engine, get_session
from db.models import LogEntry, SystemStatus

# ==============================================================================
# 1. CONSTANTES Y CONFIGURACIÓN
# ==============================================================================

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Nivel por defecto: INFO para todos
DEFAULT_LOG_LEVEL = "INFO"
LOG_LEVEL = os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()

# Los workers son más silenciosos en consola para no saturar
WORKER_LOG_LEVEL_CONSOLE = "WARNING"
WORKER_LOG_LEVEL_DB = "INFO"

# Configuración de Progreso (Usado por ProgressReporter)
PROGRESS_UPDATE_EVERY_N_ITEMS = int(os.getenv("INGEST_PROGRESS_UPDATE_ITEMS", 1))
PROGRESS_LOG_EVERY_N_SECONDS = int(os.getenv("INGEST_PROGRESS_LOG_INTERVAL", 5))

# Configuración de Limpieza
CLEAR_LOGS_ON_INGESTION_START = os.getenv("INGEST_CLEAR_LOGS", "true").lower() == "true"


# ==============================================================================
# 2. HANDLERS PERSONALIZADOS
# ==============================================================================

class SQLAlchemyHandler(logging.Handler):
    """Handler que guarda registros en la tabla log_entries de la base de datos."""
    
    def __init__(self, level=logging.NOTSET):
        super().__init__(level=level)
        self._session_factory = None

    @property
    def session_factory(self):
        if self._session_factory is None:
            engine = get_engine()
            self._session_factory = sessionmaker(bind=engine)
        return self._session_factory

    def emit(self, record):
        if record.name.startswith('sqlalchemy') or record.name.startswith('psycopg2'):
            return

        session = self.session_factory()
        try:
            tb = None
            if record.exc_info:
                tb = "".join(traceback.format_exception(*record.exc_info))
            
            log_entry = LogEntry(
                timestamp=datetime.fromtimestamp(record.created, tz=timezone.utc),
                level=record.levelname,
                module=record.name,
                message=record.getMessage(),
                traceback=tb
            )
            session.add(log_entry)
            session.commit()
        except Exception:
            session.rollback()
        finally:
            session.close()


# ==============================================================================
# 3. SETUP UNIFICADO
# ==============================================================================

def setup_logging(context: str = "cli", verbose: bool = False):
    """Configura el sistema de logging global."""
    level = logging.DEBUG if verbose else getattr(logging, LOG_LEVEL, logging.INFO)
    
    # 1. Configurar Handlers
    db_handler = SQLAlchemyHandler()
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
    
    # Ajustes para workers
    if context == "worker":
        # En la base de datos queremos INFO para no perder detalle
        db_handler.setLevel(logging.INFO)
        # En consola solo WARNING para evitar caos visual
        console_handler.setLevel(logging.WARNING)
    else:
        db_handler.setLevel(level)
        console_handler.setLevel(level)

    # 2. Aplicar configuración raíz
    root_logger = logging.getLogger()
    # Limpiar handlers previos
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    root_logger.setLevel(logging.DEBUG) # El root acepta todo, los handlers filtran
    root_logger.addHandler(db_handler)
    root_logger.addHandler(console_handler)
    
    # 3. Ajustes finos de librerías ruidosas
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('nba_api').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)


# ==============================================================================
# 4. UTILIDADES DE ALTO NIVEL
# ==============================================================================

def log_header(message: str, logger_name: str = "dateados.system"):
    """Imprime un banner decorativo uniforme."""
    logger = logging.getLogger(logger_name)
    logger.info("=" * 80)
    logger.info(message.upper())
    logger.info("=" * 80)

def log_success(message: str, logger_name: str = "dateados.system"):
    """Imprime un mensaje de éxito con icono."""
    logger = logging.getLogger(logger_name)
    logger.info(f"✅ {message}")

def log_step(message: str, logger_name: str = "dateados.system"):
    """Imprime un paso del proceso."""
    logger = logging.getLogger(logger_name)
    logger.info(f"➜ {message}...")


# ==============================================================================
# 5. HERRAMIENTAS DE MANTENIMIENTO
# ==============================================================================

def clear_all_logs(session: Session) -> int:
    """Elimina todos los logs de la base de datos."""
    try:
        deleted = session.query(LogEntry).delete()
        session.commit()
        return deleted
    except Exception:
        session.rollback()
        return 0

def clear_system_status(session: Session) -> int:
    """Elimina todos los estados del sistema."""
    try:
        deleted = session.query(SystemStatus).delete()
        session.commit()
        return deleted
    except Exception:
        session.rollback()
        return 0

def cleanup_for_new_ingestion(session: Session, clear_status: bool = True) -> Dict[str, int]:
    """Limpia logs y estados para iniciar una nueva ejecución limpia."""
    stats = {'logs_deleted': 0, 'status_deleted': 0}
    stats['logs_deleted'] = clear_all_logs(session)
    if clear_status:
        stats['status_deleted'] = clear_system_status(session)
    return stats
