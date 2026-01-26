"""Sistema centralizado de Logging para Dateados.

Este módulo unifica toda la lógica de registro, persistencia y configuración de logs
para todos los componentes del sistema (CLI, Workers, Web).

Funcionalidades:
- Configuración automática según entorno (Local vs Cloud).
- Persistencia en base de datos (PostgreSQL) vía SQLAlchemy.
- Gestión de niveles de log y formatos unificados.
- Herramientas de limpieza y mantenimiento de logs.
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

# Determinar entorno
IS_CLOUD = os.getenv("RENDER") == "true" or os.getenv("CLOUD_MODE") == "true" or os.getenv("WEB_MODE") == "true"

# Formatos
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Niveles de Log
DEFAULT_LOG_LEVEL = "WARNING" if IS_CLOUD else "INFO"
LOG_LEVEL = os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
WORKER_LOG_LEVEL = os.getenv("WORKER_LOG_LEVEL", LOG_LEVEL).upper()

# Silenciar consola (útil para subprocesos controlados por la web)
SILENT_STDOUT = os.getenv("INGEST_SILENT_STDOUT", "false").lower() == "true"

# Configuración de Progreso (Usado por ProgressReporter)
PROGRESS_UPDATE_EVERY_N_ITEMS = int(os.getenv("INGEST_PROGRESS_UPDATE_ITEMS", 1))
PROGRESS_LOG_EVERY_N_SECONDS = int(os.getenv("INGEST_PROGRESS_LOG_INTERVAL", 5))

# Configuración de Limpieza
CLEAR_LOGS_ON_INGESTION_START = os.getenv("INGEST_CLEAR_LOGS", "true").lower() == "true"


# ==============================================================================
# 2. HANDLERS PERSONALIZADOS
# ==============================================================================

class SQLAlchemyHandler(logging.Handler):
    """Handler que guarda registros en la tabla log_entries de la base de datos.
    
    Diseñado para ser resiliente: si falla la DB, no detiene la aplicación.
    """
    
    def __init__(self):
        super().__init__()
        # Inicialización lazy del engine para evitar problemas de importación circular
        self._session_factory = None

    @property
    def session_factory(self):
        if self._session_factory is None:
            engine = get_engine()
            self._session_factory = sessionmaker(bind=engine)
        return self._session_factory

    def emit(self, record):
        # Evitar bucles infinitos si SQLAlchemy o psycopg2 generan logs
        if record.name.startswith('sqlalchemy') or record.name.startswith('psycopg2'):
            return

        session = self.session_factory()
        try:
            # Capturar traceback completo si existe una excepción
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
            # Fallback silencioso: si no podemos escribir en la DB, no podemos hacer mucho más
            # sin arriesgar un crash de la app principal.
            session.rollback()
        finally:
            session.close()


# ==============================================================================
# 3. SETUP UNIFICADO
# ==============================================================================

def setup_logging(context: str = "cli", verbose: bool = False):
    """Configura el sistema de logging global según el contexto.
    
    Args:
        context: Tipo de proceso ('cli', 'worker', 'web').
        verbose: Si True, fuerza nivel DEBUG.
    """
    # 1. Determinar nivel base
    level = logging.DEBUG if verbose else getattr(logging, LOG_LEVEL, logging.INFO)
    
    # En workers, podemos querer ser más silenciosos
    if context == "worker":
        level = getattr(logging, WORKER_LOG_LEVEL, logging.INFO)

    # 2. Configurar Handlers
    handlers = []
    
    # Handler de Base de Datos (Siempre activo para trazabilidad)
    handlers.append(SQLAlchemyHandler())
    
    # Handler de Consola
    # 1. Saltamos si SILENT_STDOUT es True
    # 2. Evitamos en workers si estamos en nube para reducir ruido
    # 3. En CLI y Web siempre queremos ver output en stdout (logs de Render/Terminal)
    if not SILENT_STDOUT:
        if context != "worker" or not IS_CLOUD:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
            handlers.append(console_handler)

    # 3. Aplicar configuración raíz
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
        handlers=handlers,
        force=True
    )
    
    # 4. Ajustes finos de librerías ruidosas
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('nba_api').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    
    # Silenciar módulos internos específicos en la consola pero no en la BD si es posible
    # Nota: logging.basicConfig afecta a todos. Para diferenciar consola/BD necesitamos handlers específicos.
    
    if context == "worker":
        logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)


# ==============================================================================
# 4. UTILIDADES DE ALTO NIVEL (Para evitar duplicidad)
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


def setup_logging(context: str = "cli", verbose: bool = False):
    """Configura el sistema de logging global según el contexto.
    
    Args:
        context: Tipo de proceso ('cli', 'worker', 'web').
        verbose: Si True, fuerza nivel DEBUG.
    """
    # 1. Determinar nivel base
    level = logging.DEBUG if verbose else getattr(logging, LOG_LEVEL, logging.INFO)
    
    # En workers, podemos querer ser más silenciosos
    if context == "worker":
        level = getattr(logging, WORKER_LOG_LEVEL, logging.INFO)

    # 2. Configurar Handlers
    handlers = []
    
    # Handler de Base de Datos (Siempre activo para trazabilidad)
    handlers.append(SQLAlchemyHandler())
    
    # Handler de Consola
    # 1. Saltamos si SILENT_STDOUT es True
    # 2. Evitamos en workers si estamos en nube para reducir ruido
    # 3. En CLI y Web siempre queremos ver output en stdout (logs de Render/Terminal)
    if not SILENT_STDOUT:
        if context != "worker" or not IS_CLOUD:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
            handlers.append(console_handler)

    # 3. Aplicar configuración raíz
    # Limpiamos handlers previos para evitar duplicación al reiniciar o recargar
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
        handlers=handlers,
        force=True # Python 3.8+
    )
    
    # 4. Ajustes finos de librerías ruidosas
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('nba_api').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    
    # Si estamos en worker, silenciar aún más
    if context == "worker":
        logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)


# ==============================================================================
# 4. HERRAMIENTAS DE MANTENIMIENTO
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
    """Limpia logs y estados para iniciar una nueva ejecución limpia.
    
    Args:
        session: Sesión activa de SQLAlchemy.
        clear_status: Si True, también resetea la tabla system_status.
        
    Returns:
        Dict con contadores de eliminados.
    """
    stats = {'logs_deleted': 0, 'status_deleted': 0}
    
    stats['logs_deleted'] = clear_all_logs(session)
    
    if clear_status:
        stats['status_deleted'] = clear_system_status(session)
        
    return stats
