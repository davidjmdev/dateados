"""Utilidades para limpieza y mantenimiento de logs."""
import logging
from sqlalchemy.orm import Session
from db.models import LogEntry, SystemStatus

logger = logging.getLogger(__name__)


def clear_all_logs(session: Session):
    """Elimina todos los logs de la base de datos.
    
    Se ejecuta al inicio de cada ingesta para tener una vista limpia.
    
    Args:
        session: Sesión de SQLAlchemy
        
    Returns:
        Número de logs eliminados
    """
    try:
        deleted = session.query(LogEntry).delete(synchronize_session=False)
        session.commit()
        logger.info(f"🧹 Logs limpiados: {deleted} entradas eliminadas")
        return deleted
    except Exception as e:
        logger.error(f"Error limpiando logs: {e}")
        session.rollback()
        return 0


def clear_system_status(session: Session):
    """Elimina todos los estados del sistema (tasks completadas, etc).
    
    Se ejecuta al inicio de cada ingesta para resetear el monitor.
    
    Args:
        session: Sesión de SQLAlchemy
        
    Returns:
        Número de estados eliminados
    """
    try:
        deleted = session.query(SystemStatus).delete(synchronize_session=False)
        session.commit()
        logger.info(f"🧹 Estados del sistema limpiados: {deleted} entradas eliminadas")
        return deleted
    except Exception as e:
        logger.error(f"Error limpiando estados del sistema: {e}")
        session.rollback()
        return 0


def cleanup_for_new_ingestion(session: Session, clear_status: bool = True):
    """Limpia logs y opcionalmente estados del sistema para una nueva ingesta.
    
    Args:
        session: Sesión de SQLAlchemy
        clear_status: Si True, también limpia SystemStatus
        
    Returns:
        Diccionario con estadísticas de limpieza
    """
    stats = {
        'logs_deleted': 0,
        'status_deleted': 0
    }
    
    # Siempre limpiar logs
    stats['logs_deleted'] = clear_all_logs(session)
    
    # Opcionalmente limpiar estados
    if clear_status:
        stats['status_deleted'] = clear_system_status(session)
    
    return stats
