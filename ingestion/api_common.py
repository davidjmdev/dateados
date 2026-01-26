import logging
import time
from typing import Optional, Any

from ingestion.config import MAX_RETRIES, API_TIMEOUT, API_DELAY

logger = logging.getLogger(__name__)

class FatalIngestionError(Exception):
    """Excepción para errores fatales que requieren reinicio del proceso."""
    pass

def fetch_with_retry(api_call_func, max_retries=MAX_RETRIES, timeout=API_TIMEOUT, error_context="", fatal=True):
    """Ejecuta una llamada API con reintentos simplificados."""
    for attempt in range(max_retries):
        try:
            result = api_call_func()
            time.sleep(API_DELAY)
            return result
        except Exception as e:
            error_msg = str(e)
            
            # Si el error indica que no hay datos (resultSet vacío), no reintentamos.
            if 'resultSet' in error_msg:
                logger.warning(f"Datos no disponibles en {error_context}: {error_msg}")
                return None

            logger.warning(
                f"Error en {error_context} (intento {attempt + 1}/{max_retries}): {error_msg}. "
                f"Esperando {timeout}s para reintentar..."
            )
            time.sleep(timeout)
            if attempt == max_retries - 1:
                logger.error(f"Fallo persistente en {error_context} tras {max_retries} intentos.")
                if fatal:
                    raise FatalIngestionError(f"Agotados reintentos en {error_context}: {error_msg}")
                return None
    return None
