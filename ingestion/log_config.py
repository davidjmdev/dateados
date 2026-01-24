"""Configuración de niveles de log y comportamiento de logging."""
import os

# Determinar si estamos en la nube
IS_CLOUD = os.getenv("RENDER") == "true" or os.getenv("CLOUD_MODE") == "true"

# Nivel de log por defecto según entorno
DEFAULT_LOG_LEVEL = "WARNING" if IS_CLOUD else "INFO"

# Niveles de log: Se respeta la variable de entorno si existe, sino se usa el default por entorno
LOG_LEVEL = os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
WORKER_LOG_LEVEL = os.getenv("WORKER_LOG_LEVEL", LOG_LEVEL).upper()

# Intervalos de actualización de progreso
PROGRESS_UPDATE_EVERY_N_ITEMS = int(os.getenv("INGEST_PROGRESS_UPDATE_ITEMS", 1))
PROGRESS_LOG_EVERY_N_SECONDS = int(os.getenv("INGEST_PROGRESS_LOG_INTERVAL", 5))

# Configuración de limpieza de logs
CLEAR_LOGS_ON_INGESTION_START = os.getenv("INGEST_CLEAR_LOGS", "true").lower() == "true"
