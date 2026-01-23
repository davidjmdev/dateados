"""Configuración de niveles de log y comportamiento de logging."""
import os

# Niveles de log según el entorno
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# En modo cloud/producción, reducir verbosidad para no saturar la base de datos
if os.getenv("RENDER") == "true" or os.getenv("CLOUD_MODE") == "true":
    LOG_LEVEL = "WARNING"  # Solo warnings y errores
    WORKER_LOG_LEVEL = "WARNING"  # Workers también warnings y errores
else:
    # En local, más detalle para debugging
    LOG_LEVEL = "INFO"
    WORKER_LOG_LEVEL = "INFO"

# Intervalos de actualización de progreso
PROGRESS_UPDATE_EVERY_N_ITEMS = 1  # Actualizar SystemStatus después de cada item
PROGRESS_LOG_EVERY_N_SECONDS = 5    # Loguear en log_entries solo cada 5 segundos

# Configuración de limpieza de logs
CLEAR_LOGS_ON_INGESTION_START = True  # Limpiar logs al inicio de cada ingesta
