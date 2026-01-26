"""Configuraciones para el módulo de ingesta.

Este módulo centraliza todas las configuraciones relacionadas con:
- Timeouts y reintentos de llamadas API
- Delays para rate limiting
- Formato de logs
- Constantes
"""
import os
from dotenv import load_dotenv

# Cargar variables de entorno del archivo .env
load_dotenv()

# Configuración de API
API_TIMEOUT = int(os.getenv("INGEST_API_TIMEOUT", 5))  # Tiempo de espera máximo para respuestas y espera tras fallo (segundos)
MAX_RETRIES = int(os.getenv("INGEST_MAX_RETRIES", 2))   # Reintentos automáticos ante cualquier error
API_DELAY = float(os.getenv("INGEST_API_DELAY", 0.2))   # Pausa (segundos) entre llamadas exitosas consecutivas

# Formato de Registro (Logging)
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Paralelización
MAX_WORKERS_LOCAL = int(os.getenv("INGEST_MAX_WORKERS_LOCAL", 1))      # Procesos simultáneos en entorno local
MAX_WORKERS_CLOUD = int(os.getenv("INGEST_MAX_WORKERS_CLOUD", 1))      # Procesos simultáneos en la nube (Render)
WORKER_STAGGER_MIN = float(os.getenv("INGEST_WORKER_STAGGER_MIN", 1.0))   # Retraso mínimo inicial para workers (segundos)
WORKER_STAGGER_MAX = float(os.getenv("INGEST_WORKER_STAGGER_MAX", 10.0))  # Retraso máximo inicial para workers (segundos)
