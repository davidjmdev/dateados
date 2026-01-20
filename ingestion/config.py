"""Configuraciones para el módulo de ingesta.

Este módulo centraliza todas las configuraciones relacionadas con:
- Timeouts y reintentos de llamadas API
- Delays para rate limiting
- Formato de logs
- Constantes
"""

# Configuración de API
API_TIMEOUT = 30  # Tiempo de espera máximo para respuestas y espera tras fallo (segundos)
MAX_RETRIES = 2   # Reintentos automáticos ante cualquier error
API_DELAY = 0.2   # Pausa (segundos) entre llamadas exitosas consecutivas

# Formato de Registro (Logging)
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Constants
# IDs de equipos especiales que no siguen el rango estándar de franquicias (16106127...)
# 1610616833: All-Star East / Team LeBron, etc.
# 1610616834: All-Star West / Team Giannis, etc.
SPECIAL_EVENT_TEAM_IDS = {1610616833, 1610616834}

# Paralelización
AWARDS_WORKERS = 10  # Número de procesos simultáneos para sincronización de premios
WEB_AWARDS_WORKERS = 1 # Número de procesos para ingesta desde web (bajo consumo RAM)
WORKER_STAGGER_MIN = 1.0  # Retraso mínimo inicial para workers (segundos)
WORKER_STAGGER_MAX = 10.0 # Retraso máximo inicial para workers (segundos)

