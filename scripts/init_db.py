#!/usr/bin/env python3
"""Script para inicializar la base de datos en producción.

Este script ejecuta la inicialización del esquema de la base de datos.
Útil para ejecutar después del despliegue en Render.com o cualquier plataforma cloud.
"""

import sys
from pathlib import Path

# Agregar el directorio raíz del proyecto al path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from db import init_db
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        logger.info("Inicializando esquema de base de datos...")
        init_db()
        logger.info("✅ Base de datos inicializada correctamente")
    except Exception as e:
        logger.error(f"❌ Error al inicializar base de datos: {e}", exc_info=True)
        sys.exit(1)
