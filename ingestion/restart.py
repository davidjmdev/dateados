"""Sistema de reinicio automático para errores fatales.

Este módulo maneja el reinicio automático del proceso de ingestion
cuando se detectan errores fatales de API (rate limiting, timeouts, etc.).
"""

import os
import sys
import logging

logger = logging.getLogger(__name__)


def restart_process():
    """Reinicia el proceso actual ejecutando el mismo comando inmediatamente.
    
    Preserva:
    - Todos los argumentos de línea de comandos originales
    - Variables de entorno
    - El checkpoint guardado (se cargará automáticamente al reiniciar)
    
    Este método se invoca cuando se captura FatalIngestionError,
    permitiendo que la ingesta continúe tras errores de rate limiting.
    
    El proceso se reemplaza usando os.execv en lugar de subprocess
    para mantener el PID padre y evitar crear procesos huérfanos.
    """
    python_exec = sys.executable
    args = sys.argv[1:]
    
    # Asegurar que --resume está presente para cargar checkpoint
    if '--resume' not in args:
        args.append('--resume')
    
    cmd = [python_exec, '-m', 'ingestion.cli'] + args
    
    logger.warning("=" * 80)
    logger.warning("🔄 REINICIANDO PROCESO AUTOMÁTICAMENTE")
    logger.warning("=" * 80)
    logger.warning(f"Comando: {' '.join(cmd)}")
    logger.warning("📌 El checkpoint guardado se cargará automáticamente")
    logger.warning("⏳ Reiniciando en 3 segundos...")
    logger.warning("=" * 80)
    
    import time
    time.sleep(3)
    
    try:
        # os.execv reemplaza el proceso actual sin crear uno nuevo
        # Esto es preferible a subprocess porque mantiene el PID padre
        os.execv(python_exec, cmd)
    except Exception as e:
        logger.error(f"❌ Error al reiniciar proceso: {e}")
        logger.error("El proceso se detendrá. Ejecuta manualmente con --resume para continuar.")
        sys.exit(1)
