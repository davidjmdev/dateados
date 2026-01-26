"""CLI para ejecutar ingestas con reinicio automático.

Este módulo proporciona la interfaz de línea de comandos para ejecutar
ingestas de datos NBA utilizando una estrategia inteligente unificada.

Modos:
- smart: Ingesta inteligente (por defecto). Detecta huecos y carga incremental o masiva según sea necesario.
- full: Fuerza una ingesta completa histórica (útil para reparaciones).
"""

import argparse
import logging
import sys
import signal
from datetime import date
from pathlib import Path

# Configurar path del proyecto
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from db import init_db
from db.connection import get_session
from ingestion.api_client import NBAApiClient
from ingestion.checkpoints import CheckpointManager
from ingestion.strategies import SmartIngestion, FullIngestion
from ingestion.restart import restart_process
from ingestion.api_common import FatalIngestionError
from ingestion.utils import (
    normalize_season, 
    ProgressReporter
)
from db.logging import setup_logging, cleanup_for_new_ingestion, CLEAR_LOGS_ON_INGESTION_START, log_header, log_success
from db.models import SystemStatus
from sqlalchemy import delete

def signal_handler(sig, frame):
    """Manejador de Ctrl+C para una salida limpia y rápida."""
    # Solo actuar si somos el proceso principal
    import multiprocessing as mp
    if mp.current_process().name != 'MainProcess':
        return

    log_header("INTERRUPCIÓN DETECTADA (Ctrl+C)", "dateados.cli")
    
    # 1. Matar a todos los hijos inmediatamente
    for child in mp.active_children():
        child.terminate()
    
    # 2. Limpiar estados del monitor para que no se queden en "running"
    session = get_session()
    try:
        session.execute(delete(SystemStatus))
        session.commit()
    except:
        pass
    finally:
        session.close()
    
    # 3. Salir
    sys.exit(0)

# Registrar manejador de señales
signal.signal(signal.SIGINT, signal_handler)

# Asegurar que la tabla de logs existe
try:
    init_db()
except Exception:
    pass

# Configurar logging unificado
setup_logging(context="cli")
logger = logging.getLogger("dateados.cli")


def clear_logs():
    """Borra logs y estados de la base de datos para iniciar una nueva ejecución limpia."""
    session = get_session()
    try:
        cleanup_for_new_ingestion(session, clear_status=True)
    finally:
        session.close()


def run_smart_ingestion(limit_seasons: int | None = None, skip_outliers: bool = False):
    """Ejecuta la ingesta inteligente (híbrida incremental/full)."""
    if CLEAR_LOGS_ON_INGESTION_START:
        clear_logs()
        
    api_client = NBAApiClient()
    reporter = ProgressReporter("smart_ingestion", session_factory=get_session)
    
    try:
        strategy = SmartIngestion(api_client)
        strategy.run(limit_seasons, skip_outliers=skip_outliers, reporter=reporter)
        
    except FatalIngestionError as e:
        logger.error(f"🔴 ERROR FATAL: {e}")
        logger.error("🔄 Reiniciando proceso en 3 segundos...")
        restart_process()
        
    except Exception as e:
        logger.error(f"❌ Error inesperado: {e}", exc_info=True)
        sys.exit(1)


def run_full_ingestion_legacy(start_season: str, end_season: str, resume: bool):
    """Ejecuta ingesta histórica completa forzada (Legacy/Manual)."""
    if not resume and CLEAR_LOGS_ON_INGESTION_START:
        clear_logs()
        
    api_client = NBAApiClient()
    checkpoint_mgr = CheckpointManager()
    reporter = ProgressReporter("full_ingestion", session_factory=get_session)
    
    try:
        checkpoint = checkpoint_mgr.load_checkpoint() if resume else None
        
        full_ingestion = FullIngestion(api_client, checkpoint_mgr)
        full_ingestion.run(start_season, end_season, checkpoint, reporter=reporter)
        
    except FatalIngestionError as e:
        logger.error(f"🔴 ERROR FATAL: {e}")
        restart_process()
    except Exception as e:
        logger.error(f"❌ Error crítico: {e}", exc_info=True)
        sys.exit(1)


def main():
    """Punto de entrada principal del CLI."""
    try:
        parser = argparse.ArgumentParser(
            description='Sistema de Ingesta Inteligente NBA',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Ejemplos de uso:

  # Modo Inteligente (Recomendado): Detecta automáticamente qué falta y cómo bajarlo
  python -m ingestion.cli

  # Modo Inteligente con límite (ej: solo revisar últimas 2 temporadas)
  python -m ingestion.cli --limit-seasons 2

  # Modo Full Forzado (Para reparaciones o cargas manuales específicas)
  python -m ingestion.cli --mode full --start-season 1983-84

  # Inicializar base de datos
  python -m ingestion.cli --init-db
            """
        )
        
        # Modo de ingesta (ahora opcional, default=smart)
        parser.add_argument(
            '--mode',
            type=str,
            choices=['smart', 'full'],
            default='smart',
            help='Modo de ingesta: smart (auto, default) o full (forzado histórico)'
        )
        
        # Parámetros comunes/smart
        parser.add_argument(
            '--limit-seasons',
            type=int,
            default=None,
            help='(Smart) Número de temporadas hacia atrás a revisar (default: todas)'
        )
        
        parser.add_argument(
            '--skip-outliers',
            action='store_true',
            help='(Smart) Saltar detección de outliers al finalizar'
        )

        # Parámetros legacy full
        parser.add_argument(
            '--start-season',
            type=str,
            default='1983-84',
            help='(Full) Temporada de inicio'
        )
        parser.add_argument(
            '--end-season',
            type=str,
            help='(Full) Temporada final'
        )
        parser.add_argument(
            '--resume',
            action='store_true',
            help='(Full) Reanudar desde checkpoint'
        )
        
        # Init DB
        parser.add_argument(
            '--init-db',
            action='store_true',
            help='Inicializar base de datos antes de la ingesta'
        )
        
        args = parser.parse_args()
        
        if args.init_db:
            logger.info("Inicializando base de datos...")
            init_db()
            logger.info("✅ Base de datos inicializada")
            # Si solo se pidió init-db y no se especificó un modo explícito (o se usó el default smart),
            # podríamos parar aquí si el usuario no quería ejecutar.
            # Pero dado que smart es default, asumimos que si no hay flags extra, solo init.
            # Mejor criterio: Si solo hay init-db en sys.argv, salimos.
            if len(sys.argv) == 2 and sys.argv[1] == '--init-db':
                return
        
        if args.mode == 'smart':
            run_smart_ingestion(args.limit_seasons, args.skip_outliers)
            
        elif args.mode == 'full':
            if not args.end_season:
                today = date.today()
                year = today.year if today.month >= 10 else today.year - 1
                end_season = f"{year}-{(year + 1) % 100:02d}"
            else:
                end_season = normalize_season(args.end_season)
            
            start_season = normalize_season(args.start_season)
            run_full_ingestion_legacy(start_season, end_season, args.resume)

    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    # Asegurar modo fork para compartir eventos globales si estamos en Unix
    import multiprocessing as mp
    try:
        if sys.platform != 'win32':
            mp.set_start_method('fork', force=True)
    except RuntimeError:
        pass
    main()
