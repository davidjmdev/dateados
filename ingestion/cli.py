"""CLI para ejecutar ingestas con reinicio automático.

Este módulo proporciona la interfaz de línea de comandos para ejecutar
ingestas de datos NBA con solo 2 modos:
- full: Ingesta histórica completa con checkpoints
- incremental: Últimos partidos

Incluye manejo automático de errores fatales con reinicio del proceso.
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
from ingestion.core import FullIngestion, IncrementalIngestion
from ingestion.restart import restart_process
from ingestion.utils import (
    FatalIngestionError, normalize_season, 
    ProgressReporter
)
from ingestion.config import LOG_FORMAT, LOG_DATE_FORMAT
from ingestion.log_config import LOG_LEVEL, CLEAR_LOGS_ON_INGESTION_START
from db.utils.logging_handler import SQLAlchemyHandler
from db.utils.log_cleanup import cleanup_for_new_ingestion
from db.models import LogEntry, SystemStatus
from sqlalchemy import delete

def signal_handler(sig, frame):
    """Manejador de Ctrl+C para una salida limpia y rápida."""
    # Solo actuar si somos el proceso principal
    import multiprocessing as mp
    if mp.current_process().name != 'MainProcess':
        return

    print("\n" + "!" * 80)
    print("⚠️  INTERRUPCIÓN DETECTADA (Ctrl+C)")
    print("🛑 Cerrando el sistema de forma inmediata...")
    print("!" * 80)
    
    # 1. Matar a todos los hijos inmediatamente
    for child in mp.active_children():
        child.terminate()
    
    # 2. Limpiar estados del monitor para que no se queden en "running"
    from db.connection import get_session
    from db.models import SystemStatus
    from sqlalchemy import delete
    
    session = get_session()
    try:
        session.execute(delete(SystemStatus))
        session.commit()
        print("🧹 Monitor de estados limpiado.")
    except:
        pass
    finally:
        session.close()
    
    # 3. Salir
    sys.exit(0)

# Registrar manejador de señales
signal.signal(signal.SIGINT, signal_handler)

# Asegurar que la tabla de logs (y el resto) existe antes de configurar logging
try:
    init_db()
except Exception:
    pass

# Configurar logging con nivel dinámico
log_level = getattr(logging, LOG_LEVEL, logging.INFO)

logging.basicConfig(
    level=log_level,
    format=LOG_FORMAT,
    datefmt=LOG_DATE_FORMAT,
    handlers=[
        SQLAlchemyHandler(),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def clear_logs():
    """Borra logs y estados de la base de datos para iniciar una nueva ejecución limpia.
    
    DEPRECATED: Usar cleanup_for_new_ingestion() del módulo log_cleanup en su lugar.
    """
    session = get_session()
    try:
        cleanup_for_new_ingestion(session, clear_status=True)
    finally:
        session.close()


def run_full_ingestion(start_season: str, end_season: str, resume: bool):
    """Ejecuta ingesta histórica completa paralela."""
    # Limpiar logs si no es una reanudación y está configurado
    if not resume and CLEAR_LOGS_ON_INGESTION_START:
        clear_logs()
        
    logger.info("=" * 80)
    logger.info("INICIANDO INGESTA COMPLETA HISTÓRICA (PARALELA)")
    logger.info("=" * 80)
    
    api_client = NBAApiClient()
    checkpoint_mgr = CheckpointManager()
    reporter = ProgressReporter("full_ingestion", session_factory=get_session)
    
    try:
        # Cargar checkpoint global si aplica
        checkpoint = checkpoint_mgr.load_checkpoint() if resume else None
        
        # Ejecutar ingesta
        full_ingestion = FullIngestion(api_client, checkpoint_mgr)
        full_ingestion.run(start_season, end_season, checkpoint, reporter=reporter)
        
        reporter.complete("Ingesta completa finalizada con éxito")
        
    except FatalIngestionError as e:
        logger.error("=" * 80)
        logger.error("🔴 ERROR FATAL DETECTADO EN PROCESO PRINCIPAL")
        logger.error("=" * 80)
        logger.error(f"Error: {e}")
        logger.error("🔄 Reiniciando proceso en 3 segundos...")
        logger.error("=" * 80)
        restart_process()
        
    except Exception as e:
        logger.error(f"❌ Error crítico en proceso principal: {e}", exc_info=True)
        sys.exit(1)


def run_incremental_ingestion(limit_seasons: int | None = None):
    """Ejecuta ingesta incremental paralela."""
    # CAMBIO: Usar configuración para decidir si limpiar logs
    if CLEAR_LOGS_ON_INGESTION_START:
        clear_logs()
        
    logger.info("=" * 80)
    logger.info("INICIANDO INGESTA INCREMENTAL (PARALELA)")
    logger.info("=" * 80)
    
    api_client = NBAApiClient()
    reporter = ProgressReporter("incremental_ingestion", session_factory=get_session)
    
    try:
        incremental = IncrementalIngestion(api_client)
        incremental.run(limit_seasons, reporter=reporter)
        
        logger.info("✅ Ingesta incremental finalizada con éxito.")
        
    except FatalIngestionError as e:
        logger.error("=" * 80)
        logger.error("🔴 ERROR FATAL DETECTADO EN INGESTA INCREMENTAL")
        logger.error("=" * 80)
        logger.error(f"Error: {e}")
        logger.error("🔄 Reiniciando proceso en 3 segundos...")
        logger.error("=" * 80)
        restart_process()
        
    except Exception as e:
        logger.error(f"❌ Error inesperado: {e}", exc_info=True)
        sys.exit(1)


def main():
    """Punto de entrada principal del CLI."""
    try:
        parser = argparse.ArgumentParser(
            description='Ingesta de datos NBA con reinicio automático',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Ejemplos de uso:

  # Ingesta incremental (procesa hasta encontrar partido ya existente)
  python -m ingestion.cli --mode incremental

  # Ingesta incremental limitada a las últimas 2 temporadas
  python -m ingestion.cli --mode incremental --limit-seasons 2

  # Ingesta completa desde 1983-84
  python -m ingestion.cli --mode full --start-season 1983-84

  # Ingesta completa de temporadas específicas
  python -m ingestion.cli --mode full --start-season 2020-21 --end-season 2023-24

  # Reanudar ingesta completa desde checkpoint
  python -m ingestion.cli --mode full --resume

  # Inicializar base de datos
  python -m ingestion.cli --init-db
            """
        )
        
        # Modo de ingesta
        parser.add_argument(
            '--mode',
            type=str,
            choices=['full', 'incremental'],
            help='Modo de ingesta (requerido si no se usa --init-db)'
        )
        
        # Parámetros para modo full
        parser.add_argument(
            '--start-season',
            type=str,
            default='1983-84',
            help='Temporada de inicio para modo full (default: 1983-84)'
        )
        parser.add_argument(
            '--end-season',
            type=str,
            help='Temporada final para modo full (default: temporada actual)'
        )
        parser.add_argument(
            '--resume',
            action='store_true',
            help='Reanudar desde checkpoint (se activa automáticamente tras reinicio)'
        )
        
        # Parámetros para modo incremental
        parser.add_argument(
            '--limit-seasons',
            type=int,
            default=None,
            help='Límite opcional de temporadas para modo incremental (por defecto: sin límite, procesa hasta encontrar partido existente)'
        )
        
        # Otros
        parser.add_argument(
            '--init-db',
            action='store_true',
            help='Inicializar base de datos antes de la ingesta'
        )
        
        args = parser.parse_args()
        
        # Inicializar BD si se solicita
        if args.init_db:
            logger.info("Inicializando base de datos...")
            init_db()
            logger.info("✅ Base de datos inicializada")
            
            if not args.mode:
                return  # Solo inicializar, no ingestar
        
        # Validar modo
        if not args.mode:
            parser.error("--mode es requerido (usar --help para ver ejemplos)")
        
        # Ejecutar según modo
        if args.mode == 'full':
            # Calcular end_season si no se proporciona
            if not args.end_season:
                today = date.today()
                year = today.year if today.month >= 10 else today.year - 1
                end_season = f"{year}-{(year + 1) % 100:02d}"
            else:
                end_season = normalize_season(args.end_season)
            
            start_season = normalize_season(args.start_season)
            
            run_full_ingestion(start_season, end_season, args.resume)
            
        elif args.mode == 'incremental':
            run_incremental_ingestion(args.limit_seasons)

    except KeyboardInterrupt:
        # Ya manejado por signal_handler
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
