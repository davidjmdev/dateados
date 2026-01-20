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
from datetime import date
from pathlib import Path

# Configurar path del proyecto
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from db import init_db
from ingestion.api_client import NBAApiClient
from ingestion.checkpoints import CheckpointManager
from ingestion.core import FullIngestion, IncrementalIngestion
from ingestion.restart import restart_process
from ingestion.utils import FatalIngestionError, normalize_season
from ingestion.config import LOG_FORMAT, LOG_DATE_FORMAT
from db.utils.logging_handler import SQLAlchemyHandler
from db.models import LogEntry
from sqlalchemy import delete

# Asegurar que la tabla de logs (y el resto) existe antes de configurar logging
try:
    init_db()
except Exception:
    pass

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    datefmt=LOG_DATE_FORMAT,
    handlers=[
        SQLAlchemyHandler(),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def clear_logs():
    """Borra todos los logs de la base de datos para iniciar una nueva ejecución limpia."""
    from db.connection import get_session
    session = get_session()
    try:
        session.execute(delete(LogEntry))
        session.commit()
        logger.info("🧹 Logs de base de datos limpiados para nueva ejecución.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error al limpiar logs: {e}")
    finally:
        session.close()


def run_full_ingestion(start_season: str, end_season: str, resume: bool):
    """Ejecuta ingesta histórica completa paralela."""
    # Limpiar logs si no es una reanudación (opcional, pero según user: "cuando se lance un proceso nuevo")
    if not resume:
        clear_logs()
        
    logger.info("=" * 80)
    logger.info("INICIANDO INGESTA COMPLETA HISTÓRICA (PARALELA)")
    logger.info("=" * 80)
    logger.info(f"Temporadas: {start_season} a {end_season}")
    if resume:
        logger.info("Modo: REANUDACIÓN")
    logger.info("=" * 80)
    
    api_client = NBAApiClient()
    checkpoint_mgr = CheckpointManager() # Manager global para verificar estados generales
    
    try:
        # Cargar checkpoint global si aplica para filtrar temporadas
        checkpoint = checkpoint_mgr.load_checkpoint() if resume else None
        
        # Ejecutar ingesta
        full_ingestion = FullIngestion(api_client, checkpoint_mgr)
        full_ingestion.run(start_season, end_season, checkpoint)
        
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


def run_incremental_ingestion(limit_seasons: int):
    """Ejecuta ingesta incremental paralela."""
    clear_logs()
    logger.info("=" * 80)
    logger.info("INICIANDO INGESTA INCREMENTAL (PARALELA)")
    logger.info("=" * 80)
    
    api_client = NBAApiClient()
    
    try:
        incremental = IncrementalIngestion(api_client)
        incremental.run(limit_seasons)
        
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
    parser = argparse.ArgumentParser(
        description='Ingesta de datos NBA con reinicio automático',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:

  # Ingesta incremental (últimos partidos)
  python -m ingestion.cli --mode incremental

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
        default=3,
        help='Temporadas atrás para modo incremental (default: 3)'
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


if __name__ == '__main__':
    main()
