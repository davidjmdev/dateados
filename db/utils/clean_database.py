"""Script para limpiar la base de datos, eliminando todos los datos ingeridos.

Este script elimina todos los datos de las tablas relacionadas con la ingesta,
pero mantiene la estructura de las tablas.
"""

import sys
from pathlib import Path

# Agregar el directorio raíz al PYTHONPATH
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import logging
from db import get_session, get_engine
from db.models import (
    Game, PlayerGameStats, PlayerTeamSeason, TeamGameStats, PlayerAward
)
from outliers.models import LeagueOutlier, PlayerOutlier, StreakRecord

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def clean_database():
    """Limpia todos los datos de la base de datos."""
    session = get_session()
    get_engine()
    
    try:
        logger.info("=" * 80)
        logger.info("INICIANDO LIMPIEZA DE BASE DE DATOS")
        logger.info("=" * 80)
        
        # Contar registros antes de eliminar
        counts_before = {}
        counts_before['games'] = session.query(Game).count()
        counts_before['player_game_stats'] = session.query(PlayerGameStats).count()
        counts_before['player_team_seasons'] = session.query(PlayerTeamSeason).count()
        counts_before['team_game_stats'] = session.query(TeamGameStats).count()
        counts_before['player_awards'] = session.query(PlayerAward).count()
        counts_before['outliers_league'] = session.query(LeagueOutlier).count()
        counts_before['outliers_player'] = session.query(PlayerOutlier).count()
        counts_before['outliers_streaks'] = session.query(StreakRecord).count()
        
        logger.info("\nRegistros antes de limpiar:")
        for table, count in counts_before.items():
            logger.info(f"  {table}: {count}")
        
        # Confirmar
        logger.info("\n  ADVERTENCIA: Se eliminaran TODOS los datos de ingesta")
        logger.info("   Esto incluye: partidos, estadisticas, tablas derivadas, premios, outliers")
        logger.info("   Las tablas teams y players NO se eliminaran")
        
        # Eliminar en orden (respetando foreign keys)
        logger.info("\n1. Eliminando outliers de liga...")
        deleted = session.query(LeagueOutlier).delete()
        session.commit()
        logger.info(f"   Eliminados {deleted} registros")
        
        logger.info("2. Eliminando outliers de jugador...")
        deleted = session.query(PlayerOutlier).delete()
        session.commit()
        logger.info(f"   Eliminados {deleted} registros")
        
        logger.info("3. Eliminando rachas...")
        deleted = session.query(StreakRecord).delete()
        session.commit()
        logger.info(f"   Eliminados {deleted} registros")
        
        logger.info("4. Eliminando premios de jugadores...")
        deleted = session.query(PlayerAward).delete()
        session.commit()
        logger.info(f"   Eliminados {deleted} registros")
        
        logger.info("5. Eliminando estadisticas de equipos por partido...")
        deleted = session.query(TeamGameStats).delete()
        session.commit()
        logger.info(f"   Eliminados {deleted} registros")
        
        logger.info("6. Eliminando relaciones jugador-equipo-temporada...")
        deleted = session.query(PlayerTeamSeason).delete()
        session.commit()
        logger.info(f"   Eliminados {deleted} registros")
        
        logger.info("7. Eliminando estadisticas de jugadores por partido...")
        deleted = session.query(PlayerGameStats).delete()
        session.commit()
        logger.info(f"   Eliminados {deleted} registros")
        
        logger.info("8. Eliminando partidos...")
        deleted = session.query(Game).delete()
        session.commit()
        logger.info(f"   Eliminados {deleted} registros")
        
        # Verificar que todo se eliminó
        logger.info("\nVerificando limpieza...")
        counts_after = {}
        counts_after['games'] = session.query(Game).count()
        counts_after['player_game_stats'] = session.query(PlayerGameStats).count()
        counts_after['player_team_seasons'] = session.query(PlayerTeamSeason).count()
        counts_after['team_game_stats'] = session.query(TeamGameStats).count()
        counts_after['player_awards'] = session.query(PlayerAward).count()
        counts_after['outliers_league'] = session.query(LeagueOutlier).count()
        counts_after['outliers_player'] = session.query(PlayerOutlier).count()
        counts_after['outliers_streaks'] = session.query(StreakRecord).count()
        
        all_clean = all(count == 0 for count in counts_after.values())
        
        logger.info("\nRegistros despues de limpiar:")
        for table, count in counts_after.items():
            status = "OK" if count == 0 else "FAIL"
            logger.info(f"  {status} {table}: {count}")
        
        logger.info("\n" + "=" * 80)
        if all_clean:
            logger.info("OK BASE DE DATOS LIMPIADA CORRECTAMENTE")
        else:
            logger.warning("WARN Algunas tablas aun tienen registros")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Error durante la limpieza: {e}", exc_info=True)
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == '__main__':
    clean_database()
