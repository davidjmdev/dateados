#!/usr/bin/env python3
"""Script para limpiar solo la tabla de jugadores.

Este script elimina todos los jugadores de la base de datos.
También elimina las referencias en tablas relacionadas:
- PlayerGameStats
- AnomalyScore
- PlayerTeamSeason
"""

import sys
from pathlib import Path

# Agregar el directorio raíz al PYTHONPATH
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import logging
from db import get_session
from db.models import Player, PlayerGameStats, AnomalyScore, PlayerTeamSeason, PlayerAward

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def clean_players():
    """Elimina todos los jugadores y sus referencias relacionadas."""
    session = get_session()
    
    try:
        logger.info("=" * 80)
        logger.info("LIMPIANDO TABLA DE JUGADORES")
        logger.info("=" * 80)
        
        # Contar registros antes de eliminar
        players_count = session.query(Player).count()
        stats_count = session.query(PlayerGameStats).count()
        anomaly_count = session.query(AnomalyScore).count()
        team_seasons_count = session.query(PlayerTeamSeason).count()
        awards_count = session.query(PlayerAward).count()
        
        logger.info(f"\nRegistros antes de limpiar:")
        logger.info(f"  Jugadores: {players_count}")
        logger.info(f"  Estadísticas de jugadores: {stats_count}")
        logger.info(f"  Anomalías: {anomaly_count}")
        logger.info(f"  Relaciones jugador-equipo-temporada: {team_seasons_count}")
        logger.info(f"  Premios: {awards_count}")
        
        # Confirmar
        logger.info("\n⚠️  ADVERTENCIA: Se eliminarán TODOS los jugadores")
        logger.info("   También se eliminarán las referencias en tablas relacionadas")
        
        # Eliminar en orden (respetando foreign keys)
        logger.info("\n1. Eliminando estadísticas de anomalías...")
        deleted = session.query(AnomalyScore).delete()
        session.commit()
        logger.info(f"   Eliminados {deleted} registros")
        
        logger.info("2. Eliminando premios de jugadores...")
        deleted = session.query(PlayerAward).delete()
        session.commit()
        logger.info(f"   Eliminados {deleted} registros")
        
        logger.info("3. Eliminando relaciones jugador-equipo-temporada...")
        deleted = session.query(PlayerTeamSeason).delete()
        session.commit()
        logger.info(f"   Eliminados {deleted} registros")
        
        logger.info("4. Eliminando estadísticas de jugadores por partido...")
        deleted = session.query(PlayerGameStats).delete()
        session.commit()
        logger.info(f"   Eliminados {deleted} registros")
        
        logger.info("5. Eliminando jugadores...")
        deleted = session.query(Player).delete()
        session.commit()
        logger.info(f"   Eliminados {deleted} registros")
        
        # Verificar que todo se eliminó
        logger.info("\nVerificando limpieza...")
        players_after = session.query(Player).count()
        stats_after = session.query(PlayerGameStats).count()
        anomaly_after = session.query(AnomalyScore).count()
        team_seasons_after = session.query(PlayerTeamSeason).count()
        awards_after = session.query(PlayerAward).count()
        
        logger.info("\nRegistros después de limpiar:")
        logger.info(f"  {'✓' if players_after == 0 else '✗'} Jugadores: {players_after}")
        logger.info(f"  {'✓' if stats_after == 0 else '✗'} Estadísticas de jugadores: {stats_after}")
        logger.info(f"  {'✓' if anomaly_after == 0 else '✗'} Anomalías: {anomaly_after}")
        logger.info(f"  {'✓' if team_seasons_after == 0 else '✗'} Relaciones jugador-equipo-temporada: {team_seasons_after}")
        logger.info(f"  {'✓' if awards_after == 0 else '✗'} Premios: {awards_after}")
        
        logger.info("\n" + "=" * 80)
        if players_after == 0:
            logger.info("✓ TABLA DE JUGADORES LIMPIADA CORRECTAMENTE")
        else:
            logger.warning("⚠ Aún quedan jugadores en la base de datos")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Error durante la limpieza: {e}", exc_info=True)
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == '__main__':
    clean_players()
