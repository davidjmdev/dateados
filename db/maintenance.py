"""Herramientas de mantenimiento para la base de datos.

Este módulo consolida las funciones de limpieza y reparación de la base de datos.
"""
import logging
import sys
from typing import Optional

from sqlalchemy import or_, text
from db import get_session, get_engine
from db.models import (
    Game, PlayerGameStats, PlayerTeamSeason, TeamGameStats, PlayerAward,
    Player, AnomalyScore
)
from outliers.models import LeagueOutlier, PlayerOutlier, StreakRecord

logger = logging.getLogger(__name__)

class DatabaseMaintenance:
    """Clase para operaciones de mantenimiento de la base de datos."""

    @staticmethod
    def clean_all_data():
        """Elimina todos los datos de ingesta, manteniendo estructura de tablas."""
        session = get_session()
        get_engine()
        
        try:
            logger.info("=" * 80)
            logger.info("INICIANDO LIMPIEZA DE BASE DE DATOS")
            logger.info("=" * 80)
            
            # Tablas a limpiar en orden de dependencia (hijo -> padre)
            cleanup_steps = [
                (LeagueOutlier, "outliers de liga"),
                (PlayerOutlier, "outliers de jugador"),
                (StreakRecord, "rachas"),
                (PlayerAward, "premios de jugadores"),
                (TeamGameStats, "estadísticas de equipos"),
                (PlayerTeamSeason, "relaciones jugador-equipo"),
                (PlayerGameStats, "estadísticas de jugadores"),
                (Game, "partidos"),
            ]
            
            for model, desc in cleanup_steps:
                logger.info(f"Eliminando {desc}...")
                deleted = session.query(model).delete()
                session.commit()
                logger.info(f"   Eliminados {deleted} registros")
            
            logger.info("=" * 80)
            logger.info("BASE DE DATOS LIMPIADA CORRECTAMENTE")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Error durante la limpieza: {e}", exc_info=True)
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def clean_players():
        """Elimina todos los jugadores y sus referencias."""
        session = get_session()
        
        try:
            logger.info("=" * 80)
            logger.info("LIMPIANDO TABLA DE JUGADORES")
            logger.info("=" * 80)
            
            # Limpieza en cascada manual
            cleanup_steps = [
                (AnomalyScore, "estadísticas de anomalías"),
                (PlayerAward, "premios de jugadores"),
                (PlayerTeamSeason, "relaciones jugador-equipo"),
                (PlayerGameStats, "estadísticas de jugadores"),
                (Player, "jugadores"),
            ]

            for model, desc in cleanup_steps:
                logger.info(f"Eliminando {desc}...")
                deleted = session.query(model).delete()
                session.commit()
                logger.info(f"   Eliminados {deleted} registros")
            
            logger.info("=" * 80)
            logger.info("TABLA DE JUGADORES LIMPIADA CORRECTAMENTE")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Error durante la limpieza de jugadores: {e}", exc_info=True)
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def repair_bios():
        """Repara valores corruptos ('nan', 'None') en biografías de jugadores."""
        session = get_session()
        try:
            logger.info("Buscando jugadores con biografía corrupta...")
            
            corrupt_filter = or_(
                Player.height == 'nan',
                Player.height == 'None',
                Player.height == ''
            )
            
            players_to_fix = session.query(Player).filter(corrupt_filter).all()
            total = len(players_to_fix)
            
            if total == 0:
                logger.info("No se encontraron registros corruptos.")
                return

            logger.info(f"Reparando {total} jugadores...")
            
            for player in players_to_fix:
                player.height = "N/A"
                for field in ['school', 'country', 'jersey', 'position']:
                    val = getattr(player, field)
                    if val in ['nan', 'None', '']:
                        setattr(player, field, 'N/A')
            
            session.commit()
            logger.info(f"✓ Reparación completada. {total} jugadores actualizados.")
            
        except Exception as e:
            logger.error(f"Error durante la reparación: {e}")
            session.rollback()
        finally:
            session.close()

if __name__ == '__main__':
    # CLI simple para ejecutar acciones
    if len(sys.argv) > 1:
        action = sys.argv[1]
        logging.basicConfig(level=logging.INFO)
        if action == "clean_db":
            DatabaseMaintenance.clean_all_data()
        elif action == "clean_players":
            DatabaseMaintenance.clean_players()
        elif action == "repair_bios":
            DatabaseMaintenance.repair_bios()
        else:
            print(f"Acción desconocida: {action}")
            print("Uso: python -m db.maintenance [clean_db|clean_players|repair_bios]")
    else:
        print("Uso: python -m db.maintenance [clean_db|clean_players|repair_bios]")
