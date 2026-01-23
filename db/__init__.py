"""Módulo de base de datos para el proyecto NBA.

Este módulo centraliza toda la funcionalidad relacionada con la base de datos:
- Modelos SQLAlchemy
- Configuración de conexión
- Utilidades y migraciones
- Utilidades de consulta
"""

from db.connection import DATABASE_URL, init_db, get_session, get_engine
from db.models import (
    Base,
    Team,
    Player,
    Game,
    PlayerGameStats,
    PlayerTeamSeason,
    TeamGameStats,
    PlayerAward,
    IngestionCheckpoint
)

# Importar funciones de consulta
from db.query import (
    get_database_stats,
    get_teams,
    get_players,
    get_games,
    get_player_stats,
    get_player_season_averages,
    get_top_players,
    get_team_record,
    get_game_details,
    search_games_by_score,
    get_current_teammates,
    get_historical_teammates,
    get_player_career_stats,
    get_player_career_highs,
    get_player_awards
)

# Importar funciones de resumen
from db.summary import (
    get_record_counts,
    print_summary,
    get_summary_string
)

__all__ = [
    'DATABASE_URL',
    'init_db',
    'get_session',
    'get_engine',
    'Base',
    'Team',
    'Player',
    'Game',
    'PlayerGameStats',
    'PlayerTeamSeason',
    'TeamGameStats',
    'PlayerAward',
    'IngestionCheckpoint',
    # Funciones de consulta
    'get_database_stats',
    'get_teams',
    'get_players',
    'get_games',
    'get_player_stats',
    'get_player_season_averages',
    'get_top_players',
    'get_team_record',
    'get_game_details',
    'search_games_by_score',
    'get_current_teammates',
    'get_historical_teammates',
    'get_player_career_stats',
    'get_player_career_highs',
    'get_player_awards',
    # Funciones de resumen

    'get_record_counts',
    'print_summary',
    'get_summary_string',
]
