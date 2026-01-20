"""Configuracion y fixtures compartidas para tests.

Este modulo contiene fixtures reutilizables para todos los tests del proyecto.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date, datetime, timedelta
import sys
from pathlib import Path

# Agregar raiz al path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


# =============================================================================
# Fixtures de Modelos (sin BD)
# =============================================================================

@pytest.fixture
def sample_team_data():
    """Datos de ejemplo para crear un equipo."""
    return {
        'id': 1610612747,
        'full_name': 'Los Angeles Lakers',
        'abbreviation': 'LAL',
        'city': 'Los Angeles',
        'nickname': 'Lakers',
        'conference': 'West',
        'division': 'Pacific',
    }


@pytest.fixture
def sample_player_data():
    """Datos de ejemplo para crear un jugador."""
    return {
        'id': 2544,
        'full_name': 'LeBron James',
        'birthdate': date(1984, 12, 30),
        'height': '6-9',
        'weight': 250,
        'position': 'Forward',
        'country': 'USA',
        'season_exp': 21,
        'from_year': 2003,
        'to_year': 2024,
        'draft_year': 2003,
        'draft_round': 1,
        'draft_number': 1,
        'school': 'St. Vincent-St. Mary HS (OH)',
    }


@pytest.fixture
def sample_game_data():
    """Datos de ejemplo para crear un partido."""
    return {
        'id': '0022300123',
        'date': date(2024, 1, 15),
        'season': '2023-24',
        'rs': True,
        'po': False,
        'pi': False,
        'ist': False,
        'home_team_id': 1610612747,
        'away_team_id': 1610612744,
        'home_score': 120,
        'away_score': 115,
        'winner_team_id': 1610612747,
        'quarter_scores': {
            'home': [30, 28, 32, 30],
            'away': [28, 30, 27, 30]
        },
    }


@pytest.fixture
def sample_player_stats_data():
    """Datos de ejemplo para estadisticas de jugador."""
    return {
        'game_id': '0022300123',
        'player_id': 2544,
        'team_id': 1610612747,
        'min': timedelta(minutes=38, seconds=24),
        'pts': 32,
        'reb': 8,
        'ast': 11,
        'stl': 2,
        'blk': 1,
        'tov': 3,
        'pf': 2,
        'plus_minus': 15.0,
        'fgm': 12,
        'fga': 22,
        'fg_pct': 0.545,
        'fg3m': 4,
        'fg3a': 9,
        'fg3_pct': 0.444,
        'ftm': 4,
        'fta': 5,
        'ft_pct': 0.800,
        'jersey': '23',
    }


@pytest.fixture
def triple_double_stats():
    """Estadisticas que representan un triple-doble."""
    return {
        'pts': 25,
        'reb': 12,
        'ast': 10,
        'stl': 3,
        'blk': 2,
    }


@pytest.fixture
def double_double_stats():
    """Estadisticas que representan un doble-doble."""
    return {
        'pts': 20,
        'reb': 15,
        'ast': 5,
        'stl': 1,
        'blk': 0,
    }


@pytest.fixture
def quadruple_double_stats():
    """Estadisticas que representan un cuadruple-doble (raro)."""
    return {
        'pts': 18,
        'reb': 16,
        'ast': 10,
        'stl': 12,
        'blk': 4,
    }


# =============================================================================
# Fixtures de API Mock
# =============================================================================

@pytest.fixture
def mock_nba_api_response():
    """Retorna un objeto que simula una respuesta de la API de la NBA."""
    mock = MagicMock()
    mock.get_data_frames.return_value = []
    return mock


@pytest.fixture
def mock_boxscore_response():
    """Simula respuesta de BoxScoreTraditionalV3."""
    import pandas as pd
    
    mock = MagicMock()
    player_stats_df = pd.DataFrame([{
        'personId': 2544,
        'teamId': 1610612747,
        'nameI': 'L. James',
        'minutes': '38:24',
        'points': 32,
        'reboundsTotal': 8,
        'assists': 11,
        'steals': 2,
        'blocks': 1,
        'turnovers': 3,
        'foulsPersonal': 2,
        'plusMinusPoints': 15.0,
        'fieldGoalsMade': 12,
        'fieldGoalsAttempted': 22,
        'fieldGoalsPercentage': 0.545,
        'threePointersMade': 4,
        'threePointersAttempted': 9,
        'threePointersPercentage': 0.444,
        'freeThrowsMade': 4,
        'freeThrowsAttempted': 5,
        'freeThrowsPercentage': 0.800,
        'jerseyNum': '23',
    }])
    
    team_stats_df = pd.DataFrame([{
        'teamId': 1610612747,
        'points': 120,
    }])
    
    mock.get_data_frames.return_value = [player_stats_df, team_stats_df]
    return mock


@pytest.fixture
def mock_game_summary_response():
    """Simula respuesta de BoxScoreSummaryV3."""
    import pandas as pd
    
    mock = MagicMock()
    
    # DataFrame 0: game_info
    game_info_df = pd.DataFrame([{
        'homeTeamId': 1610612747,
        'awayTeamId': 1610612744,
        'gameTimeUTC': '2024-01-15T20:00:00Z',
        'gameStatus': 3,
    }])
    
    # Crear lista de DataFrames (8 elementos para coincidir con estructura real)
    dfs = [game_info_df] + [pd.DataFrame() for _ in range(7)]
    
    # DataFrame 7: team stats con puntos
    dfs[7] = pd.DataFrame([
        {'teamId': 1610612747, 'points': 120},
        {'teamId': 1610612744, 'points': 115},
    ])
    
    mock.get_data_frames.return_value = dfs
    return mock


@pytest.fixture
def mock_league_game_finder_response():
    """Simula respuesta de LeagueGameFinder."""
    import pandas as pd
    
    mock = MagicMock()
    games_df = pd.DataFrame([
        {'GAME_ID': '0022300123', 'GAME_DATE': '2024-01-15'},
        {'GAME_ID': '0022300122', 'GAME_DATE': '2024-01-14'},
        {'GAME_ID': '0022300121', 'GAME_DATE': '2024-01-13'},
    ])
    mock.get_data_frames.return_value = [games_df]
    return mock


# =============================================================================
# Fixtures de Game IDs
# =============================================================================

@pytest.fixture
def regular_season_game_id():
    """Game ID de temporada regular."""
    return '0022300123'


@pytest.fixture
def playoff_game_id():
    """Game ID de playoffs."""
    return '0042300123'


@pytest.fixture
def playin_game_id():
    """Game ID de PlayIn."""
    return '0052300001'


@pytest.fixture
def ist_game_id():
    """Game ID de In-Season Tournament final."""
    return '0062300001'


@pytest.fixture
def preseason_game_id():
    """Game ID de pretemporada."""
    return '0012300001'


@pytest.fixture
def allstar_game_id():
    """Game ID de All-Star Game."""
    return '0032300001'


@pytest.fixture
def long_format_game_id():
    """Game ID en formato largo con fecha embebida."""
    return '00220231015001'  # RS, 2023-10-15, game #001


# =============================================================================
# Fixtures de Temporadas
# =============================================================================

@pytest.fixture
def season_formats():
    """Diferentes formatos de temporada para testing."""
    return {
        'standard': '2023-24',
        'no_dash': '202324',
        'year_only': '2023',
        'with_spaces': '2023 - 24',
    }


# =============================================================================
# Fixtures de Minutes
# =============================================================================

@pytest.fixture
def minutes_formats():
    """Diferentes formatos de minutos para testing."""
    return {
        'standard': ('35:30', timedelta(minutes=35, seconds=30)),
        'short': ('5:30', timedelta(minutes=5, seconds=30)),
        'long': ('65:00', timedelta(minutes=65)),
        'with_hours': ('1:05:30', timedelta(hours=1, minutes=5, seconds=30)),
        'decimal': ('35.5', timedelta(minutes=35, seconds=30)),
        'empty': ('', timedelta(0)),
        'none': (None, timedelta(0)),
        'zero': ('0:00', timedelta(0)),
        'overtime': ('48:00', timedelta(minutes=48)),
    }


# =============================================================================
# Fixtures de Valores Edge Cases
# =============================================================================

@pytest.fixture
def edge_case_values():
    """Valores edge case para conversiones seguras."""
    return {
        'valid_int': ('10', 10),
        'float_to_int': ('10.7', 10),
        'negative': ('-5', -5),
        'zero': ('0', 0),
        'none': (None, 0),
        'empty_string': ('', 0),
        'invalid': ('abc', 0),
        'whitespace': ('  ', 0),
        'scientific': ('1e2', 100),
    }


@pytest.fixture
def edge_case_floats():
    """Valores edge case para conversiones de float."""
    return {
        'valid': ('10.5', 10.5),
        'integer': ('10', 10.0),
        'negative': ('-5.5', -5.5),
        'zero': ('0', 0.0),
        'none': (None, 0.0),
        'invalid': ('abc', 0.0),
        'scientific': ('1.5e2', 150.0),
    }


# =============================================================================
# Fixtures de Dates
# =============================================================================

@pytest.fixture
def date_formats():
    """Diferentes formatos de fecha para testing."""
    target_date = date(2024, 1, 15)
    return {
        'iso': ('2024-01-15', target_date),
        'datetime_obj': (datetime(2024, 1, 15, 10, 30), target_date),
        'date_obj': (date(2024, 1, 15), target_date),
        'us_format': ('01/15/2024', target_date),
        'with_time': ('2024-01-15T10:30:00Z', target_date),
        'none': (None, None),
        'invalid': ('not-a-date', None),
    }


# =============================================================================
# Fixtures de Shooting Stats
# =============================================================================

@pytest.fixture
def valid_shooting_stats():
    """Estadisticas de tiro validas."""
    return {
        'fgm': 10,
        'fga': 20,
        'fg_pct': 0.5,
        'fg3m': 3,
        'fg3a': 8,
        'fg3_pct': 0.375,
        'ftm': 5,
        'fta': 6,
        'ft_pct': 0.833,
    }


@pytest.fixture
def invalid_shooting_stats():
    """Estadisticas de tiro con errores que necesitan correccion."""
    return {
        'fgm_gt_fga': {'fgm': 15, 'fga': 10},  # fgm > fga
        'fg3m_gt_fg3a': {'fg3m': 10, 'fg3a': 5},  # fg3m > fg3a
        'fg3m_gt_fgm': {'fgm': 8, 'fg3m': 10},  # fg3m > fgm
        'fg3a_gt_fga': {'fga': 10, 'fg3a': 15},  # fg3a > fga
        'ftm_gt_fta': {'ftm': 10, 'fta': 5},  # ftm > fta
    }
