"""Tests para la logica de ingesta.

Estos tests verifican las funciones de clasificacion de partidos,
validacion de estadisticas y otras logicas de ingesta sin
necesidad de conexion a la API real.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date, timedelta

from ingestion.ingest import (
    classify_game_type,
    _validate_and_fix_stats,
    _get_col,
)


class TestClassifyGameType:
    """Tests para classify_game_type()."""
    
    def test_regular_season(self):
        """Partido de temporada regular."""
        result = classify_game_type("0022300123", "2023-24")
        assert result['rs'] is True
        assert result['po'] is False
        assert result['pi'] is False
        assert result['ist'] is False
    
    def test_playoffs(self):
        """Partido de playoffs."""
        result = classify_game_type("0042300123", "2023-24")
        assert result['rs'] is False
        assert result['po'] is True
        assert result['pi'] is False
        assert result['ist'] is False
    
    def test_playin_modern_season(self):
        """Partido de PlayIn en temporada moderna (2020-21+)."""
        result = classify_game_type("0052300001", "2023-24")
        assert result['rs'] is False
        assert result['po'] is False
        assert result['pi'] is True
        assert result['ist'] is False
    
    def test_playin_old_season(self):
        """PlayIn no existia antes de 2020-21."""
        result = classify_game_type("0052000001", "2019-20")
        # El codigo aun puede clasificar el tipo, pero pi no se activa
        assert result['pi'] is False
    
    def test_ist_final(self):
        """Final del In-Season Tournament."""
        result = classify_game_type("0062300001", "2023-24")
        assert result['rs'] is False
        assert result['po'] is False
        assert result['pi'] is False
        assert result['ist'] is True
    
    def test_preseason_returns_all_false(self):
        """Pretemporada no activa ningun flag."""
        result = classify_game_type("0012300001", "2023-24")
        assert result['rs'] is False
        assert result['po'] is False
        assert result['pi'] is False
        assert result['ist'] is False
    
    def test_allstar_returns_all_false(self):
        """All-Star no activa ningun flag."""
        result = classify_game_type("0032300001", "2023-24")
        assert result['rs'] is False
        assert result['po'] is False
        assert result['pi'] is False
        assert result['ist'] is False


class TestValidateAndFixStats:
    """Tests para _validate_and_fix_stats()."""
    
    def test_valid_stats_unchanged(self):
        """Estadisticas validas no se modifican."""
        stats = {
            'fgm': 10, 'fga': 20, 'fg_pct': 0.5,
            'fg3m': 3, 'fg3a': 8, 'fg3_pct': 0.375,
            'ftm': 5, 'fta': 6, 'ft_pct': 0.833,
        }
        result = _validate_and_fix_stats(stats, "test_game", 1)
        
        assert result['fgm'] == 10
        assert result['fga'] == 20
        assert result['fg3m'] == 3
        assert result['fg3a'] == 8
        assert result['ftm'] == 5
        assert result['fta'] == 6
    
    def test_fix_fgm_greater_than_fga(self):
        """Corrige fgm > fga."""
        stats = {
            'fgm': 25, 'fga': 20, 'fg_pct': 0.5,
            'fg3m': 3, 'fg3a': 8, 'fg3_pct': 0.375,
            'ftm': 5, 'fta': 6, 'ft_pct': 0.833,
        }
        result = _validate_and_fix_stats(stats, "test_game", 1)
        
        # fga debe ajustarse a fgm
        assert result['fgm'] == 25
        assert result['fga'] == 25
        assert result['fg_pct'] == 1.0
    
    def test_fix_fg3m_greater_than_fg3a(self):
        """Corrige fg3m > fg3a."""
        stats = {
            'fgm': 10, 'fga': 20, 'fg_pct': 0.5,
            'fg3m': 10, 'fg3a': 5, 'fg3_pct': 0.5,
            'ftm': 5, 'fta': 6, 'ft_pct': 0.833,
        }
        result = _validate_and_fix_stats(stats, "test_game", 1)
        
        # fg3a debe ajustarse a fg3m
        assert result['fg3m'] == 10
        assert result['fg3a'] == 10
    
    def test_fix_fg3m_greater_than_fgm(self):
        """Corrige fg3m > fgm (imposible en baloncesto)."""
        stats = {
            'fgm': 5, 'fga': 20, 'fg_pct': 0.25,
            'fg3m': 10, 'fg3a': 15, 'fg3_pct': 0.667,
            'ftm': 5, 'fta': 6, 'ft_pct': 0.833,
        }
        result = _validate_and_fix_stats(stats, "test_game", 1)
        
        # fg3m debe ajustarse a fgm
        assert result['fg3m'] == 5
    
    def test_fix_fg3a_greater_than_fga(self):
        """Corrige fg3a > fga."""
        stats = {
            'fgm': 10, 'fga': 20, 'fg_pct': 0.5,
            'fg3m': 5, 'fg3a': 25, 'fg3_pct': 0.2,
            'ftm': 5, 'fta': 6, 'ft_pct': 0.833,
        }
        result = _validate_and_fix_stats(stats, "test_game", 1)
        
        # fg3a debe ajustarse a fga
        assert result['fg3a'] == 20
    
    def test_fix_ftm_greater_than_fta(self):
        """Corrige ftm > fta."""
        stats = {
            'fgm': 10, 'fga': 20, 'fg_pct': 0.5,
            'fg3m': 3, 'fg3a': 8, 'fg3_pct': 0.375,
            'ftm': 10, 'fta': 5, 'ft_pct': 0.5,
        }
        result = _validate_and_fix_stats(stats, "test_game", 1)
        
        # fta debe ajustarse a ftm
        assert result['ftm'] == 10
        assert result['fta'] == 10
        assert result['ft_pct'] == 1.0
    
    def test_recalculate_invalid_percentages(self):
        """Recalcula porcentajes invalidos."""
        stats = {
            'fgm': 10, 'fga': 20, 'fg_pct': 2.0,  # Invalido
            'fg3m': 3, 'fg3a': 8, 'fg3_pct': -0.5,  # Invalido
            'ftm': 5, 'fta': 10, 'ft_pct': None,  # None
        }
        result = _validate_and_fix_stats(stats, "test_game", 1)
        
        assert result['fg_pct'] == 0.5
        assert result['fg3_pct'] == 0.375
        assert result['ft_pct'] == 0.5
    
    def test_zero_attempts(self):
        """Cero intentos produce 0% (no division por cero)."""
        stats = {
            'fgm': 0, 'fga': 0, 'fg_pct': None,
            'fg3m': 0, 'fg3a': 0, 'fg3_pct': None,
            'ftm': 0, 'fta': 0, 'ft_pct': None,
        }
        result = _validate_and_fix_stats(stats, "test_game", 1)
        
        assert result['fg_pct'] == 0.0
        assert result['fg3_pct'] == 0.0
        assert result['ft_pct'] == 0.0


class TestGetCol:
    """Tests para _get_col()."""
    
    def test_first_name_found(self):
        """Retorna el primer nombre encontrado."""
        row = {'personId': 123, 'PLAYER_ID': 456}
        assert _get_col(row, 'personId', 'PLAYER_ID') == 123
    
    def test_second_name_found(self):
        """Retorna el segundo nombre si el primero no existe."""
        row = {'PLAYER_ID': 456}
        assert _get_col(row, 'personId', 'PLAYER_ID') == 456
    
    def test_no_name_found(self):
        """Retorna None si ningun nombre existe."""
        row = {'other_field': 789}
        assert _get_col(row, 'personId', 'PLAYER_ID') is None
    
    def test_empty_row(self):
        """Retorna None para row vacio."""
        row = {}
        assert _get_col(row, 'personId', 'PLAYER_ID') is None
    
    def test_multiple_fallbacks(self):
        """Multiples fallbacks funcionan."""
        row = {'name': 'Test Player'}
        assert _get_col(row, 'nameI', 'PLAYER_NAME', 'playerName', 'name') == 'Test Player'


class TestFetchWithRetry:
    """Tests para fetch_with_retry() usando mocks."""
    
    def test_success_first_try(self):
        """Exito en el primer intento."""
        from ingestion.utils import fetch_with_retry
        
        mock_func = MagicMock(return_value="success")
        result = fetch_with_retry(mock_func, max_retries=3)
        
        assert result == "success"
        assert mock_func.call_count == 1
    
    def test_success_after_retry(self):
        """Exito despues de reintentos."""
        from ingestion.utils import fetch_with_retry
        
        # Falla 2 veces, exito en la tercera
        mock_func = MagicMock(side_effect=[
            Exception("Error 1"),
            Exception("Error 2"),
            "success"
        ])
        
        with patch('ingestion.utils.time.sleep'):
            result = fetch_with_retry(mock_func, max_retries=3)
        
        assert result == "success"
        assert mock_func.call_count == 3
    
    def test_failure_all_retries(self):
        """Fallo en todos los reintentos retorna None."""
        from ingestion.utils import fetch_with_retry
        
        mock_func = MagicMock(side_effect=Exception("Persistent error"))
        
        with patch('ingestion.utils.time.sleep'):
            result = fetch_with_retry(mock_func, max_retries=3)
        
        assert result is None
        assert mock_func.call_count == 3
    
    def test_no_retry_on_resultset_error(self):
        """No reintenta si el error indica datos no disponibles."""
        from ingestion.utils import fetch_with_retry
        
        mock_func = MagicMock(side_effect=Exception("resultSet is empty"))
        
        result = fetch_with_retry(mock_func, max_retries=3)
        
        assert result is None
        # Solo un intento porque resultSet indica datos no disponibles
        assert mock_func.call_count == 1


class TestIsValidTeamId:
    """Tests para is_valid_team_id()."""
    
    def test_valid_standard_range(self):
        """ID en el rango estandar de equipos NBA."""
        from ingestion.utils import is_valid_team_id
        
        # Lakers ID
        assert is_valid_team_id(1610612747) is True
        # Warriors ID
        assert is_valid_team_id(1610612744) is True
    
    def test_invalid_id(self):
        """ID fuera del rango conocido."""
        from ingestion.utils import is_valid_team_id
        
        assert is_valid_team_id(12345) is False
    
    def test_special_event_team_allowed(self):
        """IDs de equipos de eventos especiales cuando esta permitido."""
        from ingestion.utils import is_valid_team_id
        from ingestion.config import SPECIAL_EVENT_TEAM_IDS
        
        for team_id in SPECIAL_EVENT_TEAM_IDS:
            assert is_valid_team_id(team_id, allow_special_events=True) is True
    
    def test_special_event_team_not_allowed(self):
        """IDs de equipos de eventos especiales cuando no esta permitido."""
        from ingestion.utils import is_valid_team_id
        from ingestion.config import SPECIAL_EVENT_TEAM_IDS
        
        for team_id in SPECIAL_EVENT_TEAM_IDS:
            # Sin allow_special_events, deberia fallar
            result = is_valid_team_id(team_id, allow_special_events=False)
            assert result is False


class TestSeasonRange:
    """Tests para get_season_range() del runner."""
    
    def test_single_season(self):
        """Rango de una sola temporada."""
        from ingestion.runner import get_season_range
        
        result = get_season_range("2023-24", "2023-24")
        assert result == ["2023-24"]
    
    def test_multiple_seasons(self):
        """Rango de multiples temporadas."""
        from ingestion.runner import get_season_range
        
        result = get_season_range("2021-22", "2023-24")
        assert result == ["2021-22", "2022-23", "2023-24"]
    
    def test_century_boundary(self):
        """Rango que cruza el cambio de siglo."""
        from ingestion.runner import get_season_range
        
        result = get_season_range("1998-99", "2001-02")
        assert "1999-00" in result
        assert "2000-01" in result
