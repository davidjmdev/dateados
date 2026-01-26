"""Tests para la logica de ingesta.

Estos tests verifican validacion de estadisticas y otras logicas de ingesta sin
necesidad de conexion a la API real.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date, timedelta


class TestFetchWithRetry:
    """Tests para fetch_with_retry() usando mocks."""
    
    def test_success_first_try(self):
        """Exito en el primer intento."""
        from ingestion.api_common import fetch_with_retry
        
        mock_func = MagicMock(return_value="success")
        result = fetch_with_retry(mock_func, max_retries=3)
        
        assert result == "success"
        assert mock_func.call_count == 1
    
    def test_success_after_retry(self):
        """Exito despues de reintentos."""
        from ingestion.api_common import fetch_with_retry
        
        # Falla 2 veces, exito en la tercera
        mock_func = MagicMock(side_effect=[
            Exception("Error 1"),
            Exception("Error 2"),
            "success"
        ])
        
        with patch('ingestion.api_common.time.sleep'):
            result = fetch_with_retry(mock_func, max_retries=3)
        
        assert result == "success"
        assert mock_func.call_count == 3
    
    def test_failure_all_retries(self):
        """Fallo en todos los reintentos lanza FatalIngestionError si fatal=True."""
        from ingestion.api_common import fetch_with_retry, FatalIngestionError
        
        mock_func = MagicMock(side_effect=Exception("Persistent error"))
        
        with patch('ingestion.api_common.time.sleep'):
            with pytest.raises(FatalIngestionError):
                fetch_with_retry(mock_func, max_retries=3, fatal=True)
        
        assert mock_func.call_count == 3

    def test_failure_no_fatal(self):
        """Fallo en todos los reintentos retorna None si fatal=False."""
        from ingestion.api_common import fetch_with_retry
        
        mock_func = MagicMock(side_effect=Exception("Persistent error"))
        
        with patch('ingestion.api_common.time.sleep'):
            result = fetch_with_retry(mock_func, max_retries=3, fatal=False)
        
        assert result is None
        assert mock_func.call_count == 3
    
    def test_no_retry_on_resultset_error(self):
        """No reintenta si el error indica datos no disponibles (resultSet empty)."""
        from ingestion.api_common import fetch_with_retry
        
        mock_func = MagicMock(side_effect=Exception("resultSet is empty"))
        
        # Aunque pidamos 3 retries, deberia frenar al primero
        result = fetch_with_retry(mock_func, max_retries=3)
        
        assert result is None
        assert mock_func.call_count == 1


class TestIsValidTeamId:
    """Tests para is_valid_team_id()."""
    
    def test_valid_standard_range(self):
        """ID en el rango estandar de equipos NBA."""
        from db.services import is_valid_team_id
        
        # Lakers ID
        assert is_valid_team_id(1610612747) is True
        # Warriors ID
        assert is_valid_team_id(1610612744) is True
    
    def test_invalid_id(self):
        """ID fuera del rango conocido."""
        from db.services import is_valid_team_id
        
        assert is_valid_team_id(12345) is False
    
    def test_special_event_team_allowed(self):
        """IDs de equipos de eventos especiales cuando esta permitido."""
        from db.services import is_valid_team_id
        from db.constants import SPECIAL_EVENT_TEAM_IDS
        
        for team_id in SPECIAL_EVENT_TEAM_IDS:
            assert is_valid_team_id(team_id, allow_special_events=True) is True
    
    def test_special_event_team_not_allowed(self):
        """IDs de equipos de eventos especiales cuando no esta permitido."""
        from db.services import is_valid_team_id
        from db.constants import SPECIAL_EVENT_TEAM_IDS
        
        for team_id in SPECIAL_EVENT_TEAM_IDS:
            # Sin allow_special_events, deberia fallar
            result = is_valid_team_id(team_id, allow_special_events=False)
            assert result is False
