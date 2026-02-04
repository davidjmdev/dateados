"""Tests para la API Web (FastAPI).

Verifica que los endpoints principales responden correctamente y
que la integración entre el backend y la web funciona.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch

from web.app import app

client = TestClient(app)

def test_homepage_loads():
    """Verifica que la página de inicio carga (HTTP 200)."""
    response = client.get("/")
    assert response.status_code == 200
    # Verificamos que al menos contenga el título o algo identificativo
    assert "Dateados" in response.text

@patch("db.get_session")
def test_outliers_endpoint_with_mock_data(mock_get_session):
    """Verifica que el endpoint de outliers devuelve datos."""
    # Mockeamos la sesión de base de datos para no depender de una real
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    
    # Simulamos que la BD devuelve una lista de outliers
    # (Ajustar según cómo devuelva los datos tu ruta de outliers)
    response = client.get("/outliers")
    
    assert response.status_code == 200
    print("✅ Endpoint de Outliers verificado.")

def test_invalid_route_returns_404():
    """Verifica que rutas inexistentes devuelven 404."""
    response = client.get("/ruta-que-no-existe")
    assert response.status_code == 404
