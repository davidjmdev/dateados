"""Test de integración de flujo completo.

Verifica que los datos fluyen correctamente desde la lógica de ingesta 
hasta la base de datos y el detector de outliers.
"""

import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import date, timedelta

from db.models import Base, Game, Player, PlayerGameStats, Team
from ingestion.ingestors import GameIngestion
from outliers.stats.player_zscore import PlayerZScoreDetector

@pytest.fixture
def test_db():
    """Crea una base de datos SQLite en memoria para tests."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()

def test_full_ingestion_to_outlier_flow(test_db):
    """
    TEST DE CIRCUITO COMPLETO:
    1. Simula la llegada de un partido de la NBA.
    2. Lo ingesta en la base de datos.
    3. Ejecuta el detector de outliers.
    4. Verifica que se detectó la anomalía.
    """
    session = test_db
    
    # Pre-requisito: Necesitamos un equipo y un historial para el jugador
    team = Team(id=1, full_name="Lakers", abbreviation="LAL")
    team2 = Team(id=2, full_name="Warriors", abbreviation="GSW")
    player = Player(id=2544, full_name="LeBron James")
    session.add_all([team, team2, player])
    session.commit()
    
    # Creamos un "pasado" normal (25 partidos para superar el periodo de rookie)
    historical_stats = []
    for i in range(25):
        game_id = f"002230000{i:02d}"
        g = Game(id=game_id, date=date(2024, 1, i+1), season="2023-24", home_team_id=1, away_team_id=2, status=3)
        s = PlayerGameStats(
            game_id=game_id, player_id=2544, team_id=1, pts=20, 
            min=timedelta(minutes=30), fga=15, fgm=8,
            reb=5, ast=5, stl=1, blk=1, tov=2, pf=2,
            fg3m=2, fg3a=5, ftm=2, fta=2
        )
        session.add_all([g, s])
        historical_stats.append(s)
    session.commit()
    
    # IMPORTANTE: Inicializar el estado del detector con estos partidos previos
    detector = PlayerZScoreDetector(z_threshold=2.0)
    detector.detect(session, historical_stats)
    session.commit()

    # EL MOMENTO DE LA VERDAD: Un partido explosivo (60 puntos)
    mock_api = MagicMock()
    
    # Mock del summary
    mock_summary = MagicMock()
    mock_summary.get_dict.return_value = {
        'game': {
            'gameStatus': 3, 'gameEt': '2024-02-01T20:00:00Z',
            'homeTeamId': 1, 'awayTeamId': 2,
            'homeTeam': {'score': 120, 'periods': []},
            'awayTeam': {'score': 100, 'periods': []}
        }
    }
    mock_api.fetch_game_summary.return_value = mock_summary
    
    # Mock del boxscore
    import pandas as pd
    df_stats = pd.DataFrame([{
        'PLAYER_ID': 2544, 'TEAM_ID': 1, 'PLAYER_NAME': 'LeBron James',
        'MIN': '35:00', 'PTS': 60, 'REB': 10, 'AST': 10, 'STL': 2, 'BLK': 1, 'TOV': 2, 'PF': 2,
        'FGM': 20, 'FGA': 30, 'FG3M': 5, 'FG3A': 10, 'FTM': 15, 'FTA': 15
    }])
    mock_boxscore = MagicMock()
    mock_boxscore.get_data_frames.return_value = [df_stats]
    mock_api.fetch_game_boxscore.return_value = mock_boxscore

    # 1. Ejecutar Ingestión
    ingestor = GameIngestion(mock_api)
    ingestor.ingest_game(session, "0022300999", is_rs=True, is_po=False, is_pi=False, is_ist=False)
    
    # 2. Ejecutar Detección
    detector = PlayerZScoreDetector(z_threshold=2.0)
    new_stats = session.query(PlayerGameStats).filter_by(game_id="0022300999").all()
    results = detector.detect(session, new_stats)
    
    # 3. VERIFICACIÓN
    assert len(results) == 1
    assert results[0].is_outlier is True
    assert results[0].outlier_data['max_z_score'] > 2.0

def test_robustness_with_corrupt_data(test_db):
    """
    TEST DE ROBUSTEZ:
    Verifica que el sistema no explota si la API devuelve basura.
    """
    session = test_db
    
    # IMPORTANTE: Necesitamos los equipos en la BD para que la ingesta no los salte
    team1 = Team(id=1, full_name="Lakers", abbreviation="LAL")
    team2 = Team(id=2, full_name="Warriors", abbreviation="GSW")
    session.add_all([team1, team2])
    session.commit()
    
    mock_api = MagicMock()
    
    # Caso: La API devuelve puntos como texto en lugar de números
    import pandas as pd
    df_garbage = pd.DataFrame([{
        'PLAYER_ID': 2544, 'TEAM_ID': 1, 'PLAYER_NAME': 'LeBron James',
        'MIN': 'N/A', 'PTS': 'MUCHOS',
        'FGM': None, 'FGA': 'Basura'
    }])
    
    mock_boxscore = MagicMock()
    mock_boxscore.get_data_frames.return_value = [df_garbage]
    mock_api.fetch_game_boxscore.return_value = mock_boxscore
    
    mock_summary = MagicMock()
    mock_summary.get_dict.return_value = {
        'game': {
            'gameStatus': 3, 
            'gameEt': '2024-02-01T20:00:00Z',
            'homeTeamId': 1, 'awayTeamId': 2,
            'homeTeam': {'score': 0, 'periods': []},
            'awayTeam': {'score': 0, 'periods': []}
        }
    }
    mock_api.fetch_game_summary.return_value = mock_summary

    ingestor = GameIngestion(mock_api)
    
    # El sistema debe manejarlo sin lanzar excepción
    ingestor.ingest_game(session, "0022300666", is_rs=True, is_po=False, is_pi=False, is_ist=False)
    
    # Verificamos que se guardó (aunque sea con 0s)
    saved = session.query(PlayerGameStats).filter_by(game_id="0022300666").first()
    assert saved is not None
    assert saved.pts == 0
