"""Tests para los modelos SQLAlchemy de la base de datos.

Estos tests verifican la logica de negocio en las propiedades y metodos
de los modelos, sin necesidad de conexion a base de datos.
"""

import pytest
from datetime import date, timedelta
from db.models import (
    Team, Player, Game, PlayerGameStats, 
    TeamGameStats, PlayerTeamSeason, AnomalyScore
)


class TestPlayerGameStats:
    """Tests para el modelo PlayerGameStats."""
    
    # =========================================================================
    # Tests de Triple-Doble
    # =========================================================================
    
    def test_is_triple_double_with_pts_reb_ast(self):
        """Triple-doble clasico: puntos, rebotes, asistencias."""
        stats = PlayerGameStats(pts=25, reb=12, ast=10, stl=3, blk=2)
        assert stats.is_triple_double is True
    
    def test_is_triple_double_with_pts_reb_stl(self):
        """Triple-doble con robos en lugar de asistencias."""
        stats = PlayerGameStats(pts=20, reb=15, ast=5, stl=10, blk=2)
        assert stats.is_triple_double is True
    
    def test_is_triple_double_with_pts_ast_blk(self):
        """Triple-doble con bloqueos."""
        stats = PlayerGameStats(pts=30, reb=8, ast=12, stl=3, blk=10)
        assert stats.is_triple_double is True
    
    def test_is_triple_double_with_reb_ast_stl(self):
        """Triple-doble sin puntos (muy raro)."""
        stats = PlayerGameStats(pts=8, reb=15, ast=12, stl=10, blk=2)
        assert stats.is_triple_double is True
    
    def test_is_triple_double_exactly_10(self):
        """Triple-doble con exactamente 10 en cada categoria."""
        stats = PlayerGameStats(pts=10, reb=10, ast=10, stl=0, blk=0)
        assert stats.is_triple_double is True
    
    def test_is_not_triple_double_one_short(self):
        """No es triple-doble si una categoria tiene 9."""
        stats = PlayerGameStats(pts=10, reb=10, ast=9, stl=0, blk=0)
        assert stats.is_triple_double is False
    
    def test_is_quadruple_double(self):
        """Cuadruple-doble tambien cuenta como triple-doble."""
        stats = PlayerGameStats(pts=18, reb=16, ast=10, stl=12, blk=4)
        assert stats.is_triple_double is True
    
    def test_is_quintuple_double(self):
        """Quintuple-doble (nunca ha pasado en NBA)."""
        stats = PlayerGameStats(pts=10, reb=10, ast=10, stl=10, blk=10)
        assert stats.is_triple_double is True
    
    # =========================================================================
    # Tests de Doble-Doble
    # =========================================================================
    
    def test_is_double_double_pts_reb(self):
        """Doble-doble clasico: puntos y rebotes."""
        stats = PlayerGameStats(pts=20, reb=12, ast=5, stl=1, blk=0)
        assert stats.is_double_double is True
        assert stats.is_triple_double is False
    
    def test_is_double_double_pts_ast(self):
        """Doble-doble con puntos y asistencias."""
        stats = PlayerGameStats(pts=25, reb=5, ast=12, stl=2, blk=1)
        assert stats.is_double_double is True
    
    def test_is_double_double_reb_ast(self):
        """Doble-doble con rebotes y asistencias (raro, sin muchos puntos)."""
        stats = PlayerGameStats(pts=8, reb=15, ast=12, stl=2, blk=1)
        assert stats.is_double_double is True
    
    def test_is_double_double_pts_blk(self):
        """Doble-doble con puntos y bloqueos."""
        stats = PlayerGameStats(pts=22, reb=8, ast=3, stl=1, blk=10)
        assert stats.is_double_double is True
    
    def test_is_double_double_exactly_10(self):
        """Doble-doble con exactamente 10."""
        stats = PlayerGameStats(pts=10, reb=10, ast=0, stl=0, blk=0)
        assert stats.is_double_double is True
    
    def test_is_not_double_double(self):
        """No es doble-doble si ninguna combinacion llega a 10."""
        stats = PlayerGameStats(pts=9, reb=9, ast=9, stl=9, blk=9)
        assert stats.is_double_double is False
    
    def test_triple_double_implies_double_double(self):
        """Un triple-doble siempre es tambien un doble-doble."""
        stats = PlayerGameStats(pts=15, reb=12, ast=11, stl=3, blk=1)
        assert stats.is_triple_double is True
        assert stats.is_double_double is True
    
    # =========================================================================
    # Tests de Edge Cases
    # =========================================================================
    
    def test_all_zeros(self):
        """Jugador con 0 en todo (DNP o lesion inmediata)."""
        stats = PlayerGameStats(pts=0, reb=0, ast=0, stl=0, blk=0)
        assert stats.is_triple_double is False
        assert stats.is_double_double is False
    
    def test_single_category_high(self):
        """Alto rendimiento en una sola categoria."""
        stats = PlayerGameStats(pts=50, reb=5, ast=3, stl=1, blk=0)
        assert stats.is_double_double is False
        assert stats.is_triple_double is False


class TestGame:
    """Tests para el modelo Game."""
    
    # =========================================================================
    # Tests de get_winner()
    # =========================================================================
    
    def test_get_winner_home_wins(self):
        """El equipo local gana."""
        game = Game(
            id="0022300001",
            home_team_id=1,
            away_team_id=2,
            home_score=110,
            away_score=100
        )
        assert game.get_winner() == 1
    
    def test_get_winner_away_wins(self):
        """El equipo visitante gana."""
        game = Game(
            id="0022300001",
            home_team_id=1,
            away_team_id=2,
            home_score=95,
            away_score=105
        )
        assert game.get_winner() == 2
    
    def test_get_winner_tie(self):
        """Empate (no deberia pasar en NBA, pero el modelo lo soporta)."""
        game = Game(
            id="0022300001",
            home_team_id=1,
            away_team_id=2,
            home_score=100,
            away_score=100
        )
        assert game.get_winner() is None
    
    def test_get_winner_no_scores(self):
        """Partido sin marcadores (no finalizado)."""
        game = Game(
            id="0022300001",
            home_team_id=1,
            away_team_id=2,
            home_score=None,
            away_score=None
        )
        assert game.get_winner() is None
    
    def test_get_winner_partial_scores(self):
        """Partido con solo un marcador (datos incompletos)."""
        game = Game(
            id="0022300001",
            home_team_id=1,
            away_team_id=2,
            home_score=100,
            away_score=None
        )
        assert game.get_winner() is None
    
    def test_get_winner_blowout(self):
        """Victoria aplastante."""
        game = Game(
            id="0022300001",
            home_team_id=1,
            away_team_id=2,
            home_score=150,
            away_score=90
        )
        assert game.get_winner() == 1
    
    def test_get_winner_one_point(self):
        """Victoria por un punto."""
        game = Game(
            id="0022300001",
            home_team_id=1,
            away_team_id=2,
            home_score=100,
            away_score=99
        )
        assert game.get_winner() == 1
    
    # =========================================================================
    # Tests de is_finished
    # =========================================================================
    
    def test_is_finished_true(self):
        """Partido finalizado tiene ambos marcadores."""
        game = Game(
            id="0022300001",
            home_score=110,
            away_score=100
        )
        assert game.is_finished is True
    
    def test_is_finished_false_no_scores(self):
        """Partido no finalizado sin marcadores."""
        game = Game(id="0022300001", home_score=None, away_score=None)
        assert game.is_finished is False
    
    def test_is_finished_false_partial(self):
        """Partido con datos parciales no esta finalizado."""
        game = Game(id="0022300001", home_score=100, away_score=None)
        assert game.is_finished is False
    
    # =========================================================================
    # Tests de total_points
    # =========================================================================
    
    def test_total_points(self):
        """Total de puntos del partido."""
        game = Game(id="0022300001", home_score=120, away_score=115)
        assert game.total_points == 235
    
    def test_total_points_high_scoring(self):
        """Partido de alto puntaje."""
        game = Game(id="0022300001", home_score=150, away_score=145)
        assert game.total_points == 295
    
    def test_total_points_low_scoring(self):
        """Partido de bajo puntaje."""
        game = Game(id="0022300001", home_score=85, away_score=80)
        assert game.total_points == 165
    
    def test_total_points_none_when_not_finished(self):
        """Total es None si el partido no ha terminado."""
        game = Game(id="0022300001", home_score=None, away_score=None)
        assert game.total_points is None
    
    def test_total_points_none_when_partial(self):
        """Total es None con datos parciales."""
        game = Game(id="0022300001", home_score=100, away_score=None)
        assert game.total_points is None


class TestPlayer:
    """Tests para el modelo Player."""
    
    def test_is_active_with_none_to_year(self):
        """Jugador activo si to_year es None."""
        player = Player(id=1, full_name="Test Player", to_year=None)
        assert player.is_active is True
    
    def test_is_active_current_year(self):
        """Jugador activo si to_year es el ano actual."""
        from datetime import datetime
        current_year = datetime.now().year
        player = Player(id=1, full_name="Test Player", to_year=current_year)
        assert player.is_active is True
    
    def test_is_active_last_year(self):
        """Jugador activo si to_year es el ano pasado."""
        from datetime import datetime
        last_year = datetime.now().year - 1
        player = Player(id=1, full_name="Test Player", to_year=last_year)
        assert player.is_active is True
    
    def test_is_not_active_retired(self):
        """Jugador retirado hace mas de un ano."""
        from datetime import datetime
        old_year = datetime.now().year - 5
        player = Player(id=1, full_name="Test Player", to_year=old_year)
        assert player.is_active is False


class TestAnomalyScore:
    """Tests para el modelo AnomalyScore."""
    
    def test_is_any_anomaly_none(self):
        """No hay anomalia si todos los flags son False."""
        score = AnomalyScore(
            game_id="0022300001",
            player_id=1,
            reconstruction_loss=0.5,
            is_anomaly=False,
            player_season_is_anomaly=False,
            streak_is_anomaly=False
        )
        assert score.is_any_anomaly is False
    
    def test_is_any_anomaly_league(self):
        """Anomalia de liga detectada."""
        score = AnomalyScore(
            game_id="0022300001",
            player_id=1,
            reconstruction_loss=5.0,
            is_anomaly=True,
            player_season_is_anomaly=False,
            streak_is_anomaly=False
        )
        assert score.is_any_anomaly is True
    
    def test_is_any_anomaly_player_season(self):
        """Anomalia de temporada del jugador detectada."""
        score = AnomalyScore(
            game_id="0022300001",
            player_id=1,
            reconstruction_loss=0.5,
            is_anomaly=False,
            player_season_is_anomaly=True,
            streak_is_anomaly=False
        )
        assert score.is_any_anomaly is True
    
    def test_is_any_anomaly_streak(self):
        """Anomalia de racha detectada."""
        score = AnomalyScore(
            game_id="0022300001",
            player_id=1,
            reconstruction_loss=0.5,
            is_anomaly=False,
            player_season_is_anomaly=False,
            streak_is_anomaly=True
        )
        assert score.is_any_anomaly is True
    
    def test_is_any_anomaly_multiple(self):
        """Multiples anomalias detectadas."""
        score = AnomalyScore(
            game_id="0022300001",
            player_id=1,
            reconstruction_loss=5.0,
            is_anomaly=True,
            player_season_is_anomaly=True,
            streak_is_anomaly=True
        )
        assert score.is_any_anomaly is True


class TestTeam:
    """Tests para el modelo Team."""
    
    def test_team_repr(self):
        """Representacion string del equipo."""
        team = Team(id=1, full_name="Los Angeles Lakers", abbreviation="LAL")
        repr_str = repr(team)
        assert "Team" in repr_str
        assert "1" in repr_str
        assert "Los Angeles Lakers" in repr_str


class TestPlayerTeamSeason:
    """Tests para el modelo PlayerTeamSeason."""
    
    def test_repr(self):
        """Representacion string del registro."""
        pts = PlayerTeamSeason(
            player_id=2544,
            team_id=1610612747,
            season="2023-24"
        )
        repr_str = repr(pts)
        assert "PlayerTeamSeason" in repr_str
        assert "2544" in repr_str
        assert "2023-24" in repr_str


class TestTeamGameStats:
    """Tests para el modelo TeamGameStats."""
    
    def test_repr(self):
        """Representacion string del registro."""
        tgs = TeamGameStats(
            game_id="0022300001",
            team_id=1,
            total_pts=120,
            total_reb=45,
            total_ast=30,
            total_stl=8,
            total_blk=5,
            total_tov=12,
            total_pf=18,
            total_fgm=45,
            total_fga=90,
            total_fg3m=12,
            total_fg3a=30,
            total_ftm=18,
            total_fta=22
        )
        repr_str = repr(tgs)
        assert "TeamGameStats" in repr_str
        assert "0022300001" in repr_str
        assert "120" in repr_str
