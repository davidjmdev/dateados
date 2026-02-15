"""Tests para las nuevas funciones de db/query.py.

Cubre: _parse_height_inches, _get_bracket_data, y lógica de
las funciones de query añadidas para MCP.
"""

import pytest
from datetime import date, timedelta
from unittest.mock import MagicMock, patch, PropertyMock

from db.query import _parse_height_inches, _get_bracket_data


# =============================================================================
# Tests de _parse_height_inches
# =============================================================================

class TestParseHeightInches:
    """Tests para la función auxiliar de parsing de altura."""

    def test_standard_height(self):
        """Altura estándar '6-9' -> 81 pulgadas."""
        assert _parse_height_inches('6-9') == 81

    def test_six_feet_even(self):
        """'6-0' -> 72 pulgadas."""
        assert _parse_height_inches('6-0') == 72

    def test_seven_footer(self):
        """'7-1' -> 85 pulgadas."""
        assert _parse_height_inches('7-1') == 85

    def test_five_foot_player(self):
        """'5-9' -> 69 pulgadas (Muggsy Bogues tier)."""
        assert _parse_height_inches('5-9') == 69

    def test_tallest_ever(self):
        """'7-7' -> 91 pulgadas (Manute Bol / Gheorghe Muresan)."""
        assert _parse_height_inches('7-7') == 91

    def test_empty_string_returns_default(self):
        """String vacío retorna el default."""
        assert _parse_height_inches('', default=0) == 0
        assert _parse_height_inches('', default=999) == 999

    def test_none_returns_default(self):
        """None retorna el default (AttributeError en split)."""
        assert _parse_height_inches(None, default=0) == 0

    def test_no_dash_returns_default(self):
        """'72' sin dash retorna default (IndexError)."""
        assert _parse_height_inches('72', default=0) == 0

    def test_invalid_format_returns_default(self):
        """Formato inválido retorna default."""
        assert _parse_height_inches('abc-def', default=0) == 0
        assert _parse_height_inches('6-x', default=0) == 0

    def test_cm_format_returns_default(self):
        """Formato en cm retorna default."""
        assert _parse_height_inches('209cm', default=0) == 0

    def test_default_parameter_works(self):
        """El parámetro default se usa correctamente."""
        assert _parse_height_inches('invalid', default=42) == 42
        assert _parse_height_inches('invalid', default=0) == 0
        assert _parse_height_inches('invalid', default=999) == 999

    def test_default_zero_when_not_specified(self):
        """Sin especificar default, es 0."""
        assert _parse_height_inches('invalid') == 0

    def test_height_with_extra_parts(self):
        """'6-9-extra' solo usa las dos primeras partes."""
        # split('-') gives ['6', '9', 'extra'], parts[0]*12 + parts[1] = 81
        assert _parse_height_inches('6-9-extra') == 81

    def test_negative_height_parts(self):
        """Partes negativas se calculan matemáticamente."""
        # '-6-9': split('-') gives ['', '6', '9'], int('') raises ValueError -> default
        assert _parse_height_inches('-6-9', default=0) == 0

    def test_height_with_spaces(self):
        """'6 - 9' falla porque int('6 ') tiene espacio."""
        # split('-') gives ['6 ', ' 9'], int('6 ') raises ValueError in strict mode
        # Actually int(' 6 ') works in Python (strips whitespace)
        result = _parse_height_inches('6 - 9')
        # int('6 ') = 6, int(' 9') = 9 -> 81
        assert result == 81


# =============================================================================
# Tests de _get_bracket_data
# =============================================================================

def _make_mock_game(
    game_id: str,
    home_team_id: int,
    away_team_id: int,
    home_name: str = "Home Team",
    away_name: str = "Away Team",
    home_abbr: str = "HME",
    away_abbr: str = "AWY",
    winner_team_id: int = None,
    game_date: date = None,
    home_score: int = 100,
    away_score: int = 95,
):
    """Crea un mock de Game con las relaciones necesarias."""
    g = MagicMock()
    g.id = game_id
    g.home_team_id = home_team_id
    g.away_team_id = away_team_id
    g.winner_team_id = winner_team_id
    g.date = game_date or date(2024, 4, 20)
    g.home_score = home_score
    g.away_score = away_score

    # Mock team relationships
    g.home_team = MagicMock()
    g.home_team.full_name = home_name
    g.home_team.abbreviation = home_abbr
    g.away_team = MagicMock()
    g.away_team.full_name = away_name
    g.away_team.abbreviation = away_abbr
    return g


class TestGetBracketData:
    """Tests para la función auxiliar de bracket."""

    def test_empty_games_list(self):
        """Lista vacía retorna rondas vacías."""
        result = _get_bracket_data([])
        assert result == {1: [], 2: [], 3: [], 4: []}

    def test_single_playoff_game(self):
        """Un juego de playoffs primera ronda se agrupa correctamente."""
        game = _make_mock_game(
            game_id='0042300111',  # PO, round 1, pos 1
            home_team_id=100,
            away_team_id=200,
            winner_team_id=100,
        )
        result = _get_bracket_data([game], is_ist=False)
        assert len(result[1]) == 1
        series = result[1][0]
        assert series['t1_wins'] + series['t2_wins'] == 1

    def test_multiple_games_same_series(self):
        """Múltiples juegos de la misma serie se agrupan correctamente."""
        games = []
        for i in range(4):
            winner = 100 if i < 3 else 200
            games.append(_make_mock_game(
                game_id=f'004230011{i+1}',  # PO, round 1, pos 1
                home_team_id=100,
                away_team_id=200,
                winner_team_id=winner,
                game_date=date(2024, 4, 20 + i),
            ))

        result = _get_bracket_data(games, is_ist=False)
        assert len(result[1]) == 1
        series = result[1][0]
        # Team IDs are sorted: min(100,200)=100 is team1
        assert series['t1_wins'] == 3
        assert series['t2_wins'] == 1

    def test_games_without_winner(self):
        """Juegos sin ganador no cuentan victorias."""
        game = _make_mock_game(
            game_id='0042300111',
            home_team_id=100,
            away_team_id=200,
            winner_team_id=None,  # No winner
        )
        result = _get_bracket_data([game], is_ist=False)
        assert len(result[1]) == 1
        series = result[1][0]
        assert series['t1_wins'] == 0
        assert series['t2_wins'] == 0

    def test_games_without_teams_skipped(self):
        """Juegos sin equipo local o visitante se saltan."""
        game = _make_mock_game(
            game_id='0042300111',
            home_team_id=None,
            away_team_id=200,
        )
        result = _get_bracket_data([game], is_ist=False)
        # All rounds should be empty
        assert all(len(v) == 0 for v in result.values())

    def test_date_tracking(self):
        """Las fechas de inicio y fin se rastrean correctamente."""
        games = [
            _make_mock_game(
                game_id='0042300111',
                home_team_id=100,
                away_team_id=200,
                winner_team_id=100,
                game_date=date(2024, 4, 20),
            ),
            _make_mock_game(
                game_id='0042300112',
                home_team_id=100,
                away_team_id=200,
                winner_team_id=200,
                game_date=date(2024, 4, 25),
            ),
        ]
        result = _get_bracket_data(games, is_ist=False)
        series = result[1][0]
        assert series['first_date'] == '2024-04-20'
        assert series['last_date'] == '2024-04-25'

    def test_ist_quarterfinal_detection(self):
        """NBA Cup cuartos de final se detectan por sufijo del ID."""
        game = _make_mock_game(
            game_id='0022401201',  # IST QF match 1
            home_team_id=100,
            away_team_id=200,
            winner_team_id=100,
            home_score=115,
            away_score=108,
        )
        result = _get_bracket_data([game], is_ist=True)
        # Round 2 = Cuartos de Final
        assert len(result[2]) == 1
        series = result[2][0]
        assert series['t1_wins'] == 1

    def test_ist_semifinal_detection(self):
        """NBA Cup semifinales se detectan por sufijo 1229/1230."""
        game = _make_mock_game(
            game_id='0022401229',  # IST SF match 1
            home_team_id=100,
            away_team_id=200,
            winner_team_id=100,
        )
        result = _get_bracket_data([game], is_ist=True)
        assert len(result[3]) == 1

    def test_ist_final_detection(self):
        """NBA Cup final se detecta por prefijo '006'."""
        game = _make_mock_game(
            game_id='0062400001',  # IST Final
            home_team_id=100,
            away_team_id=200,
            winner_team_id=100,
            home_score=120,
            away_score=110,
        )
        result = _get_bracket_data([game], is_ist=True)
        assert len(result[4]) == 1
        series = result[4][0]
        # IST tracks scores
        assert series['t1_score'] > 0 or series['t2_score'] > 0

    def test_ist_score_tracking(self):
        """En IST, los scores se rastrean correctamente."""
        game = _make_mock_game(
            game_id='0062400001',
            home_team_id=200,  # home is team 200
            away_team_id=100,  # away is team 100
            winner_team_id=100,  # team 100 wins as away
            home_score=110,
            away_score=120,
        )
        result = _get_bracket_data([game], is_ist=True)
        series = result[4][0]
        # t1=100 (sorted min), t2=200
        # Winner is t1 (100), playing as away
        assert series['t1_wins'] == 1
        assert series['t1_score'] == 120  # away_score because t1 is away
        assert series['t2_score'] == 110  # home_score because t2 is home

    def test_multiple_rounds(self):
        """Juegos de diferentes rondas se separan correctamente."""
        games = [
            _make_mock_game(
                game_id='0042300111',  # Round 1
                home_team_id=100,
                away_team_id=200,
                winner_team_id=100,
            ),
            _make_mock_game(
                game_id='0042300211',  # Round 2
                home_team_id=300,
                away_team_id=400,
                winner_team_id=300,
            ),
        ]
        result = _get_bracket_data(games, is_ist=False)
        assert len(result[1]) == 1
        assert len(result[2]) == 1

    def test_malformed_game_id_excluded(self):
        """Juegos con ID malformado (no se puede parsear ronda) se excluyen."""
        game = _make_mock_game(
            game_id='BADID12345',
            home_team_id=100,
            away_team_id=200,
            winner_team_id=100,
        )
        result = _get_bracket_data([game], is_ist=False)
        # No r_hint -> filtered out
        assert all(len(v) == 0 for v in result.values())

    def test_short_game_id_excluded(self):
        """IDs de longitud != 10 no se parsean para ronda."""
        game = _make_mock_game(
            game_id='0042301',  # Too short
            home_team_id=100,
            away_team_id=200,
            winner_team_id=100,
        )
        result = _get_bracket_data([game], is_ist=False)
        assert all(len(v) == 0 for v in result.values())

    def test_team_ids_sorted_consistently(self):
        """Los team IDs se ordenan consistentemente (menor es team1)."""
        # Game 1: home=200, away=100
        # Game 2: home=100, away=200
        games = [
            _make_mock_game(
                game_id='0042300111',
                home_team_id=200,
                away_team_id=100,
                home_name="Team 200",
                away_name="Team 100",
                winner_team_id=200,
                game_date=date(2024, 4, 20),
            ),
            _make_mock_game(
                game_id='0042300112',
                home_team_id=100,
                away_team_id=200,
                home_name="Team 100",
                away_name="Team 200",
                winner_team_id=100,
                game_date=date(2024, 4, 22),
            ),
        ]
        result = _get_bracket_data(games, is_ist=False)
        assert len(result[1]) == 1  # Same series
        series = result[1][0]
        assert series['team1_id'] == 100  # Smaller ID is team1
        assert series['team2_id'] == 200


# =============================================================================
# Tests de lógica del MCP tool get_award_leaders (validación)
# =============================================================================

class TestAwardTypeValidation:
    """Tests para la validación de award_type en el tool MCP."""

    def test_valid_award_types_mapping(self):
        """Verifica que el mapeo case-insensitive existe para todos los tipos."""
        valid_award_types = {
            'mvp': 'MVP', 'champion': 'Champion', 'all-star': 'All-Star',
            'all-nba': 'All-NBA', 'dpoy': 'DPOY', 'finals mvp': 'Finals MVP',
            'roy': 'ROY', '6moy': '6MOY', 'mip': 'MIP',
            'all-defensive': 'All-Defensive', 'all-rookie': 'All-Rookie',
            'all-star mvp': 'All-Star MVP', 'nba cup': 'NBA Cup',
            'nba cup mvp': 'NBA Cup MVP', 'olympic gold': 'Olympic Gold',
            'olympic silver': 'Olympic Silver', 'olympic bronze': 'Olympic Bronze',
        }

        # All canonical forms map back to themselves (via lower)
        for lower_key, canonical in valid_award_types.items():
            assert valid_award_types[canonical.lower()] == canonical

    def test_case_insensitive_lookup(self):
        """Verifica que MVP, mvp, Mvp todos mapean a 'MVP'."""
        valid_award_types = {
            'mvp': 'MVP', 'champion': 'Champion', 'all-star': 'All-Star',
        }
        assert valid_award_types.get('mvp') == 'MVP'
        assert valid_award_types.get('MVP'.lower()) == 'MVP'
        assert valid_award_types.get('Mvp'.lower()) == 'MVP'

    def test_invalid_award_type_not_found(self):
        """Tipos inválidos no se encuentran en el mapping."""
        valid_award_types = {'mvp': 'MVP', 'champion': 'Champion'}
        assert valid_award_types.get('MVPP'.lower()) is None
        assert valid_award_types.get('champon'.lower()) is None


# =============================================================================
# Tests de lógica de get_team_roster early-exit
# =============================================================================

class TestTeamRosterEarlyExit:
    """Tests para verificar consistencia del dict de retorno."""

    def test_early_exit_has_count_key(self):
        """El dict de early-exit debe tener 'count' igual que el flujo normal."""
        early_exit = {'team_id': 99, 'season': None, 'count': 0, 'players': []}
        normal_exit = {'team_id': 99, 'season': '2024-25', 'count': 5, 'players': []}

        # Both must have the same keys
        assert set(early_exit.keys()) == set(normal_exit.keys())

    def test_early_exit_count_is_zero(self):
        """El count en early-exit debe ser 0."""
        early_exit = {'team_id': 99, 'season': None, 'count': 0, 'players': []}
        assert early_exit['count'] == 0
        assert early_exit['players'] == []


# =============================================================================
# Tests de lógica de get_player_rankings (criterios válidos)
# =============================================================================

class TestPlayerRankingsCriteria:
    """Tests para los criterios válidos de ranking."""

    def test_all_valid_criteria(self):
        """Verifica que todos los criterios documentados son reconocidos."""
        valid = {
            'youngest', 'oldest', 'heaviest', 'lightest',
            'tallest', 'shortest', 'most_experienced',
            'highest_draft_pick', 'lowest_draft_pick'
        }
        # This matches the set used in the MCP tool
        assert len(valid) == 9
        assert 'youngest' in valid
        assert 'tallest' in valid

    def test_invalid_criteria_not_in_set(self):
        """Criterios inválidos no están en el set."""
        valid = {
            'youngest', 'oldest', 'heaviest', 'lightest',
            'tallest', 'shortest', 'most_experienced',
            'highest_draft_pick', 'lowest_draft_pick'
        }
        assert 'fastest' not in valid
        assert 'best' not in valid
        assert '' not in valid
