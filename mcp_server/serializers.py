"""Utilidades de serialización para convertir objetos ORM y tipos complejos a JSON.

Centraliza la conversión de modelos SQLAlchemy, dates, timedeltas e Intervals
para que los tools del MCP devuelvan strings JSON válidos.
"""

import json
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from db.models import Team, Player, Game, PlayerGameStats


class DateadosEncoder(json.JSONEncoder):
    """Encoder JSON que maneja tipos SQLAlchemy y Python comunes."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if isinstance(obj, timedelta):
            total_seconds = int(obj.total_seconds())
            minutes = total_seconds // 60
            seconds = total_seconds % 60
            return f"{minutes}:{seconds:02d}"
        return super().default(obj)


def to_json(data: Any) -> str:
    """Convierte datos a string JSON usando el encoder personalizado."""
    return json.dumps(data, cls=DateadosEncoder, ensure_ascii=False)


def serialize_team(team: Team) -> Dict[str, Any]:
    """Serializa un objeto Team a dict."""
    return {
        'id': team.id,
        'full_name': team.full_name,
        'abbreviation': team.abbreviation,
        'city': team.city,
        'state': team.state,
        'nickname': team.nickname,
        'year_founded': team.year_founded,
        'conference': team.conference,
        'division': team.division,
    }


def serialize_player(player: Player) -> Dict[str, Any]:
    """Serializa un objeto Player a dict."""
    return {
        'id': player.id,
        'full_name': player.full_name,
        'position': player.position,
        'height': player.height,
        'weight': player.weight,
        'country': player.country,
        'jersey': player.jersey,
        'is_active': player.is_active,
        'birthdate': player.birthdate.isoformat() if player.birthdate else None,
        'season_exp': player.season_exp,
        'from_year': player.from_year,
        'to_year': player.to_year,
        'draft_year': player.draft_year,
        'draft_round': player.draft_round,
        'draft_number': player.draft_number,
        'school': player.school,
    }


def serialize_game(game: Game) -> Dict[str, Any]:
    """Serializa un objeto Game a dict."""
    return {
        'id': game.id,
        'date': game.date.isoformat() if game.date else None,
        'season': game.season,
        'home_team': game.home_team.full_name if game.home_team else f"Team {game.home_team_id}",
        'away_team': game.away_team.full_name if game.away_team else f"Team {game.away_team_id}",
        'home_team_abbr': game.home_team.abbreviation if game.home_team else None,
        'away_team_abbr': game.away_team.abbreviation if game.away_team else None,
        'home_team_id': game.home_team_id,
        'away_team_id': game.away_team_id,
        'home_score': game.home_score,
        'away_score': game.away_score,
        'status': game.status,
        'rs': game.rs,
        'po': game.po,
        'pi': game.pi,
        'ist': game.ist,
    }


def serialize_player_game_stats(stats: PlayerGameStats) -> Dict[str, Any]:
    """Serializa un objeto PlayerGameStats a dict."""
    return {
        'id': stats.id,
        'game_id': stats.game_id,
        'player_id': stats.player_id,
        'player_name': stats.player.full_name if stats.player else None,
        'team_id': stats.team_id,
        'team_abbr': stats.team.abbreviation if stats.team else None,
        'game_date': stats.game.date.isoformat() if stats.game and stats.game.date else None,
        'min': stats.minutes_formatted,
        'pts': stats.pts,
        'reb': stats.reb,
        'ast': stats.ast,
        'stl': stats.stl,
        'blk': stats.blk,
        'tov': stats.tov,
        'pf': stats.pf,
        'plus_minus': stats.plus_minus,
        'fgm': stats.fgm,
        'fga': stats.fga,
        'fg_pct': stats.fg_pct,
        'fg3m': stats.fg3m,
        'fg3a': stats.fg3a,
        'fg3_pct': stats.fg3_pct,
        'ftm': stats.ftm,
        'fta': stats.fta,
        'ft_pct': stats.ft_pct,
    }


def round_floats(data: Any, decimals: int = 2) -> Any:
    """Redondea recursivamente todos los floats en un dict/list."""
    if isinstance(data, dict):
        return {k: round_floats(v, decimals) for k, v in data.items()}
    if isinstance(data, list):
        return [round_floats(item, decimals) for item in data]
    if isinstance(data, float):
        return round(data, decimals)
    return data
