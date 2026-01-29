"""Utilidades para consultar información de la base de datos NBA.

Este módulo proporciona funciones de alto nivel para consultar datos
de manera fácil y eficiente.
"""

import sys
import math
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Union
from sqlalchemy import func, desc, asc, and_, or_
from sqlalchemy.orm import Session, joinedload

# Agregar el directorio raíz al PYTHONPATH
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from db.connection import get_session
from db.models import (
    Team, Player, Game, PlayerGameStats, TeamGameStats,
    PlayerTeamSeason, PlayerAward
)


def get_current_teammates(player_id: int, session: Optional[Session] = None) -> List[Dict[str, Any]]:
    """Obtiene compañeros del equipo actual (última temporada/equipo registrada)."""
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
        
    try:
        # Buscar el último registro de temporada del jugador
        latest_ts = session.query(PlayerTeamSeason)\
            .filter(PlayerTeamSeason.player_id == player_id)\
            .order_by(desc(PlayerTeamSeason.season), desc(PlayerTeamSeason.end_date))\
            .first()
            
        if not latest_ts: 
            return []
            
        # Buscar compañeros en el mismo equipo y temporada
        teammates_ts = session.query(PlayerTeamSeason)\
            .options(joinedload(PlayerTeamSeason.player))\
            .filter(
                PlayerTeamSeason.team_id == latest_ts.team_id,
                PlayerTeamSeason.season == latest_ts.season,
                PlayerTeamSeason.type == latest_ts.type,
                PlayerTeamSeason.player_id != player_id
            ).all()
            
        return [
            {
                'id': ts.player.id, 
                'full_name': ts.player.full_name, 
                'position': ts.player.position
            }
            for ts in teammates_ts if ts.player
        ]
    finally:
        if own_session: 
            session.close()


def get_historical_teammates(player_id: int, session: Optional[Session] = None) -> List[Dict[str, Any]]:
    """Obtiene todos los compañeros históricos del jugador."""
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
        
    try:
        player_seasons = session.query(PlayerTeamSeason).filter(PlayerTeamSeason.player_id == player_id).all()
        if not player_seasons: 
            return []
            
        teammates_data = {}
        for ps in player_seasons:
            compañeros = session.query(PlayerTeamSeason)\
                .options(joinedload(PlayerTeamSeason.player), joinedload(PlayerTeamSeason.team))\
                .filter(
                    PlayerTeamSeason.team_id == ps.team_id,
                    PlayerTeamSeason.season == ps.season,
                    PlayerTeamSeason.player_id != player_id
                ).all()
                
            for ts in compañeros:
                if not ts.player: 
                    continue
                p_id = ts.player.id
                if p_id not in teammates_data:
                    teammates_data[p_id] = {
                        'id': p_id, 
                        'full_name': ts.player.full_name, 
                        'position': ts.player.position,
                        'seasons_together': set(), 
                        'teams_together': {},
                    }
                d = teammates_data[p_id]
                d['seasons_together'].add(ts.season)
                if ts.team_id not in d['teams_together']:
                    d['teams_together'][ts.team_id] = {
                        'id': ts.team_id, 
                        'name': ts.team.full_name, 
                        'abbreviation': ts.team.abbreviation
                    }
        
        result = []
        for tid, data in teammates_data.items():
            result.append({
                'id': data['id'], 
                'full_name': data['full_name'], 
                'position': data['position'],
                'seasons_together': sorted(list(data['seasons_together']), reverse=True),
                'teams_together': list(data['teams_together'].values()),
                'total_seasons': len(data['seasons_together'])
            })
        return sorted(result, key=lambda x: (-x['total_seasons'], x['full_name']))
    finally:
        if own_session: 
            session.close()


def get_database_stats(session: Optional[Session] = None) -> Dict[str, int]:
    """Retorna estadísticas generales de la base de datos.
    
    Optimizado para realizar una única consulta a la base de datos.
    """
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        # Usamos subconsultas para obtener todos los conteos en un solo viaje a la BD
        stats = session.query(
            session.query(func.count(Team.id)).label('teams'),
            session.query(func.count(Player.id)).label('players'),
            session.query(func.count(Game.id)).label('games'),
            session.query(func.count(PlayerGameStats.id)).label('player_game_stats'),
            session.query(func.count(TeamGameStats.id)).label('team_game_stats'),
            session.query(func.count(PlayerTeamSeason.id)).label('player_team_seasons'),
            session.query(func.count(PlayerAward.id)).label('player_awards')
        ).first()
        
        return {
            'teams': stats.teams,
            'players': stats.players,
            'games': stats.games,
            'player_game_stats': stats.player_game_stats,
            'team_game_stats': stats.team_game_stats,
            'player_team_seasons': stats.player_team_seasons,
            'player_awards': stats.player_awards,
        }
    finally:
        if own_session: 
            session.close()


def get_teams(conference: Optional[str] = None, division: Optional[str] = None, session: Optional[Session] = None) -> List[Team]:
    """Obtiene una lista de equipos filtrada."""
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        query = session.query(Team)
        if conference: 
            query = query.filter(Team.conference == conference)
        if division: 
            query = query.filter(Team.division == division)
        return query.order_by(Team.full_name).all()
    finally:
        if own_session: 
            session.close()


def get_players(
    name: Optional[str] = None, 
    position: Optional[str] = None, 
    active_only: bool = False, 
    team_id: Optional[int] = None,
    season: Optional[str] = None,
    session: Optional[Session] = None
) -> List[Player]:
    """Obtiene jugadores con filtros opcionales."""
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        query = session.query(Player)
        
        if name: 
            query = query.filter(Player.full_name.ilike(f"%{name}%"))
        if position: 
            query = query.filter(Player.position.ilike(f"%{position}%"))
        if active_only:
            query = query.filter(Player.is_active == True)
            
        if team_id or season:
            query = query.join(PlayerTeamSeason)
            if team_id:
                query = query.filter(PlayerTeamSeason.team_id == team_id)
            if season:
                query = query.filter(PlayerTeamSeason.season == season)
                
        return query.order_by(Player.full_name).distinct().all()
    finally:
        if own_session: 
            session.close()


def get_games(
    season: Optional[str] = None, 
    team_id: Optional[int] = None, 
    start_date: Optional[date] = None, 
    end_date: Optional[date] = None, 
    finished_only: bool = False, 
    game_type: Optional[str] = None,
    limit: Optional[int] = None, 
    session: Optional[Session] = None
) -> List[Game]:
    """Obtiene partidos con filtros opcionales."""
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        query = session.query(Game).options(joinedload(Game.home_team), joinedload(Game.away_team))
        
        if season: 
            query = query.filter(Game.season == season)
        if team_id: 
            query = query.filter(or_(Game.home_team_id == team_id, Game.away_team_id == team_id))
        if start_date: 
            query = query.filter(Game.date >= start_date)
        if end_date: 
            query = query.filter(Game.date <= end_date)
        if finished_only: 
            query = query.filter(Game.status == 3)
            
        if game_type:
            if game_type.lower() in ['rs', 'regular', 'regular season']:
                query = query.filter(Game.rs == True)
            elif game_type.lower() in ['po', 'playoffs']:
                query = query.filter(Game.po == True)
            elif game_type.lower() in ['pi', 'playin']:
                query = query.filter(Game.pi == True)
            elif game_type.lower() in ['ist', 'cup', 'nba cup']:
                query = query.filter(Game.ist == True)
                
        query = query.order_by(desc(Game.date))
        if limit: 
            query = query.limit(limit)
        return query.all()
    finally:
        if own_session: 
            session.close()


def get_player_stats(
    player_id: Optional[int] = None, 
    game_id: Optional[str] = None, 
    team_id: Optional[int] = None, 
    season: Optional[str] = None, 
    min_points: Optional[int] = None, 
    limit: Optional[int] = None, 
    order_by_date: bool = True, 
    session: Optional[Session] = None
) -> List[PlayerGameStats]:
    """Obtiene estadísticas de jugadores con filtros opcionales."""
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        query = session.query(PlayerGameStats).options(
            joinedload(PlayerGameStats.player), 
            joinedload(PlayerGameStats.team), 
            joinedload(PlayerGameStats.game)
        )
        
        if player_id: query = query.filter(PlayerGameStats.player_id == player_id)
        if game_id: query = query.filter(PlayerGameStats.game_id == game_id)
        if team_id: query = query.filter(PlayerGameStats.team_id == team_id)
        
        if season or order_by_date: 
            query = query.join(Game)
            
        if season: 
            query = query.filter(Game.season == season)
        if min_points: 
            query = query.filter(PlayerGameStats.pts >= min_points)
            
        if order_by_date: 
            query = query.order_by(desc(Game.date))
        else: 
            query = query.order_by(desc(PlayerGameStats.pts))
            
        if limit: 
            query = query.limit(limit)
        return query.all()
    finally:
        if own_session: 
            session.close()


def get_player_season_averages(player_id: int, season: str, session: Optional[Session] = None) -> Optional[Dict[str, Any]]:
    """Obtiene promedios de un jugador en una temporada (Regular Season por defecto)."""
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        # Intentar obtener de PlayerTeamSeason (ya calculado)
        pts_record = session.query(PlayerTeamSeason).filter(
            PlayerTeamSeason.player_id == player_id,
            PlayerTeamSeason.season == season,
            PlayerTeamSeason.type == 'Regular Season'
        ).first()
        
        if pts_record and pts_record.games_played and pts_record.games_played > 0:
            n = pts_record.games_played
            total_mins = pts_record.minutes.total_seconds() / 60 if pts_record.minutes is not None else 0
            
            return {
                'player_id': player_id, 
                'season': season, 
                'games': n,
                'pts': (pts_record.pts or 0) / n, 
                'reb': (pts_record.reb or 0) / n,
                'ast': (pts_record.ast or 0) / n, 
                'stl': (pts_record.stl or 0) / n,
                'blk': (pts_record.blk or 0) / n, 
                'tov': (pts_record.tov or 0) / n,
                'mpg': total_mins / n,
                'fg_pct': (pts_record.fgm or 0) / (pts_record.fga or 1) if pts_record.fga else 0,
                'fg3_pct': (pts_record.fg3m or 0) / (pts_record.fg3a or 1) if pts_record.fg3a else 0,
                'ft_pct': (pts_record.ftm or 0) / (pts_record.fta or 1) if pts_record.fta else 0,
                'plus_minus': (pts_record.plus_minus or 0) / n,
            }
            
        # Fallback: calcular desde player_game_stats
        stats = session.query(PlayerGameStats).join(Game).filter(
            PlayerGameStats.player_id == player_id, 
            Game.season == season, 
            Game.rs == True
        ).all()
        
        if not stats: 
            return None
            
        played_stats = [s for s in stats if s.min is not None and s.min.total_seconds() > 0]
        n_games = len(played_stats)
        divisor = n_games if n_games > 0 else len(stats)
        
        total_fgm = sum(s.fgm or 0 for s in stats)
        total_fga = sum(s.fga or 0 for s in stats)
        total_fg3m = sum(s.fg3m or 0 for s in stats)
        total_fg3a = sum(s.fg3a or 0 for s in stats)
        total_ftm = sum(s.ftm or 0 for s in stats)
        total_fta = sum(s.fta or 0 for s in stats)
        total_mins = sum(s.min.total_seconds() if s.min is not None else 0 for s in stats) / 60
        
        return {
            'player_id': player_id, 
            'season': season, 
            'games': len(stats),
            'pts': sum(s.pts or 0 for s in stats) / divisor, 
            'reb': sum(s.reb or 0 for s in stats) / divisor,
            'ast': sum(s.ast or 0 for s in stats) / divisor, 
            'stl': sum(s.stl or 0 for s in stats) / divisor,
            'blk': sum(s.blk or 0 for s in stats) / divisor, 
            'tov': sum(s.tov or 0 for s in stats) / divisor,
            'mpg': total_mins / divisor,
            'fg_pct': total_fgm / total_fga if total_fga > 0 else 0.0,
            'fg3_pct': total_fg3m / total_fg3a if total_fg3a > 0 else 0.0,
            'ft_pct': total_ftm / total_fta if total_fta > 0 else 0.0,
            'plus_minus': sum(s.plus_minus or 0 for s in stats) / divisor,
        }
    finally:
        if own_session: 
            session.close()


def get_top_players(stat: str = 'pts', season: Optional[str] = None, limit: int = 10, session: Optional[Session] = None) -> List[Dict[str, Any]]:
    """Obtiene los mejores jugadores por una estadística."""
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        avg_stat = func.avg(getattr(PlayerGameStats, stat)).label('avg_stat')
        games_count = func.count(PlayerGameStats.id).label('games')
        
        query = session.query(
            Player.id, 
            Player.full_name, 
            avg_stat,
            games_count
        ).join(PlayerGameStats, Player.id == PlayerGameStats.player_id)
        
        if season: 
            query = query.join(Game, PlayerGameStats.game_id == Game.id).filter(Game.season == season)
            
        query = query.group_by(Player.id, Player.full_name).having(games_count >= 5)
        query = query.order_by(desc('avg_stat')).limit(limit)
        
        results = query.all()
        return [
            {
                'id': r.id, 
                'full_name': r.full_name, 
                'value': float(r.avg_stat) if r.avg_stat is not None else 0.0,
                'games': r.games
            } 
            for r in results
        ]
    finally:
        if own_session: 
            session.close()


def get_team_record(team_id: int, season: Optional[str] = None, session: Optional[Session] = None) -> Dict[str, Any]:
    """Obtiene el récord de un equipo y su posición en la conferencia."""
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        # 1. Obtener info del equipo (conferencia)
        team = session.query(Team).filter(Team.id == team_id).first()
        if not team:
            return {}
        
        conference = team.conference
        
        # 2. Obtener estadísticas de victorias y derrotas usando agregación SQL
        # Es mucho más eficiente que cargar todos los partidos en memoria
        from sqlalchemy import case
        
        # Filtros base
        base_filters = [Game.status == 3, Game.rs == True]
        if season:
            base_filters.append(Game.season == season)
            
        # Victorias por equipo
        wins_stats = dict(
            session.query(Game.winner_team_id, func.count(Game.id))
            .filter(*base_filters)
            .filter(Game.winner_team_id.isnot(None))
            .group_by(Game.winner_team_id).all()
        )
        
        # Derrotas por equipo (el que no ganó en un partido finalizado)
        losses_stats = dict(
            session.query(
                case(
                    (Game.winner_team_id == Game.home_team_id, Game.away_team_id),
                    else_=Game.home_team_id
                ).label('loser_id'),
                func.count(Game.id)
            )
            .filter(*base_filters)
            .filter(Game.winner_team_id.isnot(None))
            .group_by('loser_id').all()
        )
        
        # 3. Obtener todos los equipos de la misma conferencia para calcular el ranking
        conf_teams = session.query(Team.id).filter(Team.conference == conference).all()
        conf_team_ids = [t.id for t in conf_teams]
        
        # 4. Construir tabla de clasificación de la conferencia
        standings = []
        for tid in conf_team_ids:
            w = wins_stats.get(tid, 0)
            l = losses_stats.get(tid, 0)
            pct = w / (w + l) if (w + l) > 0 else 0.0
            standings.append({'team_id': tid, 'wins': w, 'losses': l, 'pct': pct})
            
        # 5. Ordenar por PCT descendente
        standings.sort(key=lambda x: x['pct'], reverse=True)
        
        # 6. Extraer datos del equipo objetivo
        target_rank = None
        for i, s in enumerate(standings):
            if s['team_id'] == team_id:
                target_rank = i + 1
                break
        
        team_stat = next((s for s in standings if s['team_id'] == team_id), {'wins': 0, 'losses': 0})
        wins = team_stat['wins']
        losses = team_stat['losses']
        total = wins + losses
        
        return {
            'team_id': team_id,
            'season': season,
            'wins': wins,
            'losses': losses,
            'total': total,
            'win_percentage': wins / total if total > 0 else 0.0,
            'conf_rank': target_rank
        }
    finally:
        if own_session: 
            session.close()


def get_game_details(game_id: str, session: Optional[Session] = None) -> Optional[Dict[str, Any]]:
    """Obtiene detalles completos de un partido."""
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        game = session.query(Game).options(
            joinedload(Game.home_team), 
            joinedload(Game.away_team)
        ).filter(Game.id == game_id).first()
        
        if not game: 
            return None
            
        player_stats = session.query(PlayerGameStats).options(
            joinedload(PlayerGameStats.player), 
            joinedload(PlayerGameStats.team)
        ).filter(PlayerGameStats.game_id == game_id).order_by(desc(PlayerGameStats.min), desc(PlayerGameStats.pts)).all()
        
        team_stats = session.query(TeamGameStats).options(
            joinedload(TeamGameStats.team)
        ).filter(TeamGameStats.game_id == game_id).all()
        
        return {
            'game': {
                'id': str(game.id), 
                'date': game.date.isoformat() if game.date else None, 
                'season': str(game.season), 
                'home_team': str(game.home_team.full_name) if game.home_team else f"Team {game.home_team_id}", 
                'away_team': str(game.away_team.full_name) if game.away_team else f"Team {game.away_team_id}", 
                'home_team_abbr': str(game.home_team.abbreviation) if game.home_team else "T1",
                'away_team_abbr': str(game.away_team.abbreviation) if game.away_team else "T2",
                'home_team_id': int(game.home_team_id), 
                'away_team_id': int(game.away_team_id), 
                'home_score': int(game.home_score) if game.home_score is not None else 0, 
                'away_score': int(game.away_score) if game.away_score is not None else 0, 
                'quarter_scores': game.quarter_scores,
                'rs': bool(game.rs), 'po': bool(game.po), 'pi': bool(game.pi), 'ist': bool(game.ist)
            },
            'player_stats': [
                {
                    'player_id': s.player_id, 
                    'player': s.player.full_name if s.player else f"Player {s.player_id}", 
                    'team': s.team.abbreviation if s.team else f"ID {s.team_id}", 
                    'team_id': s.team_id, 
                    'min': s.minutes_formatted, 
                    'pts': s.pts, 'reb': s.reb, 'ast': s.ast, 
                    'stl': s.stl, 'blk': s.blk, 'tov': s.tov, 'pf': s.pf, 
                    'fgm': s.fgm, 'fga': s.fga, 'fg3m': s.fg3m, 'fg3a': s.fg3a, 
                    'ftm': s.ftm, 'fta': s.fta, 'plus_minus': s.plus_minus
                } for s in player_stats
            ],
            'team_stats': [
                {
                    'team': ts.team.abbreviation if ts.team else f"ID {ts.team_id}", 
                    'team_id': ts.team_id,
                    'total_pts': ts.total_pts, 'total_reb': ts.total_reb, 
                    'total_ast': ts.total_ast, 'fg_pct': ts.fg_pct,
                    'total_stl': ts.total_stl, 'total_blk': ts.total_blk,
                    'total_tov': ts.total_tov, 'total_pf': ts.total_pf,
                    'total_fgm': ts.total_fgm, 'total_fga': ts.total_fga,
                    'total_fg3m': ts.total_fg3m, 'total_fg3a': ts.total_fg3a,
                    'total_ftm': ts.total_ftm, 'total_fta': ts.total_fta
                } for ts in team_stats
            ]
        }
    finally:
        if own_session: 
            session.close()


def search_games_by_score(min_total: Optional[int] = None, max_total: Optional[int] = None, season: Optional[str] = None, limit: int = 10, session: Optional[Session] = None) -> List[Game]:
    """Busca partidos por rango de puntos totales."""
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        query = session.query(Game).filter(Game.status == 3)
        if season: 
            query = query.filter(Game.season == season)
        if min_total: 
            query = query.filter((Game.home_score + Game.away_score) >= min_total)
        if max_total: 
            query = query.filter((Game.home_score + Game.away_score) <= max_total)
        return query.order_by(desc(Game.home_score + Game.away_score)).limit(limit).all()
    finally:
        if own_session: 
            session.close()


def get_player_career_stats(player_id: int, session: Optional[Session] = None) -> Dict[str, Any]:
    """Obtiene las estadísticas de carrera de un jugador."""
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
        
    try:
        # 1. Periodos recientes (últimos 100 partidos)
        recent_stats = session.query(PlayerGameStats)\
            .join(Game)\
            .options(joinedload(PlayerGameStats.game), joinedload(PlayerGameStats.team))\
            .filter(PlayerGameStats.player_id == player_id)\
            .order_by(desc(Game.date)).limit(100).all()
            
        today = date.today()
        ref_date = recent_stats[0].game.date if recent_stats and recent_stats[0].game.date > today else today
        
        last_7_stats = [s for s in recent_stats if s.game.date >= ref_date - timedelta(days=7)]
        last_month_stats = [s for s in recent_stats if s.game.date >= ref_date - timedelta(days=30)]

        def calculate_averages(stats_list: List[PlayerGameStats]) -> Optional[Dict]:
            if not stats_list: return None
            played_stats = [s for s in stats_list if s.min and s.min.total_seconds() > 0]
            n = len(played_stats)
            divisor = n if n > 0 else len(stats_list) if len(stats_list) > 0 else 1
            
            total_fgm = float(sum(s.fgm for s in stats_list))
            total_fga = float(sum(s.fga for s in stats_list))
            total_fg3m = float(sum(s.fg3m for s in stats_list))
            total_fg3a = float(sum(s.fg3a for s in stats_list))
            total_ftm = float(sum(s.ftm for s in stats_list))
            total_fta = float(sum(s.fta for s in stats_list))
            total_min_seconds = float(sum(s.min.total_seconds() if s.min else 0 for s in stats_list))
            
            return {
                'games': len(stats_list), 
                'games_played': n, 
                'mpg': (total_min_seconds / 60) / divisor,
                'ppg': float(sum(s.pts for s in stats_list)) / divisor,
                'rpg': float(sum(s.reb for s in stats_list)) / divisor,
                'apg': float(sum(s.ast for s in stats_list)) / divisor,
                'spg': float(sum(s.stl for s in stats_list)) / divisor,
                'bpg': float(sum(s.blk for s in stats_list)) / divisor,
                'topg': float(sum(s.tov for s in stats_list)) / divisor,
                'fg_pct': total_fgm / total_fga if total_fga > 0 else 0.0,
                'fg3_pct': total_fg3m / total_fg3a if total_fg3a > 0 else 0.0,
                'ft_pct': total_ftm / total_fta if total_fta > 0 else 0.0,
                'plus_minus': float(sum(s.plus_minus or 0 for s in stats_list)) / divisor,
            }

        # 2. Historial desde player_team_seasons
        pts_records = session.query(PlayerTeamSeason)\
            .options(joinedload(PlayerTeamSeason.team))\
            .filter(PlayerTeamSeason.player_id == player_id)\
            .order_by(desc(PlayerTeamSeason.season), desc(PlayerTeamSeason.type)).all()

        def format_summary(r: PlayerTeamSeason):
            n = r.games_played or 1
            total_mins = r.minutes.total_seconds() / 60 if r.minutes else 0
            return {
                'season': r.season, 
                'team_abbr': r.team.abbreviation if r.team else '???',
                'team_id': r.team_id,
                'type': r.type,
                'games': r.games_played, 
                'mpg': total_mins / n,
                'ppg': (r.pts or 0) / n, 
                'rpg': (r.reb or 0) / n, 
                'apg': (r.ast or 0) / n,
                'spg': (r.stl or 0) / n, 
                'bpg': (r.blk or 0) / n, 
                'topg': (r.tov or 0) / n,
                'fg_pct': (r.fgm or 0) / (r.fga or 1) if (r.fga or 0) > 0 else 0,
                'fg3_pct': (r.fg3m or 0) / (r.fg3a or 1) if (r.fg3a or 0) > 0 else 0,
                'ft_pct': (r.ftm or 0) / (r.fta or 1) if (r.fta or 0) > 0 else 0,
                'plus_minus': (r.plus_minus or 0) / n,
                '_total_pts': r.pts or 0, 
                '_total_reb': r.reb or 0, 
                '_total_ast': r.ast or 0,
                '_total_stl': r.stl or 0, 
                '_total_blk': r.blk or 0, 
                '_total_tov': r.tov or 0,
                '_total_fgm': r.fgm or 0, 
                '_total_fga': r.fga or 0, 
                '_total_fg3m': r.fg3m or 0,
                '_total_fg3a': r.fg3a or 0, 
                '_total_ftm': r.ftm or 0, 
                '_total_fta': r.fta or 0,
                '_total_min_seconds': r.minutes.total_seconds() if r.minutes else 0,
                '_total_plus_minus': r.plus_minus or 0,
            }

        rs_list = [format_summary(r) for r in pts_records if r.type == 'Regular Season']
        po_list = [format_summary(r) for r in pts_records if r.type == 'Playoffs']
        ist_list = [format_summary(r) for r in pts_records if r.type == 'NBA Cup']

        def calculate_career_totals(data_list: List[Dict]) -> Optional[Dict]:
            if not data_list: return None
            total_games = sum(d['games'] for d in data_list)
            if total_games == 0: return None
            
            total_pts = sum(d['_total_pts'] for d in data_list)
            total_min_secs = sum(d['_total_min_seconds'] for d in data_list)
            total_fga = sum(d['_total_fga'] for d in data_list)
            total_fgm = sum(d['_total_fgm'] for d in data_list)
            total_fg3a = sum(d['_total_fg3a'] for d in data_list)
            total_fg3m = sum(d['_total_fg3m'] for d in data_list)
            total_fta = sum(d['_total_fta'] for d in data_list)
            total_ftm = sum(d['_total_ftm'] for d in data_list)
            
            return {
                'games': total_games, 
                'mpg': (total_min_secs / 60) / total_games,
                'ppg': total_pts / total_games, 
                'rpg': sum(d['_total_reb'] for d in data_list) / total_games,
                'apg': sum(d['_total_ast'] for d in data_list) / total_games,
                'spg': sum(d['_total_stl'] for d in data_list) / total_games,
                'bpg': sum(d['_total_blk'] for d in data_list) / total_games,
                'topg': sum(d['_total_tov'] for d in data_list) / total_games,
                'fg_pct': total_fgm / total_fga if total_fga > 0 else 0,
                'fg3_pct': total_fg3m / total_fg3a if total_fg3a > 0 else 0,
                'ft_pct': total_ftm / total_fta if total_fta > 0 else 0,
                'plus_minus': sum(d['_total_plus_minus'] for d in data_list) / total_games,
            }

        return {
            'last_7_days': {'games': last_7_stats, 'averages': calculate_averages(last_7_stats)},
            'last_month': {'games': last_month_stats, 'averages': calculate_averages(last_month_stats)},
            'regular_season': rs_list, 
            'playoffs': po_list, 
            'ist': ist_list,
            'rs_totals': calculate_career_totals(rs_list), 
            'po_totals': calculate_career_totals(po_list), 
            'ist_totals': calculate_career_totals(ist_list),
        }
    finally:
        if own_session: 
            session.close()


def get_player_awards(player_id: int, session: Optional[Session] = None) -> List[Dict[str, Any]]:
    """Obtiene los premios del jugador agrupados por tipo."""
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        from collections import defaultdict
        awards = session.query(PlayerAward).filter(PlayerAward.player_id == player_id).order_by(desc(PlayerAward.season)).all()
        if not awards: 
            return []
            
        grouped = defaultdict(list)
        award_types = {}
        for a in awards:
            grouped[a.award_name].append({
                'season': a.season, 
                'name': a.award_name, 
                'description': a.description
            })
            award_types[a.award_name] = a.award_type
            
        # Orden de importancia para mostrar
        type_importance = [
            'Champion', 'NBA Cup', 'MVP', 'Finals MVP', 'DPOY', 'ROY', 
            '6MOY', 'MIP', 'All-Star', 'All-NBA', 'All-Defensive', 
            'All-Rookie', 'Olympic Gold', 'Olympic Silver', 'Olympic Bronze', 
            'All-Star MVP', 'NBA Cup MVP', 'NBA Cup Team', 'POM', 'POW', 'ROM'
        ]
        
        result, type_to_names = [], defaultdict(list)
        for name in grouped.keys(): 
            type_to_names[award_types[name]].append(name)
            
        for atype in type_importance:
            if atype in type_to_names:
                for name in sorted(type_to_names[atype]):
                    result.append({
                        'type': atype, 
                        'count': len(grouped[name]), 
                        'award_items': grouped[name], 
                        'display_name': name if atype != 'Champion' else 'NBA Champion'
                    })
                    del grouped[name]
                    
        # Añadir cualquier otro no clasificado
        for name, items in grouped.items():
            result.append({
                'type': award_types[name], 
                'count': len(items), 
                'award_items': items, 
                'display_name': name
            })
        return result
    finally:
        if own_session: 
            session.close()


def get_player_career_highs(player_id: int, session: Optional[Session] = None) -> Dict[str, Any]:
    """Obtiene los récords personales máximos del jugador."""
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        from sqlalchemy import case, func
        
        # 1. Obtener los valores máximos y conteos de hitos en una sola consulta SQL
        # Esto evita cargar miles de registros en memoria
        stats_query = session.query(
            func.count(PlayerGameStats.id).label('total_games'),
            func.max(PlayerGameStats.pts).label('max_pts'),
            func.max(PlayerGameStats.reb).label('max_reb'),
            func.max(PlayerGameStats.ast).label('max_ast'),
            func.max(PlayerGameStats.stl).label('max_stl'),
            func.max(PlayerGameStats.blk).label('max_blk'),
            func.max(PlayerGameStats.fg3m).label('max_fg3m'),
            func.max(PlayerGameStats.fgm).label('max_fgm'),
            func.max(PlayerGameStats.ftm).label('max_ftm'),
            func.max(PlayerGameStats.min).label('max_min'),
            func.max(PlayerGameStats.plus_minus).label('max_plus_minus'),
            # Conteos de hitos
            func.sum(case((PlayerGameStats.pts >= 60, 1), else_=0)).label('g60'),
            func.sum(case((and_(PlayerGameStats.pts >= 50, PlayerGameStats.pts < 60), 1), else_=0)).label('g50'),
            func.sum(case((and_(PlayerGameStats.pts >= 40, PlayerGameStats.pts < 50), 1), else_=0)).label('g40'),
            func.sum(case((PlayerGameStats.reb >= 20, 1), else_=0)).label('r20'),
            func.sum(case((PlayerGameStats.ast >= 20, 1), else_=0)).label('a20'),
            func.sum(case((PlayerGameStats.stl >= 5, 1), else_=0)).label('s5'),
            func.sum(case((PlayerGameStats.blk >= 5, 1), else_=0)).label('b5'),
            func.sum(case((PlayerGameStats.fg3m >= 10, 1), else_=0)).label('t10'),
            # Dobles y Triples dobles (Lógica SQL)
            func.sum(case((
                case((PlayerGameStats.pts >= 10, 1), else_=0) +
                case((PlayerGameStats.reb >= 10, 1), else_=0) +
                case((PlayerGameStats.ast >= 10, 1), else_=0) +
                case((PlayerGameStats.stl >= 10, 1), else_=0) +
                case((PlayerGameStats.blk >= 10, 1), else_=0) >= 3, 1), else_=0
            )).label('td'),
            func.sum(case((
                case((PlayerGameStats.pts >= 10, 1), else_=0) +
                case((PlayerGameStats.reb >= 10, 1), else_=0) +
                case((PlayerGameStats.ast >= 10, 1), else_=0) +
                case((PlayerGameStats.stl >= 10, 1), else_=0) +
                case((PlayerGameStats.blk >= 10, 1), else_=0) >= 2, 1), else_=0
            )).label('dd')
        ).filter(PlayerGameStats.player_id == player_id).first()
        
        if not stats_query or stats_query.total_games == 0:
            return {
                'pts': None, 'reb': None, 'ast': None, 'stl': None, 'blk': None, 
                'fg3m': None, 'fgm': None, 'ftm': None, 'min': None, 'plus_minus': None, 
                'double_doubles': 0, 'triple_doubles': 0, 
                'games_40_pts': 0, 'games_50_pts': 0, 'games_60_pts': 0, 
                'games_20_reb': 0, 'games_20_ast': 0, 'games_5_stl': 0, 
                'games_5_blk': 0, 'games_10_3pm': 0, 'total_games': 0
            }
            
        # 2. Para cada máximo, obtener los detalles del partido correspondiente
        # Solo hacemos esto para los campos que el usuario realmente ve en el UI
        def get_high_detail(stat_attr, max_val):
            if max_val is None: return None
            
            # Buscar el partido donde ocurrió este máximo
            best_game_stat = session.query(PlayerGameStats)\
                .options(joinedload(PlayerGameStats.game).joinedload(Game.home_team),
                         joinedload(PlayerGameStats.game).joinedload(Game.away_team))\
                .filter(PlayerGameStats.player_id == player_id, 
                        getattr(PlayerGameStats, stat_attr) == max_val)\
                .join(Game).order_by(desc(Game.date)).first()
            
            if not best_game_stat: return None
            
            game = best_game_stat.game
            vs = game.away_team.abbreviation if best_game_stat.team_id == game.home_team_id else game.home_team.abbreviation
            
            val = max_val
            if stat_attr == 'min':
                val = max_val.total_seconds() / 60
                
            return {
                'value': val, 
                'game_id': game.id, 
                'date': game.date, 
                'vs_team': vs, 
                'season': game.season
            }
            
        return {
            'pts': get_high_detail('pts', stats_query.max_pts),
            'reb': get_high_detail('reb', stats_query.max_reb),
            'ast': get_high_detail('ast', stats_query.max_ast),
            'stl': get_high_detail('stl', stats_query.max_stl),
            'blk': get_high_detail('blk', stats_query.max_blk),
            'fg3m': get_high_detail('fg3m', stats_query.max_fg3m),
            'fgm': get_high_detail('fgm', stats_query.max_fgm),
            'ftm': get_high_detail('ftm', stats_query.max_ftm),
            'min': get_high_detail('min', stats_query.max_min),
            'plus_minus': get_high_detail('plus_minus', stats_query.max_plus_minus),
            'double_doubles': int(stats_query.dd or 0),
            'triple_doubles': int(stats_query.td or 0),
            'games_40_pts': int(stats_query.g40 or 0),
            'games_50_pts': int(stats_query.g50 or 0),
            'games_60_pts': int(stats_query.g60 or 0),
            'games_20_reb': int(stats_query.r20 or 0),
            'games_20_ast': int(stats_query.a20 or 0),
            'games_5_stl': int(stats_query.s5 or 0),
            'games_5_blk': int(stats_query.b5 or 0),
            'games_10_3pm': int(stats_query.t10 or 0),
            'total_games': stats_query.total_games
        }
    finally:
        if own_session: 
            session.close()
