"""Utilidades para consultar información de la base de datos NBA.

Este módulo proporciona funciones de alto nivel para consultar datos
de manera fácil y eficiente.
"""

import sys
import math
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Union
from sqlalchemy import func, desc, asc, and_, or_, case
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
    """Obtiene los mejores jugadores por una estadística.
    
    Para stats de conteo (pts, reb, ast, etc.) calcula el promedio por partido.
    Para porcentajes (fg_pct, fg3_pct, ft_pct) calcula SUM(made)/SUM(attempted)
    con un mínimo de intentos para filtrar jugadores con pocos tiros.
    
    Args:
        stat: Estadística a ordenar. Opciones:
              Conteo: "pts", "reb", "ast", "stl", "blk", "tov", "fgm", "fg3m", "ftm"
              Porcentaje: "fg_pct", "fg3_pct", "ft_pct"
        season: Temporada opcional (ej: "2024-25")
        limit: Máximo de resultados (default: 10)
        
    Returns:
        Lista de dicts con id, full_name, value, games
    """
    # Mapeo de porcentajes a sus columnas made/attempted
    PCT_STATS = {
        'fg_pct': ('fgm', 'fga', 300),   # mín 300 intentos (~4 FGA/game * 75 games)
        'fg3_pct': ('fg3m', 'fg3a', 82),  # mín 82 intentos (~1 3PA/game * 82 games)
        'ft_pct': ('ftm', 'fta', 125),    # mín 125 intentos (~1.5 FTA/game * 82 games)
    }
    
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        games_count = func.count(PlayerGameStats.id).label('games')
        is_pct = stat in PCT_STATS
        
        if is_pct:
            made_col, att_col, min_attempts = PCT_STATS[stat]
            total_made = func.sum(getattr(PlayerGameStats, made_col)).label('total_made')
            total_att = func.sum(getattr(PlayerGameStats, att_col)).label('total_att')
            
            query = session.query(
                Player.id,
                Player.full_name,
                total_made,
                total_att,
                games_count,
            ).join(PlayerGameStats, Player.id == PlayerGameStats.player_id)
            
            if season:
                query = query.join(Game, PlayerGameStats.game_id == Game.id).filter(Game.season == season)
            
            query = query.group_by(Player.id, Player.full_name)\
                .having(and_(games_count >= 5, total_att >= min_attempts))\
                .order_by(desc(total_made * 1.0 / total_att))\
                .limit(limit)
            
            results = query.all()
            return [
                {
                    'id': r.id,
                    'full_name': r.full_name,
                    'value': round(float(r.total_made) / float(r.total_att), 4) if r.total_att else 0.0,
                    'games': r.games,
                }
                for r in results
            ]
        else:
            avg_stat = func.avg(getattr(PlayerGameStats, stat)).label('avg_stat')
            
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
        query = session.query(Game)\
            .options(joinedload(Game.home_team), joinedload(Game.away_team))\
            .filter(Game.status == 3)
        if season: 
            query = query.filter(Game.season == season)
        if min_total is not None: 
            query = query.filter((Game.home_score + Game.away_score) >= min_total)
        if max_total is not None: 
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


# ============================================================
# Funciones de Temporadas y Clasificaciones
# ============================================================

def get_all_seasons(session: Optional[Session] = None) -> List[str]:
    """Obtiene todas las temporadas disponibles ordenadas de más reciente a más antigua.
    
    Returns:
        Lista de strings de temporada (ej: ["2025-26", "2024-25", ...])
    """
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        results = session.query(Game.season)\
            .distinct()\
            .order_by(desc(Game.season))\
            .all()
        return [r[0] for r in results]
    finally:
        if own_session:
            session.close()


def get_season_standings(season: str, session: Optional[Session] = None) -> Dict[str, List[Dict[str, Any]]]:
    """Obtiene la clasificación completa de ambas conferencias para una temporada.
    
    Calcula victorias/derrotas de Regular Season usando agregaciones SQL
    y agrupa los equipos por conferencia con ranking.
    
    Args:
        season: Temporada (ej: "2023-24")
        
    Returns:
        Dict con claves 'east' y 'west', cada una con lista de equipos ordenados por PCT.
        Cada equipo: {team_id, abbreviation, full_name, conference, division, wins, losses, pct, rank}
    """
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        base_filters = [Game.status == 3, Game.rs == True, Game.season == season]
        
        # Victorias por equipo
        wins_stats = dict(
            session.query(Game.winner_team_id, func.count(Game.id))
            .filter(*base_filters)
            .filter(Game.winner_team_id.isnot(None))
            .group_by(Game.winner_team_id).all()
        )
        
        # Derrotas por equipo
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
        
        # Obtener todos los equipos
        teams_map = {t.id: t for t in session.query(Team).all()}
        
        # Construir tabla de clasificación
        standings = []
        for team_id, team in teams_map.items():
            w = wins_stats.get(team_id, 0)
            l = losses_stats.get(team_id, 0)
            total = w + l
            if total == 0:
                continue  # Equipo sin partidos en esta temporada
            pct = w / total
            standings.append({
                'team_id': team_id,
                'abbreviation': team.abbreviation,
                'full_name': team.full_name,
                'conference': team.conference,
                'division': team.division,
                'wins': w,
                'losses': l,
                'pct': pct,
            })
        
        # Separar por conferencia y ordenar por PCT
        east = sorted([s for s in standings if s['conference'] == 'East'], key=lambda x: x['pct'], reverse=True)
        west = sorted([s for s in standings if s['conference'] == 'West'], key=lambda x: x['pct'], reverse=True)
        
        # Añadir ranking
        for i, s in enumerate(east):
            s['rank'] = i + 1
        for i, s in enumerate(west):
            s['rank'] = i + 1
        
        return {'east': east, 'west': west}
    finally:
        if own_session:
            session.close()


def _get_bracket_data(games_list: List[Game], is_ist: bool = False) -> Dict[int, List[Dict[str, Any]]]:
    """Función auxiliar para construir datos de bracket desde una lista de partidos.
    
    Agrupa partidos por serie (par de equipos), detecta la ronda y posición
    a partir del ID del partido, y construye la estructura de bracket.
    
    Args:
        games_list: Lista de objetos Game con home_team/away_team cargados
        is_ist: True si es NBA Cup (afecta la lógica de parseo de IDs)
        
    Returns:
        Dict con rondas como claves (1-4) y listas de series como valores
    """
    rounds_data = {1: [], 2: [], 3: [], 4: []}
    if not games_list:
        return rounds_data
    
    series_map = {}
    for g in games_list:
        if not g.home_team_id or not g.away_team_id:
            continue
        t1, t2 = sorted([g.home_team_id, g.away_team_id])
        s_key = (t1, t2)
        if s_key not in series_map:
            series_map[s_key] = {
                'team1_id': t1,
                'team2_id': t2,
                'team1_name': g.home_team.full_name if g.home_team_id == t1 else g.away_team.full_name,
                'team2_name': g.away_team.full_name if g.home_team_id == t1 else g.home_team.full_name,
                'team1_abbr': g.home_team.abbreviation if g.home_team_id == t1 else g.away_team.abbreviation,
                'team2_abbr': g.away_team.abbreviation if g.home_team_id == t1 else g.home_team.abbreviation,
                't1_wins': 0,
                't2_wins': 0,
                't1_score': 0,
                't2_score': 0,
                'first_date': g.date,
                'last_date': g.date,
                'r_hint': None,
                'r_pos': 99,
            }
        
        s = series_map[s_key]
        if g.winner_team_id == t1:
            s['t1_wins'] += 1
            if is_ist:
                s['t1_score'] = g.home_score if g.home_team_id == t1 else g.away_score
                s['t2_score'] = g.away_score if g.home_team_id == t1 else g.home_score
        elif g.winner_team_id == t2:
            s['t2_wins'] += 1
            if is_ist:
                s['t2_score'] = g.home_score if g.home_team_id == t2 else g.away_score
                s['t1_score'] = g.away_score if g.home_team_id == t2 else g.home_score
        
        if g.date < s['first_date']:
            s['first_date'] = g.date
        if g.date > s['last_date']:
            s['last_date'] = g.date
        
        # Detección de ronda y posición basada en Game ID
        try:
            if len(g.id) == 10:
                if is_ist:
                    if g.id.startswith('006'):
                        s['r_hint'] = 4
                        s['r_pos'] = 0
                    else:
                        if g.id.endswith('1201'): s['r_pos'] = 0; s['r_hint'] = 2
                        elif g.id.endswith('1202'): s['r_pos'] = 1; s['r_hint'] = 2
                        elif g.id.endswith('1203'): s['r_pos'] = 2; s['r_hint'] = 2
                        elif g.id.endswith('1204'): s['r_pos'] = 3; s['r_hint'] = 2
                        elif g.id.endswith('1229'): s['r_pos'] = 0; s['r_hint'] = 3
                        elif g.id.endswith('1230'): s['r_pos'] = 1; s['r_hint'] = 3
                else:
                    if g.id.startswith('004'):
                        s['r_hint'] = int(g.id[7])
                        s['r_pos'] = int(g.id[8])
        except Exception:
            pass
    
    sorted_series = sorted(series_map.values(), key=lambda x: (x['r_hint'] or 0, x['r_pos']))
    
    for s in sorted_series:
        r = s['r_hint']
        if not r:
            continue
        if r in rounds_data:
            first_date = s['first_date'].isoformat() if s['first_date'] else None
            last_date = s['last_date'].isoformat() if s['last_date'] else None
            rounds_data[r].append({
                'team1_id': s['team1_id'],
                'team1_name': s['team1_name'],
                'team1_abbr': s['team1_abbr'],
                'team2_id': s['team2_id'],
                'team2_name': s['team2_name'],
                'team2_abbr': s['team2_abbr'],
                't1_wins': s['t1_wins'],
                't2_wins': s['t2_wins'],
                't1_score': s['t1_score'],
                't2_score': s['t2_score'],
                'first_date': first_date,
                'last_date': last_date,
            })
    
    return rounds_data


def get_playoff_bracket(season: str, session: Optional[Session] = None) -> List[Dict[str, Any]]:
    """Obtiene el bracket de playoffs para una temporada.
    
    Args:
        season: Temporada (ej: "2023-24")
        
    Returns:
        Lista de rondas, cada una con nombre y lista de series.
        Cada serie incluye equipos, resultados y fechas.
    """
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        po_games = session.query(Game)\
            .options(joinedload(Game.home_team), joinedload(Game.away_team))\
            .filter(Game.season == season, Game.po == True, Game.status == 3)\
            .order_by(asc(Game.date)).all()
        
        po_rounds = _get_bracket_data(po_games, is_ist=False)
        
        round_names = {
            1: 'Primera Ronda',
            2: 'Semis de Conferencia',
            3: 'Finales de Conferencia',
            4: 'Finales NBA'
        }
        
        result = []
        for r_num in sorted(po_rounds.keys()):
            if po_rounds[r_num]:
                result.append({
                    'round': r_num,
                    'name': round_names.get(r_num, f'Ronda {r_num}'),
                    'series': po_rounds[r_num]
                })
        return result
    finally:
        if own_session:
            session.close()


def get_nba_cup_bracket(season: str, session: Optional[Session] = None) -> List[Dict[str, Any]]:
    """Obtiene el bracket de la NBA Cup para una temporada.
    
    Args:
        season: Temporada (ej: "2024-25")
        
    Returns:
        Lista de rondas (Cuartos, Semis, Final), cada una con series.
    """
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        ist_ko_games = session.query(Game)\
            .options(joinedload(Game.home_team), joinedload(Game.away_team))\
            .filter(Game.season == season, Game.ist == True, Game.status == 3)\
            .filter(or_(
                Game.rs == False,
                Game.id.endswith('01201'), Game.id.endswith('01202'),
                Game.id.endswith('01203'), Game.id.endswith('01204'),
                Game.id.endswith('01229'), Game.id.endswith('01230')
            ))\
            .order_by(asc(Game.date)).all()
        
        if not ist_ko_games:
            return []
        
        ist_rounds = _get_bracket_data(ist_ko_games, is_ist=True)
        
        round_names = {
            2: 'Cuartos de Final',
            3: 'Semifinales',
            4: 'Final (NBA Cup)'
        }
        
        result = []
        for r_num in sorted(ist_rounds.keys()):
            if ist_rounds[r_num]:
                result.append({
                    'round': r_num,
                    'name': round_names.get(r_num, f'Ronda {r_num}'),
                    'series': ist_rounds[r_num]
                })
        return result
    finally:
        if own_session:
            session.close()


# ============================================================
# Funciones de Equipos
# ============================================================

def get_team_roster(
    team_id: int,
    season: Optional[str] = None,
    session: Optional[Session] = None
) -> Dict[str, Any]:
    """Obtiene el roster de un equipo para una temporada específica.
    
    Deduplica jugadores que aparecen en múltiples tipos de competición
    (Regular Season, Playoffs, NBA Cup), priorizando Regular Season.
    
    Args:
        team_id: ID del equipo NBA
        season: Temporada (ej: "2024-25"). Si no se especifica, usa la más reciente.
        
    Returns:
        Dict con team_id, season y lista de jugadores del roster.
    """
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        # Si no se especifica temporada, obtener la más reciente del equipo
        if not season:
            latest = session.query(PlayerTeamSeason.season)\
                .filter(PlayerTeamSeason.team_id == team_id)\
                .order_by(desc(PlayerTeamSeason.season))\
                .first()
            if latest:
                season = latest[0]
            else:
                return {'team_id': team_id, 'season': None, 'count': 0, 'players': []}
        
        roster_raw = session.query(PlayerTeamSeason)\
            .options(joinedload(PlayerTeamSeason.player))\
            .filter(
                PlayerTeamSeason.team_id == team_id,
                PlayerTeamSeason.season == season
            ).all()
        
        # Deduplicar: preferir 'Regular Season' o la entrada con más partidos
        roster_dict = {}
        for pts in roster_raw:
            pid = pts.player_id
            if pid not in roster_dict:
                roster_dict[pid] = pts
            else:
                existing = roster_dict[pid]
                if pts.type == 'Regular Season':
                    roster_dict[pid] = pts
                elif existing.type != 'Regular Season' and (pts.games_played or 0) > (existing.games_played or 0):
                    roster_dict[pid] = pts
        
        players = []
        for pts in sorted(roster_dict.values(), key=lambda x: x.player.full_name if x.player else ''):
            if not pts.player:
                continue
            p = pts.player
            n = pts.games_played or 1
            total_mins = pts.minutes.total_seconds() / 60 if pts.minutes else 0
            players.append({
                'id': p.id,
                'full_name': p.full_name,
                'position': p.position,
                'jersey': p.jersey,
                'height': p.height,
                'weight': p.weight,
                'country': p.country,
                'is_active': p.is_active,
                'games_played': pts.games_played or 0,
                'ppg': (pts.pts or 0) / n,
                'rpg': (pts.reb or 0) / n,
                'apg': (pts.ast or 0) / n,
                'mpg': total_mins / n,
            })
        
        return {
            'team_id': team_id,
            'season': season,
            'count': len(players),
            'players': players,
        }
    finally:
        if own_session:
            session.close()


# ============================================================
# Funciones de Ranking y Agregación de Jugadores
# ============================================================

def _parse_height_inches(h: str, default: int = 0) -> int:
    """Convierte altura en formato '6-9' a total inches (81).
    
    Args:
        h: Altura en formato 'feet-inches' (ej: '6-9')
        default: Valor por defecto si el parsing falla
        
    Returns:
        Altura en pulgadas totales, o default si falla
    """
    try:
        parts = h.split('-')
        return int(parts[0]) * 12 + int(parts[1])
    except Exception:
        return default


def get_player_rankings(
    criteria: str,
    active_only: bool = True,
    limit: int = 10,
    session: Optional[Session] = None
) -> List[Dict[str, Any]]:
    """Obtiene un ranking de jugadores según un criterio específico.
    
    Args:
        criteria: Criterio de ranking. Opciones:
            - "youngest": más jóvenes (fecha nacimiento más reciente)
            - "oldest": más veteranos (fecha nacimiento más antigua)
            - "heaviest": más pesados
            - "lightest": más ligeros
            - "tallest": más altos (requiere parsing de height string)
            - "shortest": más bajos
            - "most_experienced": más temporadas de experiencia
            - "highest_draft_pick": picks más altos (número más bajo)
            - "lowest_draft_pick": picks más bajos (número más alto)
        active_only: Si True, solo jugadores activos (default: True)
        limit: Número de resultados (default: 10, max: 50)
        
    Returns:
        Lista de dicts con id, full_name, value y detail
    """
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        limit = min(limit, 50)
        query = session.query(Player)
        
        if active_only:
            query = query.filter(Player.is_active == True)
        
        if criteria == 'youngest':
            query = query.filter(Player.birthdate.isnot(None))
            query = query.order_by(desc(Player.birthdate))
        elif criteria == 'oldest':
            query = query.filter(Player.birthdate.isnot(None))
            query = query.order_by(asc(Player.birthdate))
        elif criteria == 'heaviest':
            query = query.filter(Player.weight.isnot(None), Player.weight > 0)
            query = query.order_by(desc(Player.weight))
        elif criteria == 'lightest':
            query = query.filter(Player.weight.isnot(None), Player.weight > 0)
            query = query.order_by(asc(Player.weight))
        elif criteria == 'most_experienced':
            query = query.filter(Player.season_exp.isnot(None))
            query = query.order_by(desc(Player.season_exp))
        elif criteria == 'highest_draft_pick':
            query = query.filter(Player.draft_number.isnot(None), Player.draft_number > 0)
            query = query.order_by(asc(Player.draft_number), desc(Player.draft_year))
        elif criteria == 'lowest_draft_pick':
            query = query.filter(Player.draft_number.isnot(None), Player.draft_number > 0)
            query = query.order_by(desc(Player.draft_number), desc(Player.draft_year))
        elif criteria == 'tallest':
            # height está como string "6-9", necesitamos ordenar por conversión
            query = query.filter(Player.height.isnot(None))
            players_all = query.all()
            
            players_sorted = sorted(players_all, key=lambda p: _parse_height_inches(p.height, default=0), reverse=True)[:limit]
            return [
                {
                    'id': p.id,
                    'full_name': p.full_name,
                    'value': p.height,
                    'detail': f"{p.position or 'N/A'} | {p.country or 'N/A'}",
                }
                for p in players_sorted
            ]
        elif criteria == 'shortest':
            query = query.filter(Player.height.isnot(None))
            players_all = query.all()
            
            players_sorted = sorted(players_all, key=lambda p: _parse_height_inches(p.height, default=999))[:limit]
            return [
                {
                    'id': p.id,
                    'full_name': p.full_name,
                    'value': p.height,
                    'detail': f"{p.position or 'N/A'} | {p.country or 'N/A'}",
                }
                for p in players_sorted
            ]
        else:
            return []
        
        # Para criterios que no requieren parsing especial
        players = query.limit(limit).all()
        
        results = []
        for p in players:
            if criteria in ('youngest', 'oldest'):
                value = p.birthdate.isoformat() if p.birthdate else None
                detail = f"{p.position or 'N/A'} | {p.country or 'N/A'}"
            elif criteria in ('heaviest', 'lightest'):
                value = p.weight
                detail = f"{p.height or 'N/A'} | {p.position or 'N/A'}"
            elif criteria == 'most_experienced':
                value = p.season_exp
                detail = f"Desde {p.from_year or '?'} | {p.position or 'N/A'}"
            elif criteria in ('highest_draft_pick', 'lowest_draft_pick'):
                value = p.draft_number
                detail = f"Draft {p.draft_year or '?'} Ronda {p.draft_round or '?'}"
            else:
                value = None
                detail = None
            
            results.append({
                'id': p.id,
                'full_name': p.full_name,
                'value': value,
                'detail': detail,
            })
        
        return results
    finally:
        if own_session:
            session.close()


def get_award_leaders(
    award_type: Optional[str] = None,
    active_only: bool = False,
    limit: int = 10,
    session: Optional[Session] = None
) -> List[Dict[str, Any]]:
    """Obtiene los jugadores con más premios de un tipo específico.
    
    Args:
        award_type: Tipo de premio a filtrar (ej: "MVP", "Champion", "All-Star",
                    "All-NBA", "DPOY", "Finals MVP", "ROY", "6MOY", "MIP",
                    "All-Defensive", "All-Rookie"). None = todos los premios.
        active_only: Si True, solo jugadores activos (default: False)
        limit: Número de resultados (default: 10, max: 50)
        
    Returns:
        Lista de dicts con id, full_name, count, seasons
    """
    own_session = False
    if session is None:
        session = get_session()
        own_session = True
    try:
        limit = min(limit, 50)
        
        # Contar premios por jugador
        query = session.query(
            Player.id,
            Player.full_name,
            Player.is_active,
            func.count(PlayerAward.id).label('award_count'),
        ).join(PlayerAward, Player.id == PlayerAward.player_id)
        
        if award_type:
            query = query.filter(PlayerAward.award_type == award_type)
        
        if active_only:
            query = query.filter(Player.is_active == True)
        
        query = query.group_by(Player.id, Player.full_name, Player.is_active)\
            .order_by(desc('award_count'))\
            .limit(limit)
        
        results_raw = query.all()
        
        if not results_raw:
            return []
        
        # Obtener todas las temporadas de premios en una sola query (evitar N+1)
        player_ids = [r.id for r in results_raw]
        seasons_query = session.query(
            PlayerAward.player_id,
            PlayerAward.season
        ).filter(PlayerAward.player_id.in_(player_ids))
        if award_type:
            seasons_query = seasons_query.filter(PlayerAward.award_type == award_type)
        seasons_query = seasons_query.distinct()
        
        # Agrupar temporadas por jugador
        player_seasons = {}
        for pid, season in seasons_query.all():
            player_seasons.setdefault(pid, []).append(season)
        
        # Construir resultados
        results = []
        for r in results_raw:
            seasons = sorted(player_seasons.get(r.id, []), reverse=True)
            
            results.append({
                'id': r.id,
                'full_name': r.full_name,
                'is_active': r.is_active,
                'count': r.award_count,
                'seasons': seasons,
            })
        
        return results
    finally:
        if own_session:
            session.close()
