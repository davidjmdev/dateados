from fastapi import APIRouter, Request, Depends, Query
from web.templates import templates
from sqlalchemy.orm import Session
from pathlib import Path
from sqlalchemy import desc
from typing import Optional

from db.connection import get_session
from db import get_teams, get_team_record, get_games
from db.models import Team, PlayerTeamSeason, Player, Game

router = APIRouter(prefix="/teams")

def get_db():
    db = get_session()
    try:
        yield db
    finally:
        db.close()

@router.get("")
async def list_teams(
    request: Request,
    db: Session = Depends(get_db)
):
    # Obtener todos los equipos
    teams = get_teams(session=db)
    
    # Agrupar por conferencia y division
    teams_by_conf = {
        'East': {
            'Atlantic': [], 'Central': [], 'Southeast': []
        },
        'West': {
            'Northwest': [], 'Pacific': [], 'Southwest': []
        }
    }
    
    for team in teams:
        # Aseguramos que tratamos los valores como strings
        conf = str(team.conference) if team.conference else None
        div = str(team.division) if team.division else None
        
        # Clasificar como actual si tiene conferencia y división estándar
        is_current = (
            conf in teams_by_conf and 
            div in teams_by_conf[conf]
        )
        
        if is_current:
            teams_by_conf[conf][div].append(team)
    
    return templates.TemplateResponse("teams/list.html", {
        "request": request,
        "active_page": "teams",
        "teams_by_conf": teams_by_conf
    })

@router.get("/{team_id}")
async def team_detail(
    request: Request, 
    team_id: int, 
    season: str = Query(None),
    db: Session = Depends(get_db)
):
    team = db.query(Team).filter(Team.id == team_id).first()
    if not team:
        return templates.TemplateResponse("404.html", {"request": request})
    
    # Obtener todas las temporadas disponibles para este equipo
    # Primero buscamos en PlayerTeamSeason
    seasons_query = db.query(PlayerTeamSeason.season).filter(
        PlayerTeamSeason.team_id == team_id
    ).distinct().order_by(desc(PlayerTeamSeason.season)).all()
    
    all_seasons = [s[0] for s in seasons_query]
    
    # Si no hay datos en PlayerTeamSeason (raro), buscamos en Games
    if not all_seasons:
        games_seasons_query = db.query(Game.season).filter(
            (Game.home_team_id == team_id) | (Game.away_team_id == team_id)
        ).distinct().order_by(desc(Game.season)).all()
        all_seasons = [s[0] for s in games_seasons_query]
    
    # Si no se especifica temporada, usar la mas reciente
    if not season and all_seasons:
        season = all_seasons[0]
    elif not season:
        season = "2023-24" # Fallback extremo
    
    # Record de la temporada elegida
    record = get_team_record(team_id, season=season, session=db)
    
    # Ultimos partidos de esa temporada
    recent_games = get_games(team_id=team_id, season=season, limit=10, finished_only=True, session=db)
    
    # Roster de la temporada elegida (evitando duplicados por tipo de competición)
    roster_raw = db.query(PlayerTeamSeason).filter(
        PlayerTeamSeason.team_id == team_id,
        PlayerTeamSeason.season == season
    ).join(Player).order_by(Player.full_name).all()
    
    # Deduplicar en Python: preferir 'Regular Season' o la entrada con más partidos
    roster_dict = {}
    for pts in roster_raw:
        pid = pts.player_id
        if pid not in roster_dict:
            roster_dict[pid] = pts
        else:
            # Si ya existe, preferir 'Regular Season' o la que tenga más partidos
            existing = roster_dict[pid]
            if pts.type == 'Regular Season':
                roster_dict[pid] = pts
            elif existing.type != 'Regular Season' and (pts.games_played or 0) > (existing.games_played or 0):
                roster_dict[pid] = pts
    
    # Convertir de nuevo a lista y re-ordenar por nombre
    roster = sorted(roster_dict.values(), key=lambda x: x.player.full_name)
    
    return templates.TemplateResponse("teams/detail.html", {
        "request": request,
        "active_page": "teams",
        "team": team,
        "record": record,
        "recent_games": recent_games,
        "roster": roster,
        "season": season,
        "all_seasons": all_seasons
    })
