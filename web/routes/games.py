from fastapi import APIRouter, Request, Depends, Query
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from pathlib import Path
from math import ceil
from datetime import date as date_obj
from typing import Optional

from db.connection import get_session
from db import get_games, get_game_details
from db.models import Game

router = APIRouter()

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

def get_db():
    db = get_session()
    try:
        yield db
    finally:
        db.close()

@router.get("/games")
async def list_games(
    request: Request,
    page: int = 1,
    per_page: int = 20,
    season: Optional[str] = Query(None),
    team_id: Optional[int] = Query(None),
    date: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None), # Keep for backward compatibility if needed
    end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    # Si no se especifica temporada ni fecha, usar la mas reciente disponible
    if not season and not date and not start_date and not end_date:
        latest_season = db.query(Game.season).distinct().order_by(Game.season.desc()).first()
        season = latest_season[0] if latest_season else "2023-24"
    
    # Parse date
    parsed_date = None
    if date:
        try:
            parsed_date = date_obj.fromisoformat(date)
        except ValueError:
            pass
            
    parsed_start = None
    if start_date:
        try:
            parsed_start = date_obj.fromisoformat(start_date)
        except ValueError:
            pass
            
    parsed_end = None
    if end_date:
        try:
            parsed_end = date_obj.fromisoformat(end_date)
        except ValueError:
            pass
    
    # Calcular offset
    offset = (page - 1) * per_page
    
    # Construir query manualmente para paginacion
    from sqlalchemy import desc, or_, and_
    query = db.query(Game)
    
    if season:
        query = query.filter(Game.season == season)
    if team_id:
        query = query.filter(or_(Game.home_team_id == team_id, Game.away_team_id == team_id))
    if parsed_date:
        query = query.filter(Game.date == parsed_date)
    elif parsed_start and parsed_end:
        query = query.filter(Game.date.between(parsed_start, parsed_end))
    elif parsed_start:
        query = query.filter(Game.date >= parsed_start)
    elif parsed_end:
        query = query.filter(Game.date <= parsed_end)
        
    # Total count
    total_games = query.count()
    total_pages = ceil(total_games / per_page)
    
    # Resultados paginados
    from sqlalchemy.orm import joinedload
    games = query.options(joinedload(Game.home_team), joinedload(Game.away_team))\
                 .order_by(desc(Game.date))\
                 .offset(offset)\
                 .limit(per_page).all()
    
    # Obtener lista de temporadas para el filtro
    all_seasons = [s[0] for s in db.query(Game.season).distinct().order_by(Game.season.desc()).all()]
    
    # Obtener todos los equipos para el filtro
    from db.models import Team
    all_teams = db.query(Team).order_by(Team.full_name).all()
    
    # Calcular rango de fechas de la temporada
    min_date = None
    max_date = None
    if season and len(season) == 7: # Formato YYYY-YY
        year_start = int(season[:4])
        year_end = year_start + 1
        min_date = f"{year_start}-08-01"
        max_date = f"{year_end}-07-31"

    # Si es una peticion AJAX (Live Search), devolver solo el fragmento de la tabla
    if request.headers.get("X-Live-Search"):
        return templates.TemplateResponse("games/_table.html", {
            "request": request,
            "games": games,
            "page": page,
            "total_pages": total_pages,
            "season": season,
            "date": date,
            "team_id": team_id
        })

    return templates.TemplateResponse("games/list.html", {
        "request": request,
        "active_page": "games",
        "games": games,
        "page": page,
        "total_pages": total_pages,
        "season": season,
        "all_seasons": all_seasons,
        "all_teams": all_teams,
        "date": date,
        "team_id": team_id,
        "min_date": min_date,
        "max_date": max_date
    })

@router.get("/games/{game_id}")
async def game_detail(request: Request, game_id: str, db: Session = Depends(get_db)):
    details = get_game_details(game_id, session=db)
    if not details:
        return templates.TemplateResponse("404.html", {"request": request})
    
    # Dividir estadísticas por equipo
    home_team_id = details['game']['home_team_id']
    away_team_id = details['game']['away_team_id']
    
    home_player_stats = [s for s in details['player_stats'] if s['team_id'] == home_team_id]
    away_player_stats = [s for s in details['player_stats'] if s['team_id'] == away_team_id]
    
    # Extraer totales
    home_totals = next((t for t in details['team_stats'] if t['team_id'] == home_team_id), None)
    away_totals = next((t for t in details['team_stats'] if t['team_id'] == away_team_id), None)
    
    return templates.TemplateResponse("games/detail.html", {
        "request": request,
        "active_page": "games",
        "game": details['game'],
        "home_stats": home_player_stats,
        "away_stats": away_player_stats,
        "home_totals": home_totals,
        "away_totals": away_totals
    })
