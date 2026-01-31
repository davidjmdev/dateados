from fastapi import APIRouter, Request, Depends, Query
from web.templates import templates
from sqlalchemy.orm import Session
from pathlib import Path
from math import ceil
from datetime import date
from typing import Optional

from db.connection import get_session
from db import (
    get_players, 
    get_player_stats, 
    get_player_season_averages,
    get_current_teammates,
    get_historical_teammates,
    get_player_career_stats,
    get_player_career_highs,
    get_player_awards
)
from db.models import Player

router = APIRouter(prefix="/players")

def get_db():
    db = get_session()
    try:
        yield db
    finally:
        db.close()

@router.get("")
async def list_players(
    request: Request,
    page: int = 1,
    per_page: int = 50,
    search: Optional[str] = None,
    position: Optional[str] = None,
    active_only: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    # Calcular offset
    offset = (page - 1) * per_page
    
    # Construir query base
    query = db.query(Player)
    
    if search:
        query = query.filter(Player.full_name.ilike(f"%{search}%"))
    if position:
        query = query.filter(Player.position.ilike(f"%{position}%"))
    
    if active_only == '1':
        from datetime import datetime
        current_year = datetime.now().year
        from sqlalchemy import or_
        query = query.filter(
            or_(
                Player.to_year.is_(None),
                Player.to_year >= current_year - 1
            )
        )
        
    # Total count para paginacion
    total_players = query.count()
    total_pages = ceil(total_players / per_page)
    
    # Obtener jugadores de la pagina actual
    players = query.order_by(Player.full_name).offset(offset).limit(per_page).all()
    
    # Si es una peticion AJAX (Live Search), devolver solo el fragmento de la tabla
    if request.headers.get("X-Live-Search"):
        return templates.TemplateResponse("players/_table.html", {
            "request": request,
            "players": players,
            "page": page,
            "total_pages": total_pages,
            "search": search,
            "position": position,
            "active_only": active_only
        })

    return templates.TemplateResponse("players/list.html", {
        "request": request,
        "active_page": "players",
        "players": players,
        "page": page,
        "total_pages": total_pages,
        "search": search,
        "position": position,
        "active_only": active_only
    })

@router.get("/{player_id}")
async def player_detail(request: Request, player_id: int, db: Session = Depends(get_db)):
    player = db.query(Player).filter(Player.id == player_id).first()
    if not player:
        # TODO: Handle 404 properly
        return templates.TemplateResponse("404.html", {"request": request})
    
    # Obtener historial de temporadas (usando PlayerTeamSeason)
    # Deduplicar por (season, team_id) para evitar mostrar RS/PO por separado
    # Ordenar por temporada desc y por fecha de fin desc
    raw_sorted = sorted(player.team_seasons, key=lambda x: (x.season, x.end_date or date.min), reverse=True)
    
    dedup_raw = {}
    for ts in raw_sorted:
        key = (ts.season, ts.team_id)
        if key not in dedup_raw:
            dedup_raw[key] = ts
        else:
            # Preferir 'Regular Season' o la que tenga más partidos
            existing = dedup_raw[key]
            if ts.type == 'Regular Season':
                dedup_raw[key] = ts
            elif existing.type != 'Regular Season' and (ts.games_played or 0) > (existing.games_played or 0):
                dedup_raw[key] = ts
                
    raw_team_seasons = sorted(dedup_raw.values(), key=lambda x: (x.season, x.end_date or date.min), reverse=True)
    
    # Historial de equipos
    team_seasons = []
    for ts in raw_team_seasons:
        team_seasons.append({
            'season': ts.season,
            'team': ts.team,
            'end_date': ts.end_date
        })
    
    # Obtener el dorsal desde la ficha biográfica del jugador
    dorsal_reciente = player.jersey
    
    # Obtener ultimos partidos
    recent_stats = get_player_stats(player_id=player_id, limit=10, session=db)
    
    # Obtener promedios de la ultima temporada disponible
    ultima_temporada = None
    promedios = None
    if team_seasons:
        ultima_temporada = team_seasons[0]['season']
        promedios = get_player_season_averages(player_id, ultima_temporada, session=db)
    
    # Obtener compañeros actuales
    current_teammates = get_current_teammates(player_id, session=db)
    
    # Obtener estadísticas de carrera por temporada
    career_stats = get_player_career_stats(player_id, session=db)
    
    # Obtener récords personales
    career_highs = get_player_career_highs(player_id, session=db)
    
    # Obtener premios y logros (Persistentes en BD)
    awards = get_player_awards(player_id, session=db)
    
    return templates.TemplateResponse("players/detail.html", {
        "request": request,
        "active_page": "players",
        "player": player,
        "team_seasons": team_seasons,
        "dorsal_reciente": dorsal_reciente,
        "recent_stats": recent_stats,
        "promedios": promedios,
        "ultima_temporada": ultima_temporada,
        "current_teammates": current_teammates,
        "career_stats": career_stats,
        "career_highs": career_highs,
        "awards": awards
    })


@router.get("/{player_id}/teammates")
async def player_teammates(
    request: Request, 
    player_id: int, 
    page: int = 1,
    per_page: int = 20,
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    player = db.query(Player).filter(Player.id == player_id).first()
    if not player:
        return templates.TemplateResponse("404.html", {"request": request})
    
    # Obtener todos los compañeros (lista ya ordenada por defecto en get_historical_teammates)
    all_teammates = get_historical_teammates(player_id, session=db)
    
    # Filtrar por búsqueda si existe
    if search:
        search_lower = search.lower()
        all_teammates = [t for t in all_teammates if search_lower in t['full_name'].lower()]
    
    # Paginación manual de la lista
    total_teammates = len(all_teammates)
    total_pages = ceil(total_teammates / per_page)
    
    offset = (page - 1) * per_page
    teammates = all_teammates[offset : offset + per_page]
    
    # Si es una peticion AJAX (Live Search), devolver solo el fragmento de la tabla
    if request.headers.get("X-Live-Search"):
        return templates.TemplateResponse("players/_teammates_table.html", {
            "request": request,
            "player": player,
            "teammates": teammates,
            "search": search,
            "page": page,
            "total_pages": total_pages
        })

    return templates.TemplateResponse("players/teammates.html", {
        "request": request,
        "active_page": "players",
        "player": player,
        "teammates": teammates,
        "search": search,
        "page": page,
        "total_pages": total_pages
    })
