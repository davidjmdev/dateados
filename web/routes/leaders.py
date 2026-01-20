from fastapi import APIRouter, Request, Depends, Query
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from pathlib import Path
from typing import Optional

from db.connection import get_session
from db import get_top_players, get_games
from db.models import Game

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

router = APIRouter()

def get_db():
    db = get_session()
    try:
        yield db
    finally:
        db.close()

@router.get("/leaders")
async def leaders_index(
    request: Request,
    season: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    # Si no se especifica temporada, usar la mas reciente disponible
    if not season:
        latest_season = db.query(Game.season).distinct().order_by(Game.season.desc()).first()
        season = latest_season[0] if latest_season else "2023-24"
    
    # Obtener todas las temporadas para el dropdown
    all_seasons = [s[0] for s in db.query(Game.season).distinct().order_by(Game.season.desc()).all()]
    
    # Obtener top 10 para cada categoria principal
    leaders = {
        'pts': get_top_players(stat='pts', season=season, limit=10, session=db),
        'reb': get_top_players(stat='reb', season=season, limit=10, session=db),
        'ast': get_top_players(stat='ast', season=season, limit=10, session=db),
        'stl': get_top_players(stat='stl', season=season, limit=10, session=db),
        'blk': get_top_players(stat='blk', season=season, limit=10, session=db)
    }
    
    # Si es una peticion AJAX (Live Search), devolver solo el fragmento de la rejilla
    if request.headers.get("X-Live-Search"):
        return templates.TemplateResponse("leaders/_grid.html", {
            "request": request,
            "leaders": leaders,
            "season": season
        })

    return templates.TemplateResponse("leaders/index.html", {
        "request": request,
        "active_page": "leaders",
        "season": season,
        "all_seasons": all_seasons,
        "leaders": leaders
    })
