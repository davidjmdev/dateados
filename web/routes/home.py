from fastapi import APIRouter, Request, Depends
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from pathlib import Path

from db.connection import get_session
from db import get_database_stats, get_games

router = APIRouter()

# Configurar templates
BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Dependencia para obtener la sesion de BD
def get_db():
    db = get_session()
    try:
        yield db
    finally:
        db.close()

@router.get("/")
async def home(request: Request, db: Session = Depends(get_db)):
    # Obtener estadisticas generales
    stats = get_database_stats(session=db)
    
    # Obtener algunos partidos recientes
    # Nota: get_games devuelve objetos Game, que tienen acceso a los equipos via relationship
    games = get_games(limit=6, session=db)
    
    # Preparar datos para el template
    recent_games = []
    for game in games:
        # Forzar carga de equipos si no están cargados
        home_team = game.home_team
        away_team = game.away_team
        
        recent_games.append({
            "home_team_abbr": home_team.abbreviation if home_team else f"ID {game.home_team_id}",
            "away_team_abbr": away_team.abbreviation if away_team else f"ID {game.away_team_id}",
            "home_score": int(game.home_score) if game.home_score is not None else 0,
            "away_score": int(game.away_score) if game.away_score is not None else 0,
            "date": game.date,
            "id": str(game.id),
            "status": int(game.status) if game.status is not None else 1
        })
    
    return templates.TemplateResponse("home.html", {
        "request": request,
        "active_page": "home",
        "stats": stats,
        "recent_games": recent_games
    })
