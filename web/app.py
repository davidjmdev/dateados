from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path

import logging
import sys
from ingestion.config import LOG_FORMAT, LOG_DATE_FORMAT
from db.utils.logging_handler import SQLAlchemyHandler
from db.connection import init_db

# Asegurar que las tablas existen antes de configurar logging
try:
    init_db()
except Exception as e:
    print(f"Error inicializando DB: {e}", file=sys.stderr)

# Configurar logging global (tanto consola como archivo de DB)
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    datefmt=LOG_DATE_FORMAT,
    handlers=[
        SQLAlchemyHandler(),
        logging.StreamHandler(sys.stdout)
    ]
)

app = FastAPI(title="NBA Stats Web")

# Configurar rutas de archivos estaticos y templates
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# Importar routers
from web.routes import home
from web.routes import players
from web.routes import teams
from web.routes import seasons
from web.routes import leaders
from web.routes import games
from web.routes import pencil
from web.routes import admin
from web.routes import outliers
from web.routes import streaks

# Incluir routers
app.include_router(home.router)
app.include_router(players.router)
app.include_router(teams.router)
app.include_router(seasons.router)
app.include_router(leaders.router)
app.include_router(games.router)
app.include_router(pencil.router)
app.include_router(admin.router)
app.include_router(outliers.router)
app.include_router(streaks.router)

if __name__ == "__main__":
    import os
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("web.app:app", host="0.0.0.0", port=port, reload=True)
