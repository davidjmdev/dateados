"""Rutas para el juego 'Alto el lápiz'.

Este módulo define los endpoints para la interfaz web del juego
y las validaciones vía API.
"""

import logging
from typing import List, Optional
from fastapi import APIRouter, Request, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates

from db.connection import get_session
from web.pencil_logic import PencilGameLogic

logger = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="web/templates")
logic = PencilGameLogic()

@router.get("/pencil")
async def pencil_index(request: Request):
    """Página principal del juego Alto el lápiz."""
    return templates.TemplateResponse("pencil/index.html", {
        "request": request,
        "active_page": "pencil"
    })

@router.get("/api/pencil/validate")
async def validate_player(
    category: str, 
    letter: str, 
    name: str
):
    """Valida un jugador para una categoría y letra específica."""
    if not letter or len(letter) != 1:
        return JSONResponse(
            status_code=400,
            content={'valid': False, 'message': 'Letra inválida.'}
        )
        
    with get_session() as session:
        result = logic.validate_player(session, name, category, letter)
        return result

@router.get("/api/pencil/hint")
async def get_hint(
    category: str, 
    letter: str
):
    """Obtiene ejemplos de jugadores válidos."""
    with get_session() as session:
        hints = logic.get_hints(session, category, letter)
        return {'hints': hints}
