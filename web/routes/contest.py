"""Rutas para el juego 'Alto el lápiz' (Contest).

Este módulo define los endpoints para la interfaz web del juego
y las validaciones vía API.
"""

import logging
from typing import List, Optional
from fastapi import APIRouter, Request, HTTPException, Query
from fastapi.responses import JSONResponse
from web.templates import templates

from db.connection import get_session
from web.contest_logic import ContestGameLogic

logger = logging.getLogger(__name__)
router = APIRouter()
logic = ContestGameLogic()

@router.get("/contest")
async def contest_index(request: Request):
    """Página principal del juego Alto el lápiz."""
    return templates.TemplateResponse("contest/index.html", {
        "request": request,
        "active_page": "contest"
    })

@router.get("/api/contest/validate")
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

@router.get("/api/contest/hint")
async def get_hint(
    category: str, 
    letter: str
):
    """Obtiene ejemplos de jugadores válidos."""
    with get_session() as session:
        hints = logic.get_hints(session, category, letter)
        return {'hints': hints}
