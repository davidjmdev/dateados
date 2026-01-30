"""Rutas web para la sección independiente de Rachas.

Maneja el Dashboard de Rachas con soporte para múltiples competiciones
y tipos de hitos estadísticos.
"""

from fastapi import APIRouter, Request, Depends, Query
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, cast, Float, or_, case
from pathlib import Path
from typing import Optional, List, Dict
from datetime import timedelta
from math import ceil

from db.connection import get_session
from db.models import Game, Player
from outliers.stats.streaks import StreakCriteria
from outliers.models import StreakRecord, StreakAllTimeRecord

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

router = APIRouter(prefix="/streaks", tags=["streaks"])


def get_db():
    db = get_session()
    try:
        yield db
    finally:
        db.close()


STREAK_TYPE_NAMES = {
    'pts_20': '20+ Puntos',
    'pts_30': '30+ Puntos',
    'pts_40': '40+ Puntos',
    'triple_double': 'Triple-Dobles',
    'reb_10': '10+ Rebotes',
    'ast_10': '10+ Asistencias',
    'fg_pct_60': '60%+ FG',
    'fg3_pct_50': '50%+ 3P',
    'ft_pct_90': '90%+ FT',
}


@router.get("")
async def streaks_index(
    request: Request,
    comp: str = Query("regular"), # 'regular', 'playoffs', 'nba_cup'
    type: str = Query("all"),      # 'all', 'pts_20', 'pts_30', etc.
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=100),
    db: Session = Depends(get_db)
):
    """Página principal de rachas."""
    # Obtener temporada actual (solo para contexto)
    latest_game_date = db.query(func.max(Game.date)).scalar()
    latest_season_row = db.query(Game.season).filter(Game.date == latest_game_date).first()
    season = latest_season_row[0] if latest_season_row else "2024-25"
    
    # Obtener todos los tipos de racha disponibles para los filtros
    available_streak_types = list(StreakCriteria.get_all_criteria().keys())
    
    # Récords All-Time (para contexto y filtrado de relevancia)
    all_time_records = _get_streak_all_time_records(db, comp)
    
    # Paginación unificada: Contar total (activas + rotas recientes)
    # 1. Contar activas (con filtro de relevancia en SQL)
    active_query = _build_active_streaks_query(db, comp, None if type == "all" else type)
    total_active = db.query(func.count()).select_from(active_query.subquery()).scalar() or 0
    
    # 2. Contar rotas (con filtro de relevancia en SQL)
    broken_query = _build_broken_streaks_query(db, comp, None if type == "all" else type)
    total_broken = 0
    if broken_query:
        total_broken = db.query(func.count()).select_from(broken_query.subquery()).scalar() or 0
    
    # 3. Contar hitos históricos (longitud >= record o outlier marcado)
    total_historical = db.query(func.count()).select_from(
        active_query.filter(
            or_(
                StreakRecord.length >= func.coalesce(StreakAllTimeRecord.length, 999),
                StreakRecord.is_historical_outlier == True
            )
        ).subquery()
    ).scalar() or 0
        
    total_items = total_active + total_broken
    total_pages = ceil(total_items / per_page) if total_items > 0 else 0
    
    # Validar página
    if total_pages > 0 and page > total_pages:
        page = total_pages
        
    offset = (page - 1) * per_page
    
    # Obtener datos paginados (primero activas, luego rotas)
    active_streaks = []
    broken_streaks = []
    
    # Lógica de distribución de items entre activas y rotas según el offset
    if offset < total_active:
        # La página actual contiene al menos algunas rachas activas
        limit_active = per_page
        active_streaks = _get_active_streaks(
            db, 
            limit=limit_active, 
            offset=offset,
            competition_type=comp, 
            streak_type=None if type == "all" else type,
            all_time_records=all_time_records
        )
        
        # Si sobran espacios en la página, rellenar con las primeras rachas rotas
        remaining_slots = per_page - len(active_streaks)
        if remaining_slots > 0 and total_broken > 0:
            broken_streaks = _get_recently_broken_streaks(
                db,
                limit=remaining_slots,
                offset=0,
                competition_type=comp,
                streak_type=None if type == "all" else type,
                all_time_records=all_time_records
            )
    else:
        # La página actual contiene solo rachas rotas
        broken_offset = offset - total_active
        broken_streaks = _get_recently_broken_streaks(
            db,
            limit=per_page,
            offset=broken_offset,
            competition_type=comp,
            streak_type=None if type == "all" else type,
            all_time_records=all_time_records
        )
    
    return templates.TemplateResponse("streaks/index.html", {
        "request": request,
        "active_page": "streaks",
        "comp": comp,
        "streak_type_filter": type,
        "streak_types": available_streak_types,
        "streak_names": STREAK_TYPE_NAMES,
        "active_streaks": active_streaks,
        "broken_streaks": broken_streaks,
        "total_active": total_active,
        "total_broken": total_broken,
        "total_historical": total_historical,
        "all_time_records": all_time_records,
        "season": season,
        "page": page,
        "total_pages": total_pages,
        "per_page": per_page
    })


# ============ Helper Functions ============

def _build_active_streaks_query(
    db: Session,
    competition_type: str,
    streak_type: Optional[str] = None
):
    """Construye query base para rachas activas con filtro de relevancia SQL."""
    shooting_types = ['fg_pct_60', 'fg3_pct_50', 'ft_pct_90']
    
    query = (
        db.query(StreakRecord, Player)
        .join(Player, StreakRecord.player_id == Player.id)
        .outerjoin(StreakAllTimeRecord, and_(
            StreakRecord.streak_type == StreakAllTimeRecord.streak_type,
            StreakRecord.competition_type == StreakAllTimeRecord.competition_type
        ))
        .filter(and_(
            StreakRecord.is_active == True, 
            Player.is_active == True,
            StreakRecord.competition_type == competition_type
        ))
    )
    
    # Filtro de relevancia SQL:
    # - Para porcentajes de tiro: mínimo 3 partidos
    # - General: length >= max(2, all_time_length * 0.05)
    query = query.filter(
        case(
            (StreakRecord.streak_type.in_(shooting_types), StreakRecord.length >= 3),
            else_=StreakRecord.length >= func.greatest(2, cast(func.coalesce(StreakAllTimeRecord.length, 2), Float) * 0.05)
        )
    )
    
    if streak_type:
        query = query.filter(StreakRecord.streak_type == streak_type)
        
    return query


def _build_broken_streaks_query(
    db: Session,
    competition_type: str,
    streak_type: Optional[str] = None
):
    """Construye query base para rachas recientemente terminadas con filtro de relevancia SQL."""
    latest_game_date = db.query(func.max(Game.date)).scalar()
    if not latest_game_date:
        return None
        
    start_date = latest_game_date - timedelta(days=7)
    shooting_types = ['fg_pct_60', 'fg3_pct_50', 'ft_pct_90']
    
    query = (
        db.query(StreakRecord, Player)
        .join(Player, StreakRecord.player_id == Player.id)
        .outerjoin(StreakAllTimeRecord, and_(
            StreakRecord.streak_type == StreakAllTimeRecord.streak_type,
            StreakRecord.competition_type == StreakAllTimeRecord.competition_type
        ))
        .filter(and_(
            StreakRecord.is_active == False,
            StreakRecord.ended_at >= start_date,
            StreakRecord.competition_type == competition_type
        ))
    )
    
    # Filtro de relevancia SQL:
    # - Para porcentajes de tiro: mínimo 3 partidos
    # - General: length >= max(2, all_time_length * 0.05)
    query = query.filter(
        case(
            (StreakRecord.streak_type.in_(shooting_types), StreakRecord.length >= 3),
            else_=StreakRecord.length >= func.greatest(2, cast(func.coalesce(StreakAllTimeRecord.length, 2), Float) * 0.05)
        )
    )
    
    if streak_type:
        query = query.filter(StreakRecord.streak_type == streak_type)
        
    return query


def _get_active_streaks(
    db: Session, 
    limit: int = 20,
    offset: int = 0,
    competition_type: str = 'regular',
    streak_type: Optional[str] = None,
    all_time_records: Optional[dict] = None
) -> List[dict]:
    """Obtiene las rachas activas (ya filtradas por SQL)."""
    query = _build_active_streaks_query(db, competition_type, streak_type)
    query = query.order_by(StreakRecord.length.desc())
    
    # Al estar filtradas por SQL, limit y offset son directos
    results_raw = query.offset(offset).limit(limit).all()
    
    results = []
    for streak, player in results_raw:
        record = all_time_records.get(streak.streak_type) if all_time_records else None
        all_time_length = record['length'] if record else 2
        progress = min(100, int(100 * streak.length / all_time_length)) if all_time_length > 0 else 0
        
        results.append({
            'id': streak.id,
            'player_id': player.id,
            'player_name': player.full_name,
            'streak_type': streak.streak_type,
            'length': streak.length,
            'all_time_length': all_time_length,
            'all_time_holder': record['player_name'] if record else "Leyenda",
            'progress': progress,
            'started_at': streak.started_at.strftime('%d/%m/%Y') if streak.started_at else None,
            'is_historical': streak.length >= all_time_length or streak.is_historical_outlier,
        })
    
    return results


def _get_recently_broken_streaks(
    db: Session,
    limit: int = 15,
    offset: int = 0,
    competition_type: str = 'regular',
    streak_type: Optional[str] = None,
    all_time_records: Optional[dict] = None
) -> List[dict]:
    """Obtiene rachas terminadas recientemente (ya filtradas por SQL)."""
    query = _build_broken_streaks_query(db, competition_type, streak_type)
    if not query:
        return []
        
    query = query.order_by(StreakRecord.ended_at.desc(), StreakRecord.length.desc())
    
    results_raw = query.offset(offset).limit(limit).all()
    
    results = []
    for streak, player in results_raw:
        results.append({
            'id': streak.id,
            'player_id': player.id,
            'player_name': player.full_name,
            'streak_type': streak.streak_type,
            'length': streak.length,
            'started_at': streak.started_at.strftime('%d/%m/%Y') if streak.started_at else None,
            'ended_at': streak.ended_at.strftime('%d/%m/%Y') if streak.ended_at else None,
            'is_historical': streak.is_historical_outlier
        })
        
    return results


def _get_streak_all_time_records(db: Session, competition_type: str = 'regular') -> dict:
    """Obtiene el mapa de récords históricos."""
    records = db.query(StreakAllTimeRecord, Player.full_name).join(
        Player, StreakAllTimeRecord.player_id == Player.id
    ).filter(StreakAllTimeRecord.competition_type == competition_type).all()
    
    return {
        r.StreakAllTimeRecord.streak_type: {
            'length': r.StreakAllTimeRecord.length,
            'player_name': r.full_name,
            'player_id': r.StreakAllTimeRecord.player_id,
            'date': r.StreakAllTimeRecord.started_at.strftime('%Y') if r.StreakAllTimeRecord.started_at else "N/A"
        } for r in records
    }
