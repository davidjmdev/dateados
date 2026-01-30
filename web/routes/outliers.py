"""Rutas web para el sistema de detección de outliers.

Endpoints:
- GET /outliers - Página principal de outliers
- GET /outliers/api/league - Top outliers de liga (JSON)
- GET /outliers/api/player - Top outliers de jugador (JSON)
- GET /outliers/api/streaks - Rachas activas e históricas (JSON)
- GET /outliers/api/stats - Estadísticas del sistema (JSON)
"""

from fastapi import APIRouter, Request, Depends, Query, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
from pathlib import Path
from typing import Optional, List
from datetime import date, timedelta
from math import ceil

from db.connection import get_session
from db.models import Game, Player, PlayerGameStats
from outliers.models import LeagueOutlier, PlayerOutlier, PlayerTrendOutlier

STAT_NAMES_MAP = {
    'pts': 'Puntos', 'ast': 'Asistencias', 'reb': 'Rebotes',
    'stl': 'Robos', 'blk': 'Tapones', 'tov': 'Pérdidas',
    'fga': 'Tiros Intentados', 'fg_pct': 'FG%',
    'fg3_pct': '3P%', 'ft_pct': 'FT%'
}

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def _build_league_query(db: Session, season: str, window: str):
    """Construye query base para outliers de liga."""
    query = (
        db.query(LeagueOutlier, PlayerGameStats, Player, Game)
        .join(PlayerGameStats, LeagueOutlier.player_game_stat_id == PlayerGameStats.id)
        .join(Player, PlayerGameStats.player_id == Player.id)
        .join(Game, PlayerGameStats.game_id == Game.id)
        .filter(LeagueOutlier.is_outlier == True)
        .filter(Player.is_active == True)
    )
    
    if window == 'last_game':
        latest_date = db.query(func.max(Game.date)).scalar()
        if latest_date:
            query = query.filter(Game.date == latest_date)
    elif window == 'week':
        latest_date = db.query(func.max(Game.date)).scalar()
        if latest_date:
            query = query.filter(Game.date >= latest_date - timedelta(days=7))
    elif window == 'month':
        latest_date = db.query(func.max(Game.date)).scalar()
        if latest_date:
            query = query.filter(Game.date >= latest_date - timedelta(days=30))
    elif season:
        query = query.filter(Game.season == season)
    
    return query


def _build_player_query(db: Session, season: str, window: str):
    """Construye query base para outliers de jugador."""
    if window == 'last_game':
        latest_date = db.query(func.max(Game.date)).scalar()
        query = (
            db.query(PlayerOutlier, PlayerGameStats, Player, Game)
            .join(PlayerGameStats, PlayerOutlier.player_game_stat_id == PlayerGameStats.id)
            .join(Player, PlayerGameStats.player_id == Player.id)
            .join(Game, PlayerGameStats.game_id == Game.id)
            .filter(Player.is_active == True)
        )
        if latest_date:
            query = query.filter(Game.date == latest_date)
    else:
        query = (
            db.query(PlayerTrendOutlier, Player)
            .join(Player, PlayerTrendOutlier.player_id == Player.id)
            .filter(and_(
                Player.is_active == True,
                PlayerTrendOutlier.window_type == window
            ))
        )
        latest_date = db.query(func.max(PlayerTrendOutlier.reference_date)).scalar()
        if latest_date:
            query = query.filter(PlayerTrendOutlier.reference_date >= latest_date - timedelta(days=7))
    
    return query

router = APIRouter(prefix="/outliers", tags=["outliers"])


def get_db():
    db = get_session()
    try:
        yield db
    finally:
        db.close()


@router.get("")
async def outliers_index(
    request: Request,
    window: str = Query("last_game"),  # 'last_game', 'week', 'month'
    tab: str = Query("player"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=100),
    db: Session = Depends(get_db)
):
    """Página principal de outliers."""
    # Inicializar todas las variables para evitar UnboundLocalError
    league_outliers = []
    player_outliers = []
    total_league_pages = 0
    total_player_pages = 0
    
    # Obtener la temporada más reciente automáticamente
    latest_season_row = db.query(Game.season).distinct().order_by(Game.season.desc()).first()
    season = latest_season_row[0] if latest_season_row else "2024-25"
    
    # Calcular offset para paginación
    offset = (page - 1) * per_page
    
    # Estadísticas generales filtradas por ventana y activos
    stats = _get_outlier_stats(db, season, window)
    
    if tab == 'league':
        # 1. Construir query base
        query = _build_league_query(db, season, window)
        
        # 2. Contar (usando subquery para evitar problemas de join con count)
        total_league = db.query(func.count()).select_from(query.subquery()).scalar() or 0
        total_league_pages = ceil(total_league / per_page) if total_league > 0 else 0
        
        # 3. Obtener datos
        query = query.order_by(LeagueOutlier.percentile.desc())
        if offset is not None:
            query = query.offset(offset)
        
        for outlier, stats_row, player, game in query.limit(per_page).all():
            top_features = []
            if outlier.feature_contributions:
                sorted_features = sorted(
                    outlier.feature_contributions.items(), 
                    key=lambda x: x[1], 
                    reverse=True
                )[:3]
                top_features = [f[0] for f in sorted_features]
            
            league_outliers.append({
                'id': outlier.id,
                'player_id': player.id,
                'player_name': player.full_name,
                'game_id': game.id,
                'game_date': game.date.isoformat() if game.date else None,
                'season': game.season,
                'pts': stats_row.pts,
                'reb': stats_row.reb,
                'ast': stats_row.ast,
                'percentile': round(outlier.percentile, 1),
                'reconstruction_error': round(outlier.reconstruction_error, 4),
                'top_features': top_features,
            })
    
    elif tab == 'player':
        # 1. Construir query base
        query = _build_player_query(db, season, window)
        
        # 2. Contar
        total_player = db.query(func.count()).select_from(query.subquery()).scalar() or 0
        total_player_pages = ceil(total_player / per_page) if total_player > 0 else 0
        
        # 3. Obtener datos
        if window == 'last_game':
            query = query.order_by(
                func.abs(PlayerOutlier.max_z_score).desc()
            )
            if offset is not None:
                query = query.offset(offset)
            
            for outlier, stats_row, player, game in query.limit(per_page).all():
                features = sorted(outlier.outlier_features, key=lambda x: abs(x['z_score']), reverse=True)
                primary_feature = features[0] if features else None
                
                player_outliers.append({
                    'id': outlier.id,
                    'player_id': player.id,
                    'player_name': player.full_name,
                    'game_id': game.id,
                    'game_date': game.date.isoformat() if game.date else None,
                    'pts': stats_row.pts,
                    'primary_val': primary_feature['val'] if primary_feature else stats_row.pts,
                    'primary_feat': primary_feature['feature'] if primary_feature else 'pts',
                    'primary_avg': primary_feature['avg'] if primary_feature else 0,
                    'outlier_type': outlier.outlier_type,
                    'max_z_score': round(outlier.max_z_score, 2),
                    'outlier_features': features,
                    'window': 'game'
                })
        else:
            # Para tendencias
            player_outliers = _get_trend_player_outliers(
                db, season, window=window, outlier_type=None, limit=per_page, offset=offset
            )
    
    return templates.TemplateResponse("outliers/index.html", {
        "request": request,
        "active_page": "outliers",
        "season": season,
        "window": window,
        "tab": tab,
        "page": page,
        "total_league_pages": total_league_pages,
        "total_player_pages": total_player_pages,
        "stats": stats,
        "league_outliers": league_outliers,
        "player_outliers": player_outliers,
        "stat_names": STAT_NAMES_MAP,
    })


@router.get("/api/league")
async def api_league_outliers(
    season: Optional[str] = Query(None),
    window: str = Query("week"),
    limit: int = Query(20, ge=1, le=200),
    db: Session = Depends(get_db)
):
    """API: Top outliers de liga.
    
    Args:
        season: Temporada (ej: "2024-25")
        window: Ventana temporal ('last_game', 'week', 'month')
        limit: Número máximo de resultados (1-200, default: 20)
    """
    query = _build_league_query(db, season or "2024-25", window)
    query = query.order_by(LeagueOutlier.percentile.desc())
    
    outliers = []
    for outlier, stats_row, player, game in query.limit(limit).all():
        top_features = []
        if outlier.feature_contributions:
            sorted_features = sorted(
                outlier.feature_contributions.items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:3]
            top_features = [f[0] for f in sorted_features]
        
        outliers.append({
            'id': outlier.id,
            'player_id': player.id,
            'player_name': player.full_name,
            'game_id': game.id,
            'game_date': game.date.isoformat() if game.date else None,
            'season': game.season,
            'pts': stats_row.pts,
            'reb': stats_row.reb,
            'ast': stats_row.ast,
            'percentile': round(outlier.percentile, 1),
            'reconstruction_error': round(outlier.reconstruction_error, 4),
            'top_features': top_features,
        })
    
    return JSONResponse(content={"data": outliers, "count": len(outliers)})


@router.get("/api/player")
async def api_player_outliers(
    season: Optional[str] = Query(None),
    window: str = Query("week"),
    outlier_type: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=200),
    db: Session = Depends(get_db)
):
    """API: Top outliers de jugador.
    
    Args:
        season: Temporada (ej: "2024-25")
        window: Ventana temporal ('last_game', 'week', 'month')
        outlier_type: Tipo de outlier ('explosion', 'crisis')
        limit: Número máximo de resultados (1-200, default: 20)
    """
    query = _build_player_query(db, season or "2024-25", window)
    
    outliers = []
    if window == 'last_game':
        query = query.order_by(func.abs(PlayerOutlier.max_z_score).desc())
        if outlier_type:
            query = query.filter(PlayerOutlier.outlier_type == outlier_type)
            
        for outlier, stats_row, player, game in query.limit(limit).all():
            features = sorted(outlier.outlier_features, key=lambda x: abs(x['z_score']), reverse=True)
            primary_feature = features[0] if features else None
            
            outliers.append({
                'id': outlier.id,
                'player_id': player.id,
                'player_name': player.full_name,
                'game_id': game.id,
                'game_date': game.date.isoformat() if game.date else None,
                'pts': stats_row.pts,
                'primary_val': primary_feature['val'] if primary_feature else stats_row.pts,
                'primary_feat': primary_feature['feature'] if primary_feature else 'pts',
                'primary_avg': primary_feature['avg'] if primary_feature else 0,
                'outlier_type': outlier.outlier_type,
                'max_z_score': round(outlier.max_z_score, 2),
                'outlier_features': features,
                'window': 'game'
            })
    else:
        if outlier_type:
            query = query.filter(PlayerTrendOutlier.outlier_type == outlier_type)
        
        query = query.order_by(func.abs(PlayerTrendOutlier.max_z_score).desc())
            
        for trend, player in query.limit(limit).all():
            sorted_feats = sorted(trend.z_scores.items(), key=lambda x: abs(x[1]), reverse=True)
            top_f = sorted_feats[0][0] if sorted_feats else None
            
            # Construir outlier_features con valores y sentimiento
            outlier_features = []
            for f, z in sorted_feats[:3]:
                comp = trend.comparison_data.get(f, {})
                outlier_features.append({
                    'feature': f,
                    'z_score': z,
                    'val': comp.get('current_avg', 0),
                    'sentiment': comp.get('sentiment', 'positive' if z > 0 else 'negative')
                })
            
            comp_top = trend.comparison_data.get(top_f, {}) if top_f else {}
            
            outliers.append({
                'id': trend.id,
                'player_id': player.id,
                'player_name': player.full_name,
                'reference_date': trend.reference_date.isoformat(),
                'outlier_type': trend.outlier_type,
                'max_z_score': round(trend.max_z_score, 2),
                'outlier_features': outlier_features,
                'comparison_data': trend.comparison_data,
                'primary_feat': top_f,
                'primary_val': comp_top.get('current_avg', 0),
                'primary_avg': comp_top.get('baseline_avg', 0),
                'window': window
            })
            
    return JSONResponse(content={"data": outliers, "count": len(outliers)})


@router.get("/api/stats")
async def api_stats(
    window: str = Query("week"),
    db: Session = Depends(get_db)
):
    """API: Estadísticas del sistema de outliers."""
    # Obtener la temporada más reciente automáticamente
    latest_season_row = db.query(Game.season).distinct().order_by(Game.season.desc()).first()
    season = latest_season_row[0] if latest_season_row else "2024-25"
    
    stats = _get_outlier_stats(db, season, window)
    return JSONResponse(content=stats)


# ============ Helper Functions ============

def _get_outlier_stats(db: Session, season: str, window: str = 'week') -> dict:
    """Obtiene estadísticas generales del sistema (filtrado por activos y ventana temporal)."""
    # Determinar rango de fechas según ventana
    latest_date = db.query(func.max(Game.date)).scalar()
    start_date = None
    
    if latest_date:
        if window == 'last_game':
            start_date = latest_date
        elif window == 'week':
            start_date = latest_date - timedelta(days=7)
        elif window == 'month':
            start_date = latest_date - timedelta(days=30)
    
    # Base query con filtro de activos
    # Usamos PlayerGameStats como punto de entrada para el join con Player
    base_stats_query = db.query(func.count(PlayerGameStats.id)).join(Player).filter(Player.is_active == True)
    
    if start_date:
        base_stats_query = base_stats_query.join(Game).filter(Game.date >= start_date)
    else:
        base_stats_query = base_stats_query.join(Game).filter(Game.season == season)
    
    total_stats = base_stats_query.scalar() or 0
    
    # Outliers de liga
    league_query = db.query(func.count(LeagueOutlier.id)).join(PlayerGameStats).join(Player).filter(
        and_(LeagueOutlier.is_outlier == True, Player.is_active == True)
    )
    if start_date:
        league_query = league_query.join(Game, PlayerGameStats.game_id == Game.id).filter(Game.date >= start_date)
    else:
        league_query = league_query.join(Game, PlayerGameStats.game_id == Game.id).filter(Game.season == season)
    league_count = league_query.scalar() or 0
    
    # Outliers de jugador (Z-score)
    if window == 'last_game':
        player_base_query = db.query(func.count(PlayerOutlier.id)).join(PlayerGameStats).join(Player).filter(
            Player.is_active == True
        )
        if start_date:
            player_base_query = player_base_query.join(Game, PlayerGameStats.game_id == Game.id).filter(Game.date >= start_date)
        else:
            player_base_query = player_base_query.join(Game, PlayerGameStats.game_id == Game.id).filter(Game.season == season)
        
        explosions = player_base_query.filter(PlayerOutlier.outlier_type == 'explosion').scalar() or 0
        crises = player_base_query.filter(PlayerOutlier.outlier_type == 'crisis').scalar() or 0
    else:
        # Tendencias (week/month)
        trend_query = db.query(func.count(PlayerTrendOutlier.id)).join(Player).filter(
            and_(
                Player.is_active == True,
                PlayerTrendOutlier.window_type == window
            )
        )
        if start_date:
            trend_query = trend_query.filter(PlayerTrendOutlier.reference_date >= start_date)
            
        explosions = trend_query.filter(PlayerTrendOutlier.outlier_type == 'explosion').scalar() or 0
        crises = trend_query.filter(PlayerTrendOutlier.outlier_type == 'crisis').scalar() or 0
    
    return {
        'total_stats': total_stats,
        'league_outliers': league_count,
        'player_outliers': explosions + crises,
        'explosions': explosions,
        'crises': crises,
    }


def _get_league_outliers(
    db: Session, 
    season: Optional[str], 
    limit: Optional[int] = 20,
    offset: Optional[int] = None,
    window: str = 'season'
) -> List[dict]:
    """Obtiene los top outliers de liga con filtros temporales y de activos."""
    query = (
        db.query(LeagueOutlier, PlayerGameStats, Player, Game)
        .join(PlayerGameStats, LeagueOutlier.player_game_stat_id == PlayerGameStats.id)
        .join(Player, PlayerGameStats.player_id == Player.id)
        .join(Game, PlayerGameStats.game_id == Game.id)
        .filter(LeagueOutlier.is_outlier == True)
        .filter(Player.is_active == True)
    )
    
    # Aplicar ventana temporal
    if window == 'last_game':
        latest_date = db.query(func.max(Game.date)).scalar()
        if latest_date:
            query = query.filter(Game.date == latest_date)
    elif window == 'week':
        latest_date = db.query(func.max(Game.date)).scalar()
        if latest_date:
            query = query.filter(Game.date >= latest_date - timedelta(days=7))
    elif window == 'month':
        latest_date = db.query(func.max(Game.date)).scalar()
        if latest_date:
            query = query.filter(Game.date >= latest_date - timedelta(days=30))
    elif season:
        query = query.filter(Game.season == season)
    
    query = query.order_by(LeagueOutlier.percentile.desc())
    
    if offset is not None:
        query = query.offset(offset)
    
    results = []
    for outlier, stats, player, game in query.limit(limit).all():
        # Obtener top features que contribuyeron
        top_features = []
        if outlier.feature_contributions:
            sorted_features = sorted(
                outlier.feature_contributions.items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:3]
            top_features = [f[0] for f in sorted_features]
        
        results.append({
            'id': outlier.id,
            'player_id': player.id,
            'player_name': player.full_name,
            'game_id': game.id,
            'game_date': game.date.isoformat() if game.date else None,
            'season': game.season,
            'pts': stats.pts,
            'reb': stats.reb,
            'ast': stats.ast,
            'percentile': round(outlier.percentile, 1),
            'reconstruction_error': round(outlier.reconstruction_error, 4),
            'top_features': top_features,
        })
    
    return results


def _get_player_outliers(
    db: Session, 
    season: Optional[str], 
    limit: Optional[int] = 20,
    offset: Optional[int] = None,
    window: str = 'week',
    outlier_type: Optional[str] = None
) -> List[dict]:
    """Obtiene los top outliers de jugador (Partido o Tendencia)."""
    # Si por alguna razón llega 'season', lo cambiamos a 'month' como fallback o lo ignoramos
    effective_window = window if window in ('last_game', 'week', 'month') else 'week'
    
    if effective_window == 'last_game':
        return _get_single_game_player_outliers(db, season, effective_window, outlier_type, limit, offset)
    else:
        return _get_trend_player_outliers(db, season, effective_window, outlier_type, limit, offset)


def _get_single_game_player_outliers(db, season, window, outlier_type, limit: Optional[int] = None, offset: Optional[int] = None):
    query = (
        db.query(PlayerOutlier, PlayerGameStats, Player, Game)
        .join(PlayerGameStats, PlayerOutlier.player_game_stat_id == PlayerGameStats.id)
        .join(Player, PlayerGameStats.player_id == Player.id)
        .join(Game, PlayerGameStats.game_id == Game.id)
        .filter(Player.is_active == True)
    )
    
    # Aplicar ventana temporal
    if window == 'last_game':
        latest_date = db.query(func.max(Game.date)).scalar()
        if latest_date:
            query = query.filter(Game.date == latest_date)
    elif season:
        query = query.filter(Game.season == season)
    
    if outlier_type:
        query = query.filter(PlayerOutlier.outlier_type == outlier_type)
    
    query = query.order_by(func.abs(PlayerOutlier.max_z_score).desc())
    
    if offset is not None:
        query = query.offset(offset)
        
    results = []
    for outlier, stats, player, game in query.limit(limit).all():
        # Encontrar la feature con el mayor Z-score absoluto para mostrarla como principal
        features = sorted(outlier.outlier_features, key=lambda x: abs(x['z_score']), reverse=True)
        primary_feature = features[0] if features else None
        
        results.append({
            'id': outlier.id,
            'player_id': player.id,
            'player_name': player.full_name,
            'game_id': game.id,
            'game_date': game.date.isoformat() if game.date else None,
            'pts': stats.pts,
            'primary_val': primary_feature['val'] if primary_feature else stats.pts,
            'primary_feat': primary_feature['feature'] if primary_feature else 'pts',
            'primary_avg': primary_feature['avg'] if primary_feature else 0,
            'outlier_type': outlier.outlier_type,
            'max_z_score': round(outlier.max_z_score, 2),
            'outlier_features': features,
            'window': 'game'
        })
    return results


def _get_trend_player_outliers(db, season, window, outlier_type, limit: Optional[int] = None, offset: Optional[int] = None):
    query = (
        db.query(PlayerTrendOutlier, Player)
        .join(Player, PlayerTrendOutlier.player_id == Player.id)
        .filter(and_(Player.is_active == True, PlayerTrendOutlier.window_type == window))
    )
    
    # Filtrar por fecha reciente
    latest_date = db.query(func.max(PlayerTrendOutlier.reference_date)).scalar()
    if latest_date:
        query = query.filter(PlayerTrendOutlier.reference_date >= latest_date - timedelta(days=7))
    
    if outlier_type:
        query = query.filter(PlayerTrendOutlier.outlier_type == outlier_type)
    
    query = query.order_by(func.abs(PlayerTrendOutlier.max_z_score).desc())
    
    if offset is not None:
        query = query.offset(offset)
        
    results = []
    for trend, player in query.limit(limit).all():
        # Encontrar la feature con el mayor Z-score absoluto
        sorted_feats = sorted(trend.z_scores.items(), key=lambda x: abs(x[1]), reverse=True)
        top_f = sorted_feats[0][0] if sorted_feats else None
        
        # Extraer valores de comparison_data para la métrica principal
        comp_top = trend.comparison_data.get(top_f, {}) if top_f else {}
        
        # Construir outlier_features con valores y sentimiento
        outlier_features = []
        for f, z in sorted_feats[:3]:
            comp = trend.comparison_data.get(f, {})
            outlier_features.append({
                'feature': f,
                'z_score': z,
                'val': comp.get('current_avg', 0),
                'sentiment': comp.get('sentiment', 'positive' if z > 0 else 'negative')
            })
        
        results.append({
            'id': trend.id,
            'player_id': player.id,
            'player_name': player.full_name,
            'reference_date': trend.reference_date.isoformat(),
            'outlier_type': trend.outlier_type,
            'max_z_score': round(trend.max_z_score, 2),
            'outlier_features': outlier_features,
            'comparison_data': trend.comparison_data,
            'primary_feat': top_f,
            'primary_val': comp_top.get('current_avg', 0),
            'primary_avg': comp_top.get('baseline_avg', 0),
            'window': window
        })
    return results
