"""Detector de outliers por Z-score individual del jugador y tendencias (Optimizado).

Utiliza un enfoque de estadística acumulativa (sumatorios) para calcular
la media y desviación estándar en tiempo real, permitiendo un procesamiento
ultra-rápido apto para ejecución diaria.

Gestión de Rookies:
- Solo se analizan jugadores tras 30 días desde su primer partido en la BD.
"""

import logging
from typing import List, Optional, Dict, Any, Tuple
from datetime import date, timedelta
import math

import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, or_

from db.models import PlayerGameStats, Game, Player
from outliers.base import BaseDetector, OutlierResult
from outliers.models import PlayerOutlier, PlayerTrendOutlier, PlayerSeasonState

logger = logging.getLogger(__name__)

# Configuración
Z_SCORE_THRESHOLD = 2.0
MIN_MINUTES_THRESHOLD = 15.0
ROOKIE_GRACE_PERIOD_DAYS = 30
MIN_GAMES_FOR_BASELINE = 5

# Estadísticas para análisis
ANALYSIS_FEATURES = [
    'pts', 'ast', 'reb', 'stl', 'blk', 'tov',
    'fga', 'fta', 'fg3a', 'fg_pct', 'fg3_pct', 'ft_pct'
]

# Valores mínimos para considerar una explosión como relevante (evitar ruido)
FEATURE_MIN_VALUES = {
    'pts': 10, 'ast': 4, 'reb': 5, 'stl': 2, 'blk': 2, 'tov': 4,
    'fga': 5, 'fta': 4, 'fg3a': 3, 'fg_pct': 0.4, 'fg3_pct': 0.3, 'ft_pct': 0.5
}

# Mapeo de porcentajes a sus campos de intentos
PERCENTAGE_ATTEMPTS_MAP = {
    'fg_pct': 'fga',
    'fg3_pct': 'fg3a',
    'ft_pct': 'fta'
}

# Exportar para compatibilidad
ZSCORE_FEATURES = ANALYSIS_FEATURES
MIN_GAMES_REQUIRED = 10

class PlayerZScoreDetector(BaseDetector):
    def __init__(self, z_threshold: float = Z_SCORE_THRESHOLD):
        self.z_threshold = z_threshold

    def detect(self, session: Session, game_stats: List[PlayerGameStats]) -> List[OutlierResult]:
        """Detección incremental (diaria)."""
        results = []
        if not game_stats: return results
        
        # Agrupar por jugador para una sola carga de estado
        by_player = {}
        for s in game_stats:
            if s.player_id not in by_player: by_player[s.player_id] = []
            by_player[s.player_id].append(s)
            
        for p_id, stats_list in by_player.items():
            # Ordenar por fecha por si acaso
            stats_list = self._sort_by_date(session, stats_list)
            
            for stats in stats_list:
                game = session.query(Game).filter(Game.id == stats.game_id).first()
                if not game: continue
                
                # Obtener o crear estado
                state = self._get_or_create_state(session, p_id, game.season)
                
                # 1. Detectar outlier de partido (usando estado previo)
                res = self._detect_game_with_state(session, stats, game, state)
                if res:
                    results.append(res)
                    if res.is_outlier:
                        self._persist_single_outlier(session, res)
                
                # 2. Actualizar estado con este partido
                self._update_state_with_game(state, stats, game)
            
            session.commit()
            
            # 3. Detectar tendencias (Semana/Mes) al final del proceso del jugador
            last_stats = stats_list[-1]
            last_game = session.query(Game).filter(Game.id == last_stats.game_id).first()
            if last_game:
                self.detect_trends(session, last_game.date, [p_id])

        return results

    def _get_or_create_state(self, session: Session, player_id: int, season: str) -> PlayerSeasonState:
        state = session.query(PlayerSeasonState).filter_by(player_id=player_id, season=season).first()
        if not state:
            state = PlayerSeasonState(player_id=player_id, season=season, accumulated_stats={}, games_played=0)
            session.add(state)
        return state

    def _update_state_with_game(self, state: PlayerSeasonState, stats: PlayerGameStats, game: Game):
        """Actualiza los sumatorios del estado."""
        mins = self._get_minutes_float(stats)
        if mins <= 0: return # DNP o no jugó
        
        if not state.first_game_date: state.first_game_date = game.date
        state.last_game_date = game.date
        
        if state.games_played is None: state.games_played = 0
        state.games_played += 1
        
        # Crear copia del diccionario para asegurar que SQLAlchemy detecte el cambio
        acc = dict(state.accumulated_stats) if state.accumulated_stats else {}
        for feat in ANALYSIS_FEATURES:
            val = getattr(stats, feat)
            if val is None: val = 0.0
            
            # Para porcentajes, solo acumulamos si hubo intentos
            if feat in PERCENTAGE_ATTEMPTS_MAP:
                attempts_feat = PERCENTAGE_ATTEMPTS_MAP[feat]
                attempts = getattr(stats, attempts_feat) or 0
                if attempts <= 0:
                    continue
            
            sum_key = feat
            sum_sq_key = f"{feat}_sq"
            count_key = f"{feat}_count"
            
            acc[sum_key] = acc.get(sum_key, 0.0) + float(val)
            acc[sum_sq_key] = acc.get(sum_sq_key, 0.0) + float(val)**2
            acc[count_key] = acc.get(count_key, 0) + 1
            
        state.accumulated_stats = acc

    def _detect_game_with_state(self, session: Session, stats: PlayerGameStats, game: Game, state: PlayerSeasonState) -> Optional[OutlierResult]:
        """Calcula Z-Score usando el estado acumulado (Baseline)."""
        if self._get_minutes_float(stats) < MIN_MINUTES_THRESHOLD: return None
        
        games_played = state.games_played or 0
        if games_played < MIN_GAMES_FOR_BASELINE: return None
        
        # Validar periodo de gracia para rookies
        if state.first_game_date:
            try:
                days_since_debut = (game.date - state.first_game_date).days
                if days_since_debut < ROOKIE_GRACE_PERIOD_DAYS or games_played < 20:
                    # Si es veterano (tiene historial previo), saltamos esta restricción
                    # Buscamos si debutó antes de esta temporada
                    has_prior = session.query(PlayerSeasonState.player_id).filter(
                        and_(PlayerSeasonState.player_id == state.player_id, PlayerSeasonState.season < state.season)
                    ).first() is not None
                    
                    if not has_prior:
                        return None
            except Exception as e:
                logger.warning(f"Error calculando madurez para player {state.player_id}: {e}")
                return None

        z_scores = {}
        n = games_played
        acc = state.accumulated_stats or {}
        
        outlier_features = []
        max_z = 0.0
        
        for feat in ANALYSIS_FEATURES:
            sum_x = acc.get(feat, 0.0)
            sum_x2 = acc.get(f"{feat}_sq", 0.0)
            n_feat = acc.get(f"{feat}_count", 0)
            
            if n_feat < MIN_GAMES_FOR_BASELINE:
                continue
            
            mean = sum_x / n_feat
            var = (sum_x2 / n_feat) - (mean ** 2)
            
            # Suelo de varianza aumentado para estabilidad (evitar explosiones por ruido)
            # Para porcentajes usamos un suelo menor (0.01)
            var_floor = 0.01 if feat in PERCENTAGE_ATTEMPTS_MAP else 0.2
            std = math.sqrt(max(var_floor, var))
            
            val = getattr(stats, feat)
            if val is None: val = 0.0
            
            z = (float(val) - mean) / std
            z_scores[feat] = round(z, 2)
            
            # Solo considerar outlier si supera el umbral Z Y el valor mínimo de relevancia
            is_relevant = True
            if feat in FEATURE_MIN_VALUES and float(val) < FEATURE_MIN_VALUES[feat]:
                is_relevant = False
            
            if abs(z) > self.z_threshold and is_relevant:
                outlier_features.append({
                    'feature': feat, 
                    'z_score': round(z, 2), 
                    'direction': 'high' if z > 0 else 'low',
                    'val': round(float(val), 3) if feat in PERCENTAGE_ATTEMPTS_MAP else int(val),
                    'avg': round(mean, 3) if feat in PERCENTAGE_ATTEMPTS_MAP else round(mean, 2)
                })
                if abs(z) > abs(max_z): max_z = z

        if not outlier_features: return None
        
        return OutlierResult(
            player_game_stat_id=stats.id,
            is_outlier=True,
            outlier_data={
                'z_scores': z_scores,
                'max_z_score': round(max_z, 2),
                'outlier_type': 'explosion' if max_z > 0 else 'crisis',
                'outlier_features': outlier_features,
                'games_in_sample': games_played
            }
        )

    def detect_trends(self, session: Session, ref_date: date, player_ids: List[int]) -> None:
        """Detección de tendencias (ventana de 7 y 30 días)."""
        for p_id in player_ids:
            for window_days in [7, 30]:
                self._detect_trend_window(session, p_id, ref_date, window_days)

    def _detect_trend_window(self, session: Session, player_id: int, ref_date: date, days: int):
        window_type = 'week' if days == 7 else 'month'
        start_window = ref_date - timedelta(days=days)
        
        # 1. Obtener partidos de la ventana
        window_stats = session.query(PlayerGameStats).join(Game).filter(
            PlayerGameStats.player_id == player_id,
            Game.date > start_window,
            Game.date <= ref_date
        ).all()
        
        if not window_stats: return

        # Filtrar partidos donde no jugó minutos (DNP/0 min)
        window_stats = [s for s in window_stats if self._get_minutes_float(s) > 0]
        
        if len(window_stats) < (2 if days == 7 else 5): return

        # 2. Obtener baseline desde el estado acumulado
        game_ref = session.query(Game).filter(Game.date == ref_date).first()
        if not game_ref: return
        state = session.query(PlayerSeasonState).filter_by(player_id=player_id, season=game_ref.season).first()
        
        if not state or state.games_played < MIN_GAMES_FOR_BASELINE: return

        # 3. Calcular Z-Scores de tendencia
        z_dict = {}
        comp_dict = {}
        n_w = len(window_stats)
        n_b = state.games_played
        acc = state.accumulated_stats
        
        max_z = 0.0
        
        for feat in ANALYSIS_FEATURES:
            sum_x = acc.get(feat, 0.0)
            sum_x2 = acc.get(f"{feat}_sq", 0.0)
            n_b_feat = acc.get(f"{feat}_count", 0)
            
            if n_b_feat < MIN_GAMES_FOR_BASELINE:
                continue
            
            mu_b = sum_x / n_b_feat
            var_b = (sum_x2 / n_b_feat) - (mu_b ** 2)
            
            var_floor = 0.01 if feat in PERCENTAGE_ATTEMPTS_MAP else 0.2
            sigma_b = math.sqrt(max(var_floor, var_b))
            
            vals_w = [getattr(s, feat) for s in window_stats]
            # Si es porcentaje, filtrar solo partidos con intentos en la ventana también
            if feat in PERCENTAGE_ATTEMPTS_MAP:
                attempts_feat = PERCENTAGE_ATTEMPTS_MAP[feat]
                vals_w = [getattr(s, feat) for s in window_stats if (getattr(s, attempts_feat) or 0) > 0]
            
            if not vals_w: continue
            
            vals_w = [float(v) if v is not None else 0.0 for v in vals_w]
            mu_w = float(np.mean(vals_w))
            n_w_feat = len(vals_w)
            
            standard_error = sigma_b / math.sqrt(n_w_feat)
            z = (mu_w - mu_b) / standard_error
            
            if abs(z) > self.z_threshold:
                z_dict[feat] = round(float(z), 2)
                comp_dict[feat] = {
                    "current_avg": round(mu_w, 3) if feat in PERCENTAGE_ATTEMPTS_MAP else round(mu_w, 2),
                    "baseline_avg": round(mu_b, 3) if feat in PERCENTAGE_ATTEMPTS_MAP else round(mu_b, 2),
                    "diff_pct": round(((mu_w/mu_b)-1)*100, 1) if mu_b > 0 else 0
                }
                if abs(z) > abs(max_z): max_z = z

        if abs(max_z) > self.z_threshold:
            self._persist_trend_outlier(session, player_id, window_type, ref_date, z_dict, max_z, comp_dict, n_w, n_b)

    def _persist_single_outlier(self, session: Session, result: OutlierResult):
        data = result.outlier_data
        existing = session.query(PlayerOutlier).filter_by(player_game_stat_id=result.player_game_stat_id).first()
        if existing:
            existing.z_scores = data['z_scores']
            existing.max_z_score = data['max_z_score']
            existing.outlier_type = data['outlier_type']
            existing.outlier_features = data['outlier_features']
            existing.games_in_sample = data['games_in_sample']
        else:
            session.add(PlayerOutlier(player_game_stat_id=result.player_game_stat_id, **data))

    def _persist_trend_outlier(self, session: Session, p_id: int, w_type: str, ref_date: date, z: dict, max_z: float, comp: dict, n_w: int, n_b: int):
        existing = session.query(PlayerTrendOutlier).filter_by(player_id=p_id, window_type=w_type, reference_date=ref_date).first()
        out_type = 'explosion' if max_z > 0 else 'crisis'
        if existing:
            existing.max_z_score = max_z
            existing.outlier_type = out_type
            existing.z_scores = z
            existing.comparison_data = comp
            existing.games_in_window = n_w
            existing.games_in_baseline = n_b
        else:
            session.add(PlayerTrendOutlier(
                player_id=p_id, window_type=w_type, reference_date=ref_date,
                z_scores=z, max_z_score=max_z, outlier_type=out_type,
                comparison_data=comp, games_in_window=n_w, games_in_baseline=n_b
            ))

    def backfill(self, session: Session, season: Optional[str] = None) -> int:
        """Backfill ultra-rápido O(N) por jugador."""
        if not season:
            latest = session.query(Game.season).order_by(Game.date.desc()).first()
            season = latest[0] if latest else "2024-25"
            
        logger.info(f"Iniciando backfill optimizado para {season}...")
        
        # Limpiar estados previos
        session.query(PlayerSeasonState).filter_by(season=season).delete()
        
        # Eliminar outliers de jugador por ID para evitar problemas con JOIN en delete
        outlier_ids = [o[0] for o in session.query(PlayerOutlier.id).join(PlayerGameStats).join(Game).filter(Game.season == season).all()]
        if outlier_ids:
            session.query(PlayerOutlier).filter(PlayerOutlier.id.in_(outlier_ids)).delete(synchronize_session=False)
        
        # Tendencias: Limpiar solo las de la temporada procesada
        start_season = date(int(season.split('-')[0]), 10, 1)
        session.query(PlayerTrendOutlier).filter(PlayerTrendOutlier.reference_date >= start_season).delete(synchronize_session=False)
        session.commit()

        players = session.query(Player.id).filter(Player.is_active == True).all()
        outliers_found = 0
        
        for p_row in players:
            p_id = p_row[0]
            stats_list = session.query(PlayerGameStats, Game).join(Game).filter(
                PlayerGameStats.player_id == p_id, Game.season == season
            ).order_by(Game.date).all()
            
            if not stats_list: continue
            
            state = self._get_or_create_state(session, p_id, season)
            for stats, game in stats_list:
                res = self._detect_game_with_state(session, stats, game, state)
                if res and res.is_outlier:
                    session.add(PlayerOutlier(player_game_stat_id=stats.id, **res.outlier_data))
                    outliers_found += 1
                
                self._update_state_with_game(state, stats, game)
            
            # Tendencias al final de su historial
            last_date = stats_list[-1][1].date
            self.detect_trends(session, last_date, [p_id])
            session.commit()

        return outliers_found

    def _sort_by_date(self, session: Session, stats_list: List[PlayerGameStats]) -> List[PlayerGameStats]:
        game_ids = [s.game_id for s in stats_list]
        games = session.query(Game.id, Game.date).filter(Game.id.in_(game_ids)).all()
        game_dates = {g.id: g.date for g in games}
        return sorted(stats_list, key=lambda s: game_dates.get(s.game_id, date.min))

    def _get_minutes_float(self, stats: PlayerGameStats) -> float:
        m = getattr(stats, 'min', None)
        if m is None: return 0.0
        from datetime import timedelta
        if isinstance(m, timedelta): return m.total_seconds() / 60.0
        try:
            s = str(m)
            if ':' in s:
                parts = s.split(':')
                return float(parts[0]) + float(parts[1])/60.0
            return float(s)
        except: return 0.0

def detect_player_outliers(session: Session, game_stats: List[PlayerGameStats]) -> List[OutlierResult]:
    detector = PlayerZScoreDetector()
    return detector.detect(session, game_stats)
