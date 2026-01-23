"""Módulo de inferencia para detección de outliers de liga.

Proporciona funciones para detectar outliers en estadísticas de partidos
usando el modelo de autoencoder entrenado.
"""

import logging
from typing import List, Optional, Tuple, Dict, Any
from pathlib import Path

import numpy as np
from sqlalchemy import func
from sqlalchemy.orm import Session

from datetime import datetime, timedelta
from db.models import PlayerGameStats, Player, Game
from outliers.models import LeagueOutlier
from outliers.base import BaseDetector, OutlierResult
from outliers.ml.data_pipeline import DataPipeline, StandardScaler, MODELS_DIR

try:
    from outliers.ml.autoencoder import LeagueAnomalyDetector
    HAS_ML = True
except ImportError:
    LeagueAnomalyDetector = None
    HAS_ML = False

logger = logging.getLogger(__name__)

# Umbral de percentil para considerar outlier
DEFAULT_OUTLIER_PERCENTILE = 99.0


class LeagueOutlierDetector(BaseDetector):
    """Detector de outliers de liga usando autoencoder.
    
    Implementa BaseDetector para integración con el sistema de detección.
    """
    
    def __init__(self, percentile_threshold: float = DEFAULT_OUTLIER_PERCENTILE):
        """Inicializa el detector.
        
        Args:
            percentile_threshold: Percentil mínimo para considerar outlier
        """
        self.percentile_threshold = percentile_threshold
        self._model: Optional[LeagueAnomalyDetector] = None
        self._scaler: Optional[StandardScaler] = None
        self._pipeline: Optional[DataPipeline] = None
    
    def _ensure_model_loaded(self) -> bool:
        """Carga el modelo si existe."""
        if self._model is not None:
            return True
        
        if not HAS_ML:
            logger.warning("Sistema de ML no disponible (torch no instalado)")
            return False
            
        if not LeagueAnomalyDetector.exists():
            logger.warning("No hay modelo de autoencoder entrenado")
            return False
        
        try:
            self._model = LeagueAnomalyDetector.load()
            self._scaler = StandardScaler.load(MODELS_DIR / "scaler_global.joblib")
            return True
        except Exception as e:
            logger.error(f"Error cargando modelo: {e}")
            return False
    
    def detect(
        self,
        session: Session,
        game_stats: List[PlayerGameStats]
    ) -> List[OutlierResult]:
        """Detecta outliers en una lista de estadísticas.
        
        Args:
            session: Sesión de SQLAlchemy
            game_stats: Lista de estadísticas de partidos
            
        Returns:
            Lista de resultados de detección
        """
        if not self._ensure_model_loaded():
            return []
        
        if self._pipeline is None:
            self._pipeline = DataPipeline(session)
        
        results = []
        
        for stats in game_stats:
            try:
                result = self._detect_single(stats)
                if result is not None:
                    results.append(result)
            except Exception as e:
                logger.error(f"Error detectando outlier para stat_id={stats.id}: {e}")
                continue
        
        return results
    
    def _detect_single(self, stats: PlayerGameStats) -> Optional[OutlierResult]:
        """Detecta outlier en una sola línea estadística."""
        # Extraer features
        features = self._pipeline.extract_single(stats)
        if features is None:
            return None  # No cumple umbral de minutos
        
        # Normalizar
        features_normalized = self._scaler.transform(features.reshape(1, -1))
        
        # Predecir
        errors, percentiles, contributions = self._model.predict(features_normalized)
        
        error = float(errors[0])
        percentile = float(percentiles[0])
        contribution = contributions[0]
        
        is_outlier = percentile >= self.percentile_threshold
        
        return OutlierResult(
            player_game_stat_id=stats.id,
            is_outlier=is_outlier,
            outlier_data={
                'reconstruction_error': error,
                'percentile': percentile,
                'feature_contributions': contribution,
                'model_version': self._model.version,
            }
        )
    
    def backfill(
        self,
        session: Session,
        season: Optional[str] = None
    ) -> int:
        """Procesa datos históricos para detectar outliers.
        
        Args:
            session: Sesión de SQLAlchemy
            season: Temporada a procesar (None = todas)
            
        Returns:
            Número de outliers detectados
        """
        if not self._ensure_model_loaded():
            return 0
        
        self._pipeline = DataPipeline(session)
        
        # Obtener datos
        if season:
            data, stat_ids = self._pipeline.get_season_data(season)
        else:
            data, stat_ids = self._pipeline.get_all_historical_data()
        
        # Filtrar solo IDs de jugadores activos
        active_ids = {p_id for p_id, in session.query(Player.id).filter(Player.is_active == True).all()}
        
        # Necesitamos saber a qué jugador pertenece cada stat_id para filtrar 'data' y 'stat_ids'
        # Esto es un poco ineficiente aquí, pero necesario para cumplir el requisito de no procesar inactivos
        relevant_stats = session.query(PlayerGameStats.id, PlayerGameStats.player_id).filter(
            PlayerGameStats.id.in_(stat_ids)
        ).all()
        
        active_stat_map = {s_id for s_id, p_id in relevant_stats if p_id in active_ids}
        
        filtered_indices = [i for i, s_id in enumerate(stat_ids) if s_id in active_stat_map]
        
        if len(filtered_indices) < len(stat_ids):
            logger.info(f"Filtrando {len(stat_ids) - len(filtered_indices)} registros de jugadores inactivos")
            data = data[filtered_indices]
            stat_ids = [stat_ids[i] for i in filtered_indices]

        if len(data) == 0:
            logger.info("No hay datos para procesar")
            return 0
        
        logger.info(f"Procesando {len(data)} registros...")
        
        # Normalizar
        data_normalized = self._scaler.transform(data)
        
        # Predecir en batch
        errors, percentiles, contributions = self._model.predict(data_normalized)
        
        # Guardar resultados
        outlier_count = 0
        batch_size = 1000
        
        for i in range(0, len(stat_ids), batch_size):
            batch_ids = stat_ids[i:i + batch_size]
            batch_errors = errors[i:i + batch_size]
            batch_percentiles = percentiles[i:i + batch_size]
            batch_contributions = contributions[i:i + batch_size]
            
            for j, stat_id in enumerate(batch_ids):
                is_outlier = batch_percentiles[j] >= self.percentile_threshold
                
                # Verificar si ya existe
                existing = session.query(LeagueOutlier).filter(
                    LeagueOutlier.player_game_stat_id == stat_id
                ).first()
                
                if existing:
                    existing.reconstruction_error = float(batch_errors[j])
                    existing.percentile = float(batch_percentiles[j])
                    existing.feature_contributions = batch_contributions[j]
                    existing.is_outlier = is_outlier
                    existing.model_version = self._model.version
                else:
                    outlier = LeagueOutlier(
                        player_game_stat_id=stat_id,
                        reconstruction_error=float(batch_errors[j]),
                        percentile=float(batch_percentiles[j]),
                        feature_contributions=batch_contributions[j],
                        is_outlier=is_outlier,
                        model_version=self._model.version,
                    )
                    session.add(outlier)
                
                if is_outlier:
                    outlier_count += 1
            
            session.commit()
            
            if (i + batch_size) % 10000 == 0:
                logger.info(f"Procesados {i + batch_size}/{len(stat_ids)} registros...")
        
        logger.info(f"Backfill completado: {outlier_count} outliers detectados")
        return outlier_count


def detect_league_outliers(
    session: Session,
    game_stats: List[PlayerGameStats],
    persist: bool = True
) -> List[OutlierResult]:
    """Función de conveniencia para detectar outliers de liga.
    
    Args:
        session: Sesión de SQLAlchemy
        game_stats: Lista de estadísticas a analizar
        persist: Si True, guarda los resultados en la BD
        
    Returns:
        Lista de resultados
    """
    detector = LeagueOutlierDetector()
    results = detector.detect(session, game_stats)
    
    if persist and results:
        for result in results:
            if result.is_outlier:
                existing = session.query(LeagueOutlier).filter(
                    LeagueOutlier.player_game_stat_id == result.player_game_stat_id
                ).first()
                
                if existing:
                    existing.reconstruction_error = result.outlier_data['reconstruction_error']
                    existing.percentile = result.outlier_data['percentile']
                    existing.feature_contributions = result.outlier_data['feature_contributions']
                    existing.is_outlier = True
                    existing.model_version = result.outlier_data.get('model_version')
                else:
                    outlier = LeagueOutlier(
                        player_game_stat_id=result.player_game_stat_id,
                        reconstruction_error=result.outlier_data['reconstruction_error'],
                        percentile=result.outlier_data['percentile'],
                        feature_contributions=result.outlier_data['feature_contributions'],
                        is_outlier=True,
                        model_version=result.outlier_data.get('model_version'),
                    )
                    session.add(outlier)
        
        session.commit()
    
    return results


def get_top_outliers(
    session: Session,
    limit: int = 10,
    season: Optional[str] = None,
    window: str = 'season'  # 'last_game', 'week', 'month', 'season'
) -> List[Dict[str, Any]]:
    """Obtiene los outliers más extremos con enfoque periodístico.
    
    Args:
        session: Sesión de SQLAlchemy
        limit: Número máximo de resultados
        season: Filtrar por temporada (usado si window='season')
        window: Ventana temporal para el filtrado
        
    Returns:
        Lista de diccionarios con información del outlier
    """
    from db.models import Game, Player
    
    query = (
        session.query(LeagueOutlier, PlayerGameStats, Player, Game)
        .join(PlayerGameStats, LeagueOutlier.player_game_stat_id == PlayerGameStats.id)
        .join(Player, PlayerGameStats.player_id == Player.id)
        .join(Game, PlayerGameStats.game_id == Game.id)
        .filter(LeagueOutlier.is_outlier == True)
        .filter(Player.is_active == True)  # Solo jugadores activos
    )
    
    # Aplicar ventana temporal
    if window == 'last_game':
        latest_date = session.query(func.max(Game.date)).scalar()
        if latest_date:
            query = query.filter(Game.date == latest_date)
    elif window == 'week':
        latest_date = session.query(func.max(Game.date)).scalar()
        if latest_date:
            start_date = latest_date - timedelta(days=7)
            query = query.filter(Game.date >= start_date)
    elif window == 'month':
        latest_date = session.query(func.max(Game.date)).scalar()
        if latest_date:
            start_date = latest_date - timedelta(days=30)
            query = query.filter(Game.date >= start_date)
    elif season:
        query = query.filter(Game.season == season)
    
    query = query.order_by(LeagueOutlier.percentile.desc())
    query = query.limit(limit)
    
    results = []
    for outlier, stats, player, game in query.all():
        results.append({
            'player_name': player.full_name,
            'game_date': game.date.isoformat() if game.date else None,
            'season': game.season,
            'pts': stats.pts,
            'reb': stats.reb,
            'ast': stats.ast,
            'percentile': outlier.percentile,
            'reconstruction_error': outlier.reconstruction_error,
            'top_features': _get_top_features(outlier.feature_contributions, 3),
        })
    
    return results


def _get_top_features(contributions: Dict[str, float], n: int = 3) -> List[str]:
    """Obtiene las N features que más contribuyen al error."""
    if not contributions:
        return []
    sorted_features = sorted(contributions.items(), key=lambda x: x[1], reverse=True)
    return [f[0] for f in sorted_features[:n]]
