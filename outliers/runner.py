"""Orquestador para ejecutar los tres detectores de outliers.

Ejecuta en secuencia:
1. LeagueOutlierDetector (autoencoder) - si el modelo está entrenado
2. PlayerZScoreDetector (Z-scores individuales)
3. StreakDetector (rachas históricas)
"""

import logging
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy.orm import Session

from db.models import PlayerGameStats, Player, Game
from outliers.base import OutlierResult
from outliers.models import StreakAllTimeRecord, PlayerSeasonState
from outliers.ml.inference import LeagueOutlierDetector
from outliers.stats.player_zscore import PlayerZScoreDetector
from outliers.stats.streaks import StreakDetector

logger = logging.getLogger(__name__)


@dataclass
class DetectionResults:
    """Resultados consolidados de la detección de outliers."""
    
    league_results: List[OutlierResult] = field(default_factory=list)
    player_results: List[OutlierResult] = field(default_factory=list)
    streak_results: List[OutlierResult] = field(default_factory=list)
    
    # Estadísticas
    total_processed: int = 0
    league_outliers: int = 0
    player_outliers: int = 0
    streak_outliers: int = 0
    
    # Metadata
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    errors: List[str] = field(default_factory=list)
    
    @property
    def total_outliers(self) -> int:
        """Total de outliers detectados."""
        return self.league_outliers + self.player_outliers + self.streak_outliers
    
    @property
    def duration_seconds(self) -> float:
        """Duración de la detección en segundos."""
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convierte a diccionario para serialización."""
        return {
            'total_processed': self.total_processed,
            'league_outliers': self.league_outliers,
            'player_outliers': self.player_outliers,
            'streak_outliers': self.streak_outliers,
            'total_outliers': self.total_outliers,
            'duration_seconds': round(self.duration_seconds, 2),
            'errors': self.errors,
        }


class OutlierRunner:
    """Ejecuta los tres detectores de outliers en secuencia."""
    
    def __init__(
        self,
        run_league: bool = True,
        run_player: bool = True,
        run_streaks: bool = True,
        league_percentile: float = 99.0,
        player_z_threshold: float = 2.5,
        streak_types: Optional[List[str]] = None
    ):
        """Inicializa el runner.
        
        Args:
            run_league: Ejecutar detector de liga (autoencoder)
            run_player: Ejecutar detector de Z-score individual
            run_streaks: Ejecutar detector de rachas
            league_percentile: Umbral de percentil para outliers de liga
            player_z_threshold: Umbral de Z-score para outliers de jugador
            streak_types: Tipos de racha a rastrear (None = todos)
        """
        self.run_league = run_league
        self.run_player = run_player
        self.run_streaks = run_streaks
        
        # Inicializar detectores
        self._league_detector: Optional[LeagueOutlierDetector] = None
        self._player_detector: Optional[PlayerZScoreDetector] = None
        self._streak_detector: Optional[StreakDetector] = None
        
        if run_league:
            self._league_detector = LeagueOutlierDetector(
                percentile_threshold=league_percentile
            )
        
        if run_player:
            self._player_detector = PlayerZScoreDetector(
                z_threshold=player_z_threshold
            )
        
        if run_streaks:
            self._streak_detector = StreakDetector(
                streak_types=streak_types
            )
    
    def detect(
        self,
        session: Session,
        game_stats: List[PlayerGameStats]
    ) -> DetectionResults:
        """Ejecuta la detección en una lista de estadísticas.
        
        Args:
            session: Sesión de SQLAlchemy
            game_stats: Lista de estadísticas a procesar
            
        Returns:
            Resultados consolidados
        """
        results = DetectionResults(
            started_at=datetime.now(),
            total_processed=len(game_stats)
        )
        
        # Auto-inicialización: Si los récords All-Time están vacíos, ejecutar backfill rápido
        if self.run_streaks and session.query(StreakAllTimeRecord).count() == 0:
            logger.info("Detectada tabla de récords vacía. Iniciando auto-backfill...")
            self._streak_detector.backfill(session)
            
        # Auto-inicialización: Si los estados de temporada están vacíos, ejecutar backfill
        if self.run_player and session.query(PlayerSeasonState).count() == 0:
            logger.info("Detectada tabla de estados vacía. Iniciando backfill de Z-scores...")
            self._player_detector.backfill(session)
            
        if not game_stats:
            results.finished_at = datetime.now()
            return results
        
        # Filtrar solo estadísticas de jugadores activos para ahorrar cómputo
        active_player_ids = {
            p_id for p_id, in session.query(Player.id).filter(Player.is_active == True).all()
        }
        original_count = len(game_stats)
        game_stats = [s for s in game_stats if s.player_id in active_player_ids]
        
        if len(game_stats) < original_count:
            logger.info(f"Filtrados {original_count - len(game_stats)} registros de jugadores no activos.")
        
        if not game_stats:
            logger.info("No hay estadísticas de jugadores activos para procesar.")
            results.finished_at = datetime.now()
            return results
        
        # 1. Detector de Liga (autoencoder)
        if self._league_detector:
            try:
                logger.info("Ejecutando detector de liga (autoencoder)...")
                league_results = self._league_detector.detect(session, game_stats)
                results.league_results = league_results
                results.league_outliers = sum(1 for r in league_results if r.is_outlier)
                logger.info(f"Liga: {results.league_outliers} outliers de {len(game_stats)}")
            except Exception as e:
                error_msg = f"Error en detector de liga: {e}"
                logger.error(error_msg)
                results.errors.append(error_msg)
        
        # 2. Detector de Jugador (Z-score y Tendencias)
        if self._player_detector:
            try:
                logger.info("Ejecutando detector de Z-score y tendencias...")
                # El método detect ahora gestiona internamente el disparo de tendencias
                player_results = self._player_detector.detect(session, game_stats)
                results.player_results = player_results
                results.player_outliers = sum(1 for r in player_results if r.is_outlier)
                logger.info(f"Jugador: {results.player_outliers} outliers de partido detectados.")
            except Exception as e:
                error_msg = f"Error en detector de jugador: {e}"
                logger.error(error_msg)
                results.errors.append(error_msg)
        
        # 3. Detector de Rachas
        if self._streak_detector:
            try:
                logger.info("Ejecutando detector de rachas...")
                streak_results = self._streak_detector.detect(session, game_stats)
                results.streak_results = streak_results
                results.streak_outliers = len(streak_results)  # Todas son notables
                logger.info(f"Rachas: {results.streak_outliers} notables de {len(game_stats)}")
            except Exception as e:
                error_msg = f"Error en detector de rachas: {e}"
                logger.error(error_msg)
                results.errors.append(error_msg)
        
        results.finished_at = datetime.now()
        logger.info(
            f"Detección completada: {results.total_outliers} outliers "
            f"en {results.duration_seconds:.2f}s"
        )
        
        return results
    
    def backfill(
        self,
        session: Session,
        season: Optional[str] = None
    ) -> DetectionResults:
        """Procesa datos históricos para todos los detectores.
        
        Args:
            session: Sesión de SQLAlchemy
            season: Temporada a procesar (None = todas)
            
        Returns:
            Resultados consolidados
        """
        results = DetectionResults(started_at=datetime.now())
        
        logger.info(f"Iniciando backfill para temporada: {season or 'todas'}")
        
        # 1. Backfill de Liga
        if self._league_detector:
            try:
                logger.info("Backfill: detector de liga...")
                league_count = self._league_detector.backfill(session, season)
                results.league_outliers = league_count
            except Exception as e:
                error_msg = f"Error en backfill de liga: {e}"
                logger.error(error_msg)
                results.errors.append(error_msg)
        
        # 2. Backfill de Jugador
        if self._player_detector:
            try:
                logger.info("Backfill: detector de Z-score...")
                player_count = self._player_detector.backfill(session, season)
                results.player_outliers = player_count
            except Exception as e:
                error_msg = f"Error en backfill de jugador: {e}"
                logger.error(error_msg)
                results.errors.append(error_msg)
        
        # 3. Backfill de Rachas
        if self._streak_detector:
            try:
                logger.info("Backfill: detector de rachas...")
                streak_count = self._streak_detector.backfill(session, season)
                results.streak_outliers = streak_count
            except Exception as e:
                error_msg = f"Error en backfill de rachas: {e}"
                logger.error(error_msg)
                results.errors.append(error_msg)
        
        results.finished_at = datetime.now()
        logger.info(
            f"Backfill completado: {results.total_outliers} outliers "
            f"en {results.duration_seconds:.2f}s"
        )
        
        return results


def run_detection_for_games(
    session: Session,
    game_stats: List[PlayerGameStats],
    skip_league: bool = False,
    skip_player: bool = False,
    skip_streaks: bool = False
) -> DetectionResults:
    """Función de conveniencia para ejecutar detección.
    
    Args:
        session: Sesión de SQLAlchemy
        game_stats: Lista de estadísticas
        skip_league: Omitir detector de liga
        skip_player: Omitir detector de jugador
        skip_streaks: Omitir detector de rachas
        
    Returns:
        Resultados de detección
    """
    runner = OutlierRunner(
        run_league=not skip_league,
        run_player=not skip_player,
        run_streaks=not skip_streaks
    )
    return runner.detect(session, game_stats)


def run_backfill(
    session: Session,
    season: Optional[str] = None,
    skip_league: bool = False,
    skip_player: bool = False,
    skip_streaks: bool = False
) -> DetectionResults:
    """Función de conveniencia para ejecutar backfill.
    
    Args:
        session: Sesión de SQLAlchemy
        season: Temporada a procesar
        skip_league: Omitir detector de liga
        skip_player: Omitir detector de jugador
        skip_streaks: Omitir detector de rachas
        
    Returns:
        Resultados de backfill
    """
    runner = OutlierRunner(
        run_league=not skip_league,
        run_player=not skip_player,
        run_streaks=not skip_streaks
    )
    return runner.backfill(session, season)
