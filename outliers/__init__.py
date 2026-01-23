"""Sistema de detección de outliers para estadísticas NBA.

Este módulo implementa tres tipos de detectores:
1. LeagueDetector: Autoencoder para detectar rendimientos atípicos vs toda la liga
2. PlayerDetector: Z-scores para detectar desviaciones del rendimiento personal
3. StreakDetector: Seguimiento de rachas históricas excepcionales
"""

from outliers.models import LeagueOutlier, PlayerOutlier, StreakRecord
from outliers.base import BaseDetector, OutlierResult

# Statistical detectors
from outliers.stats import (
    PlayerZScoreDetector,
    detect_player_outliers,
    StreakDetector,
    StreakCriteria,
    get_streak_summary,
    Z_SCORE_THRESHOLD,
)

# Runner
from outliers.runner import (
    OutlierRunner,
    DetectionResults,
    run_detection_for_games,
    run_backfill,
)

__all__ = [
    # Models
    'LeagueOutlier',
    'PlayerOutlier', 
    'StreakRecord',
    # Base classes
    'BaseDetector',
    'OutlierResult',
    # Player Z-score detector
    'PlayerZScoreDetector',
    'detect_player_outliers',
    'Z_SCORE_THRESHOLD',
    # Streak detector
    'StreakDetector',
    'StreakCriteria',
    'get_streak_summary',
    # Runner
    'OutlierRunner',
    'DetectionResults',
    'run_detection_for_games',
    'run_backfill',
]
