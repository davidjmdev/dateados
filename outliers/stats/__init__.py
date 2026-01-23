"""Submódulo de detectores estadísticos.

Contiene:
- player_zscore: Detector de explosiones/crisis individuales (Z-score)
- streaks: Detector de rachas históricas
"""

from outliers.stats.player_zscore import (
    PlayerZScoreDetector,
    detect_player_outliers,
    Z_SCORE_THRESHOLD,
)

from outliers.stats.streaks import (
    StreakDetector,
    StreakCriteria,
    get_streak_summary,
)

__all__ = [
    # Player Z-score detector
    'PlayerZScoreDetector',
    'detect_player_outliers',
    'Z_SCORE_THRESHOLD',
    # Streak detector
    'StreakDetector',
    'StreakCriteria',
    'get_streak_summary',
]
