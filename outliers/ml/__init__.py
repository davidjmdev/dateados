"""Submódulo de Machine Learning para detección de outliers.

Contiene:
- data_pipeline: Extracción y normalización de datos
- autoencoder: Modelo de detección de outliers de liga
- train: CLI para entrenamiento
- inference: Detección en tiempo real
"""

from outliers.ml.data_pipeline import (
    DataPipeline,
    StandardScaler,
    STAT_FEATURES,
    get_feature_names,
)
from outliers.ml.autoencoder import LeagueAnomalyDetector
from outliers.ml.inference import (
    LeagueOutlierDetector,
    detect_league_outliers,
    get_top_outliers,
)

__all__ = [
    'DataPipeline',
    'StandardScaler',
    'STAT_FEATURES',
    'get_feature_names',
    'LeagueAnomalyDetector',
    'LeagueOutlierDetector',
    'detect_league_outliers',
    'get_top_outliers',
]
