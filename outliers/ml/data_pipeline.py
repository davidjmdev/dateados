"""Pipeline de datos para el autoencoder de detección de outliers.

Extrae y normaliza las 14 variables estadísticas principales de player_game_stats
para alimentar el entrenamiento del modelo de detección de outliers de liga.

Variables estadísticas:
- pts, ast, reb, stl, blk, tov, pf (core stats)
- fg_pct, fg3_pct, ft_pct (porcentajes de tiro)
- fga, fta, fg3a (intentos)
- min (minutos jugados)
"""

import logging
from pathlib import Path
from typing import Tuple, Optional, List
from datetime import timedelta

import numpy as np
import joblib

from sqlalchemy.orm import Session
from sqlalchemy import and_

from db.models import PlayerGameStats, Game

logger = logging.getLogger(__name__)

# Directorio para guardar scalers
MODELS_DIR = Path(__file__).parent / 'models'
MODELS_DIR.mkdir(parents=True, exist_ok=True)

# Variables estadísticas a extraer (14 features)
STAT_FEATURES = [
    'pts', 'ast', 'reb', 'stl', 'blk', 'tov', 'pf',
    'fg_pct', 'fg3_pct', 'ft_pct',
    'fga', 'fta', 'fg3a',
    'min'
]

# Umbral mínimo de minutos para incluir un partido (evitar ruido estadístico)
MIN_MINUTES_THRESHOLD = 5.0


class StandardScaler:
    """Normalizador estadístico compatible con scikit-learn.
    
    Transforma los datos restando la media y dividiendo por la desviación estándar.
    Implementación propia usando NumPy para evitar dependencia de scikit-learn.
    """
    
    def __init__(self):
        self.mean_: Optional[np.ndarray] = None
        self.std_: Optional[np.ndarray] = None
        self.n_features_: Optional[int] = None
        self._is_fitted: bool = False
    
    def fit(self, X: np.ndarray) -> 'StandardScaler':
        """Calcula la media y desviación estándar de los datos.
        
        Args:
            X: Array de forma (n_samples, n_features)
            
        Returns:
            self
        """
        X = np.asarray(X, dtype=np.float64)
        self.mean_ = X.mean(axis=0)
        self.std_ = X.std(axis=0)
        # Evitar división por cero
        self.std_ = np.where(self.std_ == 0, 1.0, self.std_)
        self.n_features_ = X.shape[1]
        self._is_fitted = True
        return self
    
    def transform(self, X: np.ndarray) -> np.ndarray:
        """Aplica la normalización a los datos.
        
        Args:
            X: Array de forma (n_samples, n_features)
            
        Returns:
            Array normalizado
        """
        if not self._is_fitted:
            raise RuntimeError("Scaler no ha sido ajustado. Llama a fit() primero.")
        X = np.asarray(X, dtype=np.float64)
        return (X - self.mean_) / self.std_
    
    def fit_transform(self, X: np.ndarray) -> np.ndarray:
        """Ajusta el scaler y transforma los datos en un solo paso."""
        return self.fit(X).transform(X)
    
    def inverse_transform(self, X: np.ndarray) -> np.ndarray:
        """Revierte la normalización."""
        if not self._is_fitted:
            raise RuntimeError("Scaler no ha sido ajustado. Llama a fit() primero.")
        X = np.asarray(X, dtype=np.float64)
        return X * self.std_ + self.mean_
    
    def save(self, path: Path) -> None:
        """Guarda el scaler en disco."""
        joblib.dump({
            'mean_': self.mean_,
            'std_': self.std_,
            'n_features_': self.n_features_
        }, path)
        logger.info(f"Scaler guardado en {path}")
    
    @classmethod
    def load(cls, path: Path) -> 'StandardScaler':
        """Carga un scaler desde disco."""
        data = joblib.load(path)
        scaler = cls()
        scaler.mean_ = data['mean_']
        scaler.std_ = data['std_']
        scaler.n_features_ = data['n_features_']
        scaler._is_fitted = True
        logger.info(f"Scaler cargado desde {path}")
        return scaler


class DataPipeline:
    """Pipeline para extraer y preparar datos para el autoencoder."""
    
    def __init__(self, session: Session):
        """Inicializa el pipeline.
        
        Args:
            session: Sesión de SQLAlchemy
        """
        self.session = session
    
    def extract_features(
        self, 
        stats: List[PlayerGameStats]
    ) -> np.ndarray:
        """Extrae las 14 features de una lista de estadísticas.
        
        Args:
            stats: Lista de objetos PlayerGameStats
            
        Returns:
            Array numpy de forma (n_samples, 14)
        """
        data = []
        for s in stats:
            minutes = self._interval_to_minutes(s.min)
            if minutes < MIN_MINUTES_THRESHOLD:
                continue
            
            row = [
                s.pts or 0,
                s.ast or 0,
                s.reb or 0,
                s.stl or 0,
                s.blk or 0,
                s.tov or 0,
                s.pf or 0,
                s.fg_pct or 0.0,
                s.fg3_pct or 0.0,
                s.ft_pct or 0.0,
                s.fga or 0,
                s.fta or 0,
                s.fg3a or 0,
                minutes
            ]
            data.append(row)
        
        return np.array(data, dtype=np.float64)
    
    def extract_single(self, stats: PlayerGameStats) -> Optional[np.ndarray]:
        """Extrae features de una sola línea estadística.
        
        Args:
            stats: Objeto PlayerGameStats
            
        Returns:
            Array numpy de forma (14,) o None si no cumple umbral de minutos
        """
        minutes = self._interval_to_minutes(stats.min)
        if minutes < MIN_MINUTES_THRESHOLD:
            return None
        
        return np.array([
            stats.pts or 0,
            stats.ast or 0,
            stats.reb or 0,
            stats.stl or 0,
            stats.blk or 0,
            stats.tov or 0,
            stats.pf or 0,
            stats.fg_pct or 0.0,
            stats.fg3_pct or 0.0,
            stats.ft_pct or 0.0,
            stats.fga or 0,
            stats.fta or 0,
            stats.fg3a or 0,
            minutes
        ], dtype=np.float64)
    
    def get_season_data(
        self, 
        season: str,
        limit: Optional[int] = None
    ) -> Tuple[np.ndarray, List[int]]:
        """Extrae todos los datos de una temporada.
        
        Args:
            season: Temporada en formato "YYYY-YY"
            limit: Límite de registros (para testing)
            
        Returns:
            Tupla (features array, lista de player_game_stat_ids)
        """
        query = (
            self.session.query(PlayerGameStats)
            .join(Game)
            .filter(Game.season == season)
            .order_by(Game.date)
        )
        
        if limit:
            query = query.limit(limit)
        
        stats = query.all()
        
        data = []
        stat_ids = []
        
        for s in stats:
            features = self.extract_single(s)
            if features is not None:
                data.append(features)
                stat_ids.append(s.id)
        
        logger.info(f"Temporada {season}: {len(data)} registros extraídos (de {len(stats)} totales)")
        return np.array(data), stat_ids
    
    def get_all_historical_data(
        self,
        end_season: Optional[str] = None,
        start_season: Optional[str] = None,
        return_seasons: bool = False
    ) -> Tuple[np.ndarray, List[int], Optional[List[str]]]:
        """Extrae todos los datos históricos en un rango de temporadas.
        
        Args:
            end_season: Última temporada a incluir (None = todas)
            start_season: Primera temporada a incluir (None = desde el inicio)
            return_seasons: Si True, retorna también la temporada de cada registro
            
        Returns:
            Tupla (features array, lista de player_game_stat_ids, [lista de seasons])
        """
        query = (
            self.session.query(PlayerGameStats, Game.season)
            .join(Game)
            .order_by(Game.date)
        )
        
        if end_season:
            query = query.filter(Game.season <= end_season)
        if start_season:
            query = query.filter(Game.season >= start_season)
        
        results = query.all()
        
        data = []
        stat_ids = []
        seasons = []
        
        for s, season in results:
            features = self.extract_single(s)
            if features is not None:
                data.append(features)
                stat_ids.append(s.id)
                seasons.append(season)
        
        # Log del rango de temporadas
        if seasons:
            unique_seasons = sorted(set(seasons))
            logger.info(f"Total: {len(data)} registros extraídos (de {len(results)} totales)")
            logger.info(f"Rango de temporadas: {unique_seasons[0]} a {unique_seasons[-1]} ({len(unique_seasons)} temporadas)")
        else:
            logger.info(f"Total: {len(data)} registros extraídos (de {len(results)} totales)")
        
        if return_seasons:
            return np.array(data), stat_ids, seasons
        return np.array(data), stat_ids, None
    
    def create_train_val_split(
        self,
        data: np.ndarray,
        stat_ids: List[int],
        train_ratio: float = 0.8
    ) -> Tuple[np.ndarray, np.ndarray, List[int], List[int]]:
        """Divide los datos en train/val manteniendo el orden temporal.
        
        Args:
            data: Array de features
            stat_ids: Lista de IDs correspondientes
            train_ratio: Proporción de datos para entrenamiento
            
        Returns:
            Tupla (train_data, val_data, train_ids, val_ids)
        """
        n_samples = len(data)
        split_idx = int(n_samples * train_ratio)
        
        train_data = data[:split_idx]
        val_data = data[split_idx:]
        train_ids = stat_ids[:split_idx]
        val_ids = stat_ids[split_idx:]
        
        logger.info(f"Split: {len(train_data)} train, {len(val_data)} val")
        return train_data, val_data, train_ids, val_ids
    
    def fit_scaler(
        self, 
        train_data: np.ndarray,
        season: Optional[str] = None
    ) -> StandardScaler:
        """Ajusta y guarda un scaler para los datos de entrenamiento.
        
        Args:
            train_data: Datos de entrenamiento
            season: Temporada (para nombrar el archivo)
            
        Returns:
            Scaler ajustado
        """
        scaler = StandardScaler()
        scaler.fit(train_data)
        
        # Guardar scaler
        suffix = f"_{season}" if season else "_global"
        scaler_path = MODELS_DIR / f"scaler{suffix}.joblib"
        scaler.save(scaler_path)
        
        return scaler
    
    def load_scaler(self, season: Optional[str] = None) -> StandardScaler:
        """Carga un scaler guardado.
        
        Args:
            season: Temporada del scaler
            
        Returns:
            Scaler cargado
        """
        suffix = f"_{season}" if season else "_global"
        scaler_path = MODELS_DIR / f"scaler{suffix}.joblib"
        return StandardScaler.load(scaler_path)
    
    @staticmethod
    def _interval_to_minutes(interval: Optional[timedelta]) -> float:
        """Convierte un intervalo a minutos flotantes."""
        if interval is None:
            return 0.0
        return interval.total_seconds() / 60.0


def get_feature_names() -> List[str]:
    """Retorna los nombres de las features en orden."""
    return STAT_FEATURES.copy()


def calculate_temporal_weights(
    seasons: List[str],
    decay_rate: float = 0.1,
    reference_season: Optional[str] = None
) -> np.ndarray:
    """Calcula pesos exponenciales basados en la temporada.
    
    Las temporadas más recientes tienen mayor peso.
    Peso = exp(-decay_rate * años_atrás)
    
    Args:
        seasons: Lista de temporadas en formato "YYYY-YY"
        decay_rate: Tasa de decaimiento (mayor = más énfasis en recientes)
        reference_season: Temporada de referencia (default: la más reciente)
        
    Returns:
        Array de pesos normalizados (suma = len(seasons))
    """
    if not seasons:
        return np.array([])
    
    # Extraer año de inicio de cada temporada
    season_years = np.array([int(s.split('-')[0]) for s in seasons])
    
    # Determinar temporada de referencia
    if reference_season:
        ref_year = int(reference_season.split('-')[0])
    else:
        ref_year = season_years.max()
    
    # Calcular años atrás respecto a la referencia
    years_back = ref_year - season_years
    
    # Calcular pesos exponenciales
    weights = np.exp(-decay_rate * years_back)
    
    # Normalizar para que la suma sea igual al número de muestras
    # Esto mantiene el loss en escala similar al no-weighted
    weights = weights * len(weights) / weights.sum()
    
    return weights.astype(np.float32)


def get_current_season() -> str:
    """Retorna la temporada actual basada en la fecha."""
    from datetime import date
    today = date.today()
    # La temporada NBA comienza en octubre
    year = today.year if today.month >= 10 else today.year - 1
    return f"{year}-{(year + 1) % 100:02d}"


def get_previous_season(season: str) -> str:
    """Retorna la temporada anterior."""
    year = int(season.split('-')[0])
    prev_year = year - 1
    return f"{prev_year}-{year % 100:02d}"
