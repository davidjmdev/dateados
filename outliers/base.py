"""Clase base abstracta para detectores de outliers.

Todos los detectores (Liga, Jugador, Rachas) heredan de BaseDetector
para mantener una interfaz consistente.
"""

from abc import ABC, abstractmethod
from typing import List, Any, Optional
from dataclasses import dataclass

from sqlalchemy.orm import Session

from db.models import PlayerGameStats


@dataclass
class OutlierResult:
    """Resultado de detección de un outlier."""
    player_game_stat_id: int
    is_outlier: bool
    outlier_data: dict


class BaseDetector(ABC):
    """Clase base abstracta para todos los detectores de outliers."""
    
    @abstractmethod
    def detect(
        self, 
        session: Session, 
        game_stats: List[PlayerGameStats]
    ) -> List[OutlierResult]:
        """Detecta outliers en una lista de estadísticas de partido.
        
        Args:
            session: Sesión de SQLAlchemy
            game_stats: Lista de estadísticas de jugadores en partidos
            
        Returns:
            Lista de resultados de detección
        """
        pass
    
    @abstractmethod
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
        pass
    
    def _get_minutes_float(self, stats: PlayerGameStats) -> float:
        """Convierte el intervalo de minutos a float."""
        if stats.min is None:
            return 0.0
        return stats.min.total_seconds() / 60.0
