"""Sistema de checkpoints para reanudar ingestas.

Este módulo proporciona un sistema simple de checkpoints que permite
guardar y cargar el progreso de ingestas largas para poder reanudarlas
si fallan por errores de API o interrupciones, utilizando la base de datos.
"""

import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any

from db import get_session
from db.models import IngestionCheckpoint

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Maneja checkpoints de ingesta para reanudar tras errores."""
    
    # Constantes para el checkpoint global de reanudación
    RESUME_TYPE = 'resume'
    DEFAULT_KEY = 'global'
    
    def __init__(self, checkpoint_key: str = DEFAULT_KEY):
        """Inicializa el manager de checkpoints.
        
        Args:
            checkpoint_key: Clave única para este manager (ej: 'season_1995-96')
        """
        self.checkpoint_key = checkpoint_key
    
    def save_games_checkpoint(self, season: str, game_id: str, context: Optional[Dict[str, Any]] = None):
        """Guarda checkpoint de ingesta de partidos.
        
        Args:
            season: Temporada actual (ej: "2023-24")
            game_id: ID del último partido procesado
            context: Información adicional del contexto (start_season, end_season, etc.)
        """
        metadata = {
            'type': 'games',
            'season': season,
            'game_id': game_id,
            'context': context or {}
        }
        
        self._upsert_checkpoint(
            last_game_id=game_id,
            metadata=metadata,
            games_processed=context.get('total', 0) if context else 0
        )
        logger.debug(f"Checkpoint guardado [{self.checkpoint_key}]: temporada {season}, partido {game_id}")
    
    def save_sync_checkpoint(self, sync_type: str, entity_id: int, context: Optional[Dict[str, Any]] = None):
        """Guarda checkpoint de sincronización (premios/carrera).
        
        Args:
            sync_type: Tipo de sincronización ('awards', 'career', 'jerseys')
            entity_id: ID de la entidad (player_id, team_id, etc.)
            context: Información adicional del contexto (season, etc.)
        """
        metadata = {
            'type': sync_type,
            'entity_id': entity_id,
            'context': context or {}
        }
        
        # Intentar determinar si es un player_id para la columna específica
        last_player = entity_id if sync_type in ['awards', 'career'] else None
        
        self._upsert_checkpoint(
            last_player_id=last_player,
            metadata=metadata
        )
        logger.debug(f"Checkpoint guardado [{self.checkpoint_key}]: {sync_type}, entidad {entity_id}")
    
    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Carga el último checkpoint guardado para la clave actual.
        
        Returns:
            Diccionario con datos del checkpoint o None si no existe
        """
        try:
            with get_session() as session:
                ckpt = session.query(IngestionCheckpoint).filter_by(
                    checkpoint_type=self.RESUME_TYPE,
                    checkpoint_key=self.checkpoint_key
                ).first()
                
                if not ckpt or not ckpt.metadata_json:
                    return None
                
                data = ckpt.metadata_json
                # Asegurar timestamp para compatibilidad
                data['timestamp'] = ckpt.updated_at.isoformat() if ckpt.updated_at else datetime.now().isoformat()
                
                logger.info(f"✅ Checkpoint cargado [{self.checkpoint_key}]: tipo={data.get('type')}, "
                           f"timestamp={data.get('timestamp')}")
                return data
                
        except Exception as e:
            logger.warning(f"Error cargando checkpoint de BD para {self.checkpoint_key}: {e}")
            return None
    
    def clear(self):
        """Elimina el checkpoint.
        
        Se debe llamar cuando la ingesta finaliza exitosamente.
        """
        try:
            with get_session() as session:
                session.query(IngestionCheckpoint).filter_by(
                    checkpoint_type=self.RESUME_TYPE,
                    checkpoint_key=self.checkpoint_key
                ).delete()
                session.commit()
                logger.info(f"🗑️  Checkpoint eliminado [{self.checkpoint_key}]")
        except Exception as e:
            logger.warning(f"Error eliminando checkpoint {self.checkpoint_key}: {e}")
    
    def _upsert_checkpoint(self, last_game_id=None, last_player_id=None, metadata=None, games_processed=0):
        """Actualiza o inserta el registro de checkpoint en la BD."""
        try:
            with get_session() as session:
                ckpt = session.query(IngestionCheckpoint).filter_by(
                    checkpoint_type=self.RESUME_TYPE,
                    checkpoint_key=self.checkpoint_key
                ).first()
                
                if not ckpt:
                    ckpt = IngestionCheckpoint(
                        checkpoint_type=self.RESUME_TYPE,
                        checkpoint_key=self.checkpoint_key,
                        status='in_progress'
                    )
                    session.add(ckpt)
                
                if last_game_id is not None:
                    ckpt.last_game_id = last_game_id
                if last_player_id is not None:
                    ckpt.last_player_id = last_player_id
                if metadata is not None:
                    ckpt.metadata_json = metadata
                if games_processed:
                    ckpt.games_processed = games_processed
                
                session.commit()
        except Exception as e:
            logger.error(f"Error guardando checkpoint {self.checkpoint_key} en BD: {e}")
    
    def get_resume_info(self, checkpoint: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extrae información de reanudación del checkpoint.
        
        Args:
            checkpoint: Diccionario con datos del checkpoint
            
        Returns:
            Diccionario con información procesada para reanudar o None
        """
        if not checkpoint:
            return None
        
        checkpoint_type = checkpoint.get('type')
        
        if checkpoint_type == 'games':
            return {
                'type': 'games',
                'season': checkpoint.get('season'),
                'game_id': checkpoint.get('game_id'),
                'context': checkpoint.get('context', {})
            }
        elif checkpoint_type in ['awards', 'career', 'jerseys']:
            return {
                'type': checkpoint_type,
                'entity_id': checkpoint.get('entity_id'),
                'context': checkpoint.get('context', {})
            }
        
        return None
