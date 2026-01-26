"""Funciones de worker para procesamiento paralelo.

Este módulo contiene las funciones que se ejecutan en procesos separados
para ingestar temporadas, premios y biografías.
"""
import logging
from typing import List, Optional

from db import get_session
from ingestion.api_client import NBAApiClient
from ingestion.checkpoints import CheckpointManager
from ingestion.utils import ProgressReporter
from ingestion.ingestors import SeasonIngestion
from ingestion.derived_tables import DerivedTablesGenerator
from ingestion.models_sync import PlayerSync, PlayerAwardsSync
from ingestion.api_common import FatalIngestionError

logger = logging.getLogger(__name__)

def season_worker_func(season: str, resume_game_id: Optional[str] = None):
    """Función de worker para procesar una temporada completa."""
    api_client = NBAApiClient()
    ckpt_mgr = CheckpointManager(checkpoint_key=f"season_{season}")
    
    # Cargar checkpoint propio si no se pasó resume_game_id
    if not resume_game_id:
        ckpt = ckpt_mgr.load_checkpoint()
        if ckpt and ckpt.get('season') == season:
            resume_game_id = ckpt.get('game_id')
    
    session = get_session()
    reporter = None
    try:
        reporter = ProgressReporter(f"Season-{season}", session_factory=get_session)
        reporter.update(0, f"Iniciando temporada {season}...")
        
        # 1. Ingestar partidos
        season_ingest = SeasonIngestion(api_client, ckpt_mgr)
        season_ingest.ingest_season(session, season, resume_game_id, reporter=reporter)
        
        # 2. Generar tablas derivadas inmediatamente
        derived = DerivedTablesGenerator()
        
        reporter.update(90, f"Generando tablas derivadas para {season}...")
        derived.regenerate_for_seasons(session, [season])
        
        # 3. Limpiar checkpoint al finalizar
        ckpt_mgr.clear()
        reporter.complete(f"Temporada {season} completada")
        logger.info(f"✅ Temporada {season} completada")
        
    except FatalIngestionError:
        raise
    except Exception as e:
        logger.error(f"Error en worker de temporada {season}: {e}")
        if reporter: reporter.fail(str(e))
        raise
    finally:
        session.close()

def season_batch_worker_func(seasons: List[str]):
    """Función de worker para procesar un lote de temporadas."""
    for season in seasons:
        season_worker_func(season)

def awards_worker_func(batch_id: int, player_ids: List[int], resume_player_id: Optional[int] = None, task_name: Optional[str] = None, checkpoint_prefix: str = "awards_batch"):
    """Función de worker para procesar un lote de premios."""
    api_client = NBAApiClient()
    actual_task_name = task_name or f"Awards-Batch-{batch_id}"
    ckpt_mgr = CheckpointManager(checkpoint_key=f"{checkpoint_prefix}_{batch_id}")
    
    if not resume_player_id:
        ckpt = ckpt_mgr.load_checkpoint()
        if ckpt:
            resume_player_id = ckpt.get('entity_id')

    session = get_session()
    reporter = None
    try:
        reporter = ProgressReporter(actual_task_name, session_factory=get_session)
        
        awards_sync = PlayerAwardsSync(api_client)
        reporter.update(0, f"Procesando {len(player_ids)} jugadores...")
        
        awards_sync.sync_batch(session, player_ids, ckpt_mgr, resume_player_id=resume_player_id, reporter=reporter)
        
        ckpt_mgr.clear()
        reporter.complete(f"Lote de {len(player_ids)} jugadores finalizado")
    except FatalIngestionError:
        raise
    except Exception as e:
        logger.error(f"Error en worker de premios batch {batch_id}: {e}")
        try:
            if reporter:
                reporter.fail(str(e))
        except: pass
        raise
    finally:
        session.close()

def player_info_worker_func(batch_id: int, player_ids: List[int], resume_player_id: Optional[int] = None, task_name: Optional[str] = None, checkpoint_prefix: str = "player_info_batch"):
    """Función de worker para procesar un lote de biografías de jugadores."""
    api_client = NBAApiClient()
    actual_task_name = task_name or f"Bio-Batch-{batch_id}"
    ckpt_mgr = CheckpointManager(checkpoint_key=f"{checkpoint_prefix}_{batch_id}")
    
    if not resume_player_id:
        ckpt = ckpt_mgr.load_checkpoint()
        if ckpt:
            resume_player_id = ckpt.get('entity_id')

    session = get_session()
    reporter = None
    try:
        reporter = ProgressReporter(actual_task_name, session_factory=get_session)
        reporter.update(0, f"Procesando biografías para {len(player_ids)} jugadores...")
        
        player_sync = PlayerSync()
        player_sync.sync_detailed_batch(session, player_ids, api_client, ckpt_mgr, reporter=reporter)
        
        ckpt_mgr.clear()
        reporter.complete(f"Biografías finalizadas")
    except FatalIngestionError:
        raise
    except Exception as e:
        logger.error(f"Error en worker de biografías batch {batch_id}: {e}")
        try:
            if reporter:
                reporter.fail(str(e))
        except: pass
        raise
    finally:
        session.close()
