"""Módulo de ingesta de datos de la NBA.

Este módulo contiene toda la lógica para descargar, procesar y almacenar
datos de la NBA desde su API oficial.
"""

from ingestion.api_client import NBAApiClient
from ingestion.checkpoints import CheckpointManager
from ingestion.ingestors import GameIngestion, SeasonIngestion
from ingestion.strategies import FullIngestion, IncrementalIngestion
from ingestion.models_sync import TeamSync, PlayerSync, PlayerAwardsSync
from ingestion.derived_tables import DerivedTablesGenerator
from ingestion.api_common import FatalIngestionError
from ingestion.utils import normalize_season
from ingestion.restart import restart_process

__all__ = [
    'NBAApiClient',
    'CheckpointManager',
    'GameIngestion',
    'SeasonIngestion',
    'FullIngestion',
    'IncrementalIngestion',
    'TeamSync',
    'PlayerSync',
    'PlayerAwardsSync',
    'DerivedTablesGenerator',
    'FatalIngestionError',
    'normalize_season',
    'restart_process',
]
