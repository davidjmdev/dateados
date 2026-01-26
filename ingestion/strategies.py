"""Estrategias de ingesta (Completa e Incremental).

Este módulo define las clases principales que orquestan el proceso de ingesta.
"""
import logging
import time
import multiprocessing
import math
from typing import Optional, Dict, Any, List, Tuple
from datetime import date

from sqlalchemy.orm import Session
from sqlalchemy import or_, and_

from db import get_session
from db.models import Game, PlayerGameStats, Player
from ingestion.api_client import NBAApiClient
from ingestion.checkpoints import CheckpointManager
from ingestion.models_sync import TeamSync, PlayerSync
from ingestion.ingestors import GameIngestion
from ingestion.derived_tables import DerivedTablesGenerator
from ingestion.config import API_DELAY
from ingestion.api_common import FatalIngestionError
from ingestion.utils import (
    get_max_workers, clear_memory, get_all_seasons, 
    normalize_season, ProgressReporter
)
from db.logging import (
    log_header, log_success, log_step
)
from ingestion.parallel import run_worker_with_stagger, run_parallel_task
from ingestion.workers import (
    season_batch_worker_func, awards_worker_func, player_info_worker_func
)

logger = logging.getLogger("dateados.ingestion.strategies")

class BaseIngestion:
    """Clase base con lógica común para ingestas."""
    
    def __init__(self, api_client: NBAApiClient, checkpoint_mgr: Optional[CheckpointManager] = None):
        self.api = api_client
        self.checkpoints = checkpoint_mgr or CheckpointManager()
        self.team_sync = TeamSync()
        self.player_sync = PlayerSync()
        self.game_ingestion = GameIngestion(api_client)
        self.derived = DerivedTablesGenerator()

    def sync_base_entities(self, session: Session, reporter: Optional[ProgressReporter] = None):
        """Sincroniza equipos y jugadores base."""
        log_step("Sincronizando entidades base (equipos y jugadores)")
        if reporter: reporter.update(2, "Sincronizando equipos...")
        self.team_sync.sync_all(session)
        if reporter: reporter.update(5, "Sincronizando jugadores...")
        self.player_sync.sync_all(session)

    def sync_post_process(self, session: Session, reporter: Optional[ProgressReporter] = None, 
                        active_only_awards: bool = False, prefix: str = ""):
        """Sincroniza premios y biografías detalladas de forma dinámica."""
        # Obtener capacidad actual del sistema
        max_workers = get_max_workers()

        # 1. Premios
        filter_query = Player.is_active == True if active_only_awards else Player.awards_synced == False
        player_ids = [pid for (pid,) in session.query(Player.id).filter(filter_query).all()]
        
        if player_ids:
            msg = f"Sincronizando premios para {len(player_ids)} jugadores"
            log_step(msg)
            if reporter: reporter.update(80, msg)
            
            run_parallel_task(
                awards_worker_func, 
                player_ids, 
                max_workers, 
                f"{prefix}awards_batch",
                lambda bid: f"{prefix.capitalize()}Awards-Batch-{bid}"
            )

        # 2. Biografías
        pending_bio = [pid for (pid,) in session.query(Player.id).filter(Player.bio_synced == False).all()]
        if pending_bio:
            msg = f"Sincronizando biografías para {len(pending_bio)} jugadores"
            log_step(msg)
            if reporter: reporter.update(90, msg)
            
            run_parallel_task(
                player_info_worker_func,
                pending_bio,
                max_workers,
                f"{prefix}player_info_batch",
                lambda bid: f"{prefix.capitalize()}Bio-Batch-{bid}"
            )

    def run_outlier_detection(self, session: Session, game_ids: List[str], reporter: Optional[ProgressReporter] = None):
        """Ejecuta detección de anomalías para una lista de partidos."""
        try:
            from outliers.runner import run_detection_for_games
            
            # Si no hay IDs, ejecutamos con lista vacía para asegurar inicialización de rachas
            if not game_ids:
                run_detection_for_games(session, [])
                return

            msg = f"Detectando outliers en {len(game_ids)} partidos"
            log_step(msg)
            if reporter: reporter.update(95, msg)
            
            new_stats = session.query(PlayerGameStats).filter(PlayerGameStats.game_id.in_(game_ids)).all()
            if new_stats:
                res = run_detection_for_games(session, new_stats)
                logger.info(f"Outliers detectados: {res.total_outliers}")
            else:
                run_detection_for_games(session, [])
                
        except ImportError: pass
        except Exception as e: logger.warning(f"Error en outliers: {e}")

    def _get_season_range(self, start: str, end: str) -> List[str]:
        """Genera lista de temporadas."""
        start, end = normalize_season(start), normalize_season(end)
        s_year, e_year = int(start.split('-')[0]), int(end.split('-')[0])
        return [f"{y}-{(y+1)%100:02d}" for y in range(s_year, e_year + 1)]

    def _supervise_processes(self, processes, worker_func, prefix):
        active_processes = processes.copy()
        while active_processes:
            for key, p in list(active_processes.items()):
                if not p.is_alive():
                    if p.exitcode == 0:
                        logger.info(f"Process {p.name} finalizado.")
                        active_processes.pop(key)
                    else:
                        logger.warning(f"Process {p.name} falló (Code {p.exitcode}). Relanzando...")
                        batch_name = "_".join(key)
                        new_p = multiprocessing.Process(
                            target=run_worker_with_stagger,
                            args=(worker_func, f"{prefix}_{batch_name}", list(key)),
                            name=p.name
                        )
                        new_p.start()
                        active_processes[key] = new_p
                        time.sleep(2)
            time.sleep(5)


class SmartIngestion(BaseIngestion):
    """Estrategia inteligente que combina incremental y carga masiva.
    
    Analiza el historial de forma inversa (hoy -> 1983) para determinar
    el estado de la base de datos y actuar en consecuencia:
    1. Temporadas incompletas (frontera) -> Escaneo incremental preciso.
    2. Temporadas vacías -> Carga masiva paralela.
    """
    
    def run(self, limit_seasons: Optional[int] = None, skip_outliers: bool = False, reporter: Optional[ProgressReporter] = None):
        """Ejecuta la ingesta inteligente."""
        session = get_session()
        try:
            log_header("INICIANDO INGESTA INTELIGENTE", "dateados.ingestion")
            
            # 1. Sincronizar entidades base
            self.sync_base_entities(session, reporter)
            
            # 2. Análisis de estado (Scan Phase)
            log_step("Analizando estado de la base de datos")
            if reporter: reporter.update(5, "Analizando estado...")
            
            all_seasons = sorted(get_all_seasons(), reverse=True)
            if limit_seasons:
                all_seasons = all_seasons[:limit_seasons]
                
            batch_seasons = []
            incremental_season = None
            
            for season in all_seasons:
                # Check rápido
                has_games = session.query(Game.id).filter(
                    and_(Game.season == season, Game.status == 3)
                ).first() is not None
                
                if not has_games:
                    batch_seasons.append(season)
                else:
                    incremental_season = season
                    logger.info(f"➜ Frontera de datos detectada en: {season}")
                    break
            
            # 3. Fase Incremental (Frontera)
            new_game_ids = []
            new_seasons_processed = set()
            
            if incremental_season:
                log_step(f"Sincronizando temporada frontera: {incremental_season}")
                if reporter: reporter.update(10, f"Actualizando {incremental_season}...")
                
                _, processed_ids = self._process_incremental_season(session, incremental_season, reporter)
                if processed_ids:
                    new_game_ids.extend(processed_ids)
                    new_seasons_processed.add(incremental_season)
            
            # 4. Fase Batch (Histórico Vacío)
            if batch_seasons:
                msg = f"Cargando {len(batch_seasons)} temporadas vacías en paralelo"
                log_step(msg)
                if reporter: reporter.update(15, "Iniciando carga masiva...")
                
                self._run_parallel_batch(batch_seasons)
                new_seasons_processed.update(batch_seasons)
            
            # 5. Regenerar tablas derivadas
            if new_seasons_processed:
                log_step("Recalculando estadísticas agregadas")
                if reporter: reporter.update(80, "Generando tablas derivadas...")
                self.derived.regenerate_for_seasons(session, list(new_seasons_processed))
            
            # 6. Post-procesamiento y Outliers
            if new_game_ids or batch_seasons:
                prefix = "smart_"
                active_only = not batch_seasons
                
                self.sync_post_process(session, reporter, active_only_awards=active_only, prefix=prefix)
                
                if not skip_outliers:
                    self.run_outlier_detection(session, new_game_ids, reporter)
            
            if reporter: reporter.complete("Ingesta finalizada")
            log_success("Ingesta inteligente completada con éxito")
            
        except FatalIngestionError:
            raise
        except Exception as e:
            logger.error(f"Error en SmartIngestion: {e}", exc_info=True)
            if reporter: reporter.fail(str(e))
            raise
        finally:
            session.close()

    def _process_incremental_season(self, session, season, reporter=None) -> Tuple[bool, List[str]]:
        """Procesa una temporada incrementalmente detectando brechas."""
        # Reutilizamos lógica similar a IncrementalIngestion pero simplificada
        games_data = self.api.fetch_season_games(season)
        if not games_data: return False, []
        
        # Escaneo de brecha (Nuevo -> Viejo)
        gap_games = []
        consecutive_existing = 0
        today_str = date.today().isoformat()
        
        for gd in games_data:
            gid = gd['game_id']
            api_finished = gd.get('is_finished', False)

            existing = session.query(Game.status, Game.home_score, Game.date).filter(Game.id == gid).first()
            
            db_finished = False
            if existing:
                home_score = existing.home_score or 0
                db_finished = (existing.status == 3 or (existing.status == 1 and home_score > 0 and str(existing.date) < today_str))
            
            if api_finished and not db_finished:
                gap_games.append(gd)
                consecutive_existing = 0
            elif db_finished:
                consecutive_existing += 1
                if consecutive_existing >= 3:
                    break
        
        if not gap_games:
            return True, []

        # Procesamiento (Viejo -> Nuevo)
        gap_games.reverse()
        processed_ids = []
        
        logger.info(f"  + Rellenando brecha de {len(gap_games)} partidos en {season}...")
        
        for i, gd in enumerate(gap_games):
            gid = gd['game_id']
            try:
                res = self.game_ingestion.ingest_game(
                    session, gid,
                    is_rs=gd.get('is_rs', False),
                    is_po=gd.get('is_po', False),
                    is_pi=gd.get('is_pi', False),
                    is_ist=gd.get('is_ist', False),
                    season_fallback=season
                )
                if res is True: 
                    processed_ids.append(gid)
                    if reporter: reporter.increment(f"Partido {gid[:8]}", delta=1)
                elif res is False:
                    raise FatalIngestionError(f"Fallo en partido {gid}")
                
                if i % 10 == 0: clear_memory()
                time.sleep(API_DELAY)
            except FatalIngestionError:
                raise
        
        return True, processed_ids

    def _run_parallel_batch(self, seasons: List[str]):
        """Ejecuta la carga paralela para una lista de temporadas."""
        max_workers = get_max_workers()
        num_seasons = len(seasons)
        num_workers = min(max_workers, num_seasons)
        
        if num_workers <= 0: return

        chunk_size = math.ceil(num_seasons / num_workers)
        season_chunks = [seasons[i:i + chunk_size] for i in range(0, num_seasons, chunk_size)]
        
        processes = {}
        for chunk in season_chunks:
            if not chunk: continue
            batch_name = f"{chunk[0]}_to_{chunk[-1]}"
            p = multiprocessing.Process(
                target=run_worker_with_stagger,
                args=(season_batch_worker_func, f"batch_{batch_name}", list(chunk)),
                name=f"Worker-{batch_name}"
            )
            p.start()
            processes[tuple(chunk)] = p
        
        self._supervise_processes(processes, season_batch_worker_func, "batch")


# Alias para compatibilidad, aunque SmartIngestion reemplaza a ambas en uso general
FullIngestion = SmartIngestion
IncrementalIngestion = SmartIngestion
