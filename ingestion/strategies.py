"""Estrategias de ingesta (Completa e Incremental).

Este módulo define las clases principales que orquestan el proceso de ingesta.
"""
import logging
import time
import multiprocessing
from typing import Optional, Dict, Any, List
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
    ProgressReporter, get_max_workers, clear_memory, get_all_seasons, 
    normalize_season
)
from ingestion.parallel import run_worker_with_stagger, run_parallel_task
from ingestion.workers import (
    season_batch_worker_func, awards_worker_func, player_info_worker_func
)

logger = logging.getLogger(__name__)

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
            msg = f"Sincronizando premios para {len(player_ids)} jugadores..."
            logger.info(msg)
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
            msg = f"Sincronizando biografías para {len(pending_bio)} jugadores..."
            logger.info(msg)
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

            msg = f"Detectando outliers en {len(game_ids)} partidos..."
            logger.info(msg)
            if reporter: reporter.update(95, msg)
            
            new_stats = session.query(PlayerGameStats).filter(PlayerGameStats.game_id.in_(game_ids)).all()
            if new_stats:
                res = run_detection_for_games(session, new_stats)
                logger.info(f"Outliers: {res.total_outliers} (L:{res.league_outliers}, J:{res.player_outliers}, R:{res.streak_outliers})")
            else:
                # Si por alguna razón no hay stats para esos IDs, aún así intentamos inicialización
                run_detection_for_games(session, [])
                
        except ImportError: pass
        except Exception as e: logger.warning(f"Error en outliers: {e}")

    def _get_season_range(self, start: str, end: str) -> List[str]:
        """Genera lista de temporadas."""
        start, end = normalize_season(start), normalize_season(end)
        s_year, e_year = int(start.split('-')[0]), int(end.split('-')[0])
        return [f"{y}-{(y+1)%100:02d}" for y in range(s_year, e_year + 1)]


class FullIngestion(BaseIngestion):
    """Maneja la ingesta histórica completa."""
    
    FATAL_EXIT_CODE = 42

    def run(self, start_season: str, end_season: str, checkpoint: Optional[Dict] = None, reporter: Optional[ProgressReporter] = None):
        """Ejecuta ingesta completa histórica paralela."""
        if reporter: reporter.update(0, "Iniciando ingesta completa...")
        session = get_session()
        try:
            # 1. Sincronizar entidades base
            self.sync_base_entities(session, reporter)
            
            # 2. Obtener lista de temporadas
            seasons = self._get_season_range(start_season, end_season)
            
            if checkpoint:
                seasons = [s for s in seasons if self._is_season_pending(session, s)]

            if not seasons:
                logger.info("No hay temporadas pendientes.")
                if reporter: reporter.update(100, "No hay temporadas pendientes")
            else:
                msg = f"Iniciando ingesta paralela para {len(seasons)} temporadas"
                logger.info(f"{msg}: {seasons}")
                if reporter: reporter.update(10, msg)
            
            # 3. Lanzar procesos de temporadas de forma dinámica
            max_workers = get_max_workers()
            num_seasons = len(seasons)
            # Calculamos cuántos workers necesitamos realmente (mínimo entre capacidad y trabajo)
            num_workers = min(max_workers, num_seasons)
            
            if num_workers > 0:
                # Repartimos las temporadas equitativamente entre los workers
                import math
                chunk_size = math.ceil(num_seasons / num_workers)
                season_chunks = [seasons[i:i + chunk_size] for i in range(0, num_seasons, chunk_size)]
                
                processes = {}
                for chunk in season_chunks:
                    batch_name = "_".join(chunk)
                    p = multiprocessing.Process(
                        target=run_worker_with_stagger,
                        args=(season_batch_worker_func, f"batch_{batch_name}", list(chunk)),
                        name=f"Worker-{chunk[0]}_etc"
                    )
                    p.start()
                    processes[tuple(chunk)] = p
                
                # 4. Supervisar procesos
                self._supervise_processes(processes, season_batch_worker_func, "batch")
            
            # 5. Post-procesamiento (Premios y Bios)
            logger.info("Fase de temporadas completada. Iniciando post-procesamiento...")
            if reporter: reporter.update(75, "Temporadas completadas. Sincronizando datos de jugadores...")
            self.sync_post_process(session, reporter)
            
            # 6. Outliers (Asegurar inicialización de rachas)
            if reporter: reporter.update(95, "Verificando sistema de outliers...")
            from outliers.runner import run_detection_for_games
            run_detection_for_games(session, []) # Esto disparará el auto-backfill si está vacío
            
            if reporter: reporter.complete("Ingesta completa histórica finalizada")
            logger.info("✅ Ingesta completa histórica finalizada")
            
        finally:
            session.close()

    def _is_season_pending(self, session, season):
        """Verifica si una temporada necesita ser procesada."""
        ckpt_mgr = CheckpointManager(checkpoint_key=f"season_{season}")
        if ckpt_mgr.load_checkpoint(): return True
        return session.query(Game.id).filter(Game.season == season).first() is None

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


class IncrementalIngestion(BaseIngestion):
    """Maneja la ingesta incremental paralela."""
    
    def __init__(self, api_client: NBAApiClient, checkpoint_mgr: Optional[CheckpointManager] = None, skip_outliers: bool = False):
        super().__init__(api_client, checkpoint_mgr)
        self.skip_outliers = skip_outliers
    
    def run(self, limit_seasons: Optional[int] = None, reporter: Optional[ProgressReporter] = None):
        """Ejecuta la sincronización incremental usando detección de brechas."""
        limit_text = f"últimas {limit_seasons} temporadas" if limit_seasons else "hasta encontrar historial"
        logger.info(f"Iniciando ingesta incremental ({limit_text})...")
        if reporter: reporter.update(0, "Iniciando ingesta incremental...")
        
        session = get_session()
        try:
            # 1. Entidades base (Equipos/Jugadores)
            self.sync_base_entities(session, reporter)

            # 2. Procesar partidos recientes
            all_seasons = sorted(get_all_seasons(), reverse=True)
            seasons_to_process = all_seasons[:limit_seasons] if limit_seasons else all_seasons
            
            new_seasons = set()
            new_game_ids = []
            
            for i, season in enumerate(seasons_to_process):
                msg = f"Escaneando temporada {season}..."
                logger.info(msg)
                if reporter: reporter.update(5 + int((i / len(seasons_to_process)) * 60), msg)

                # Procesar la temporada con la nueva lógica de brechas
                gap_closed, processed_ids = self._process_incremental_season(session, season, reporter=reporter)
                
                if processed_ids:
                    new_seasons.add(season)
                    new_game_ids.extend(processed_ids)
                
                # Si hemos encontrado y cerrado la brecha de datos, terminamos
                if gap_closed:
                    logger.info(f"Brecha de datos cerrada en temporada {season}. Sincronización completa.")
                    break
            
            # 3. Regenerar tablas derivadas
            if new_seasons:
                if reporter: reporter.update(70, "Recalculando estadísticas agregadas...")
                self.derived.regenerate_for_seasons(session, list(new_seasons))
            
            # 4. Post-procesamiento y Outliers (SOLO SI HAY NOVEDADES REALES)
            if new_game_ids:
                # Actualizar premios y biografías solo si ha habido partidos nuevos.
                self.sync_post_process(session, reporter, active_only_awards=True, prefix="incremental_")
                
                # Outliers
                if not self.skip_outliers:
                    self.run_outlier_detection(session, new_game_ids, reporter)
                
                if reporter: reporter.complete("Base de datos actualizada.")
            else:
                logger.info("Base de datos ya estaba al día. No se detectaron partidos terminados nuevos.")
                if reporter: reporter.complete("Base de datos al día.")
            
            return # <--- Fin del proceso (Ahorra tiempo de limpieza innecesaria)
            
        except FatalIngestionError:
            raise
        except Exception as e:
            if reporter: reporter.fail(str(e))
            logger.error(f"Error en incremental: {e}")
            raise
        finally:
            session.close()
            clear_memory()

    def _process_incremental_season(self, session, season, reporter=None):
        """Procesa una temporada incrementalmente detectando brechas de datos."""
        games_data = self.api.fetch_season_games(season)
        if not games_data: return False, []
        
        # FASE 1: Escaneo de brecha (Nuevo -> Viejo)
        # Identificamos qué partidos faltan o están incompletos
        gap_games = []
        consecutive_existing = 0
        today_str = date.today().isoformat()
        
        logger.info(f"Escaneando {len(games_data)} partidos en busca de novedades...")
        
        for gd in games_data:
            gid = gd['game_id']
            api_finished = gd.get('is_finished', False)

            # Comprobar si existe y está finalizado en nuestra DB
            existing = session.query(Game.status, Game.home_score, Game.date).filter(Game.id == gid).first()
            
            # Un partido se considera "completo" en nuestra DB si status es 3 (finalizado oficial)
            # o si es status 1 pero tiene marcador y es de fecha pasada (error común de la API)
            db_finished = False
            if existing:
                home_score = existing.home_score or 0
                db_finished = (existing.status == 3 or (existing.status == 1 and home_score > 0 and str(existing.date) < today_str))
            
            if api_finished and not db_finished:
                # ¡Es una novedad real! (Terminado en la API pero no completo en nuestra DB)
                logger.info(f"  + Novedad detectada: Partido {gid} (API dice terminado, DB incompleto)")
                gap_games.append(gd)
                consecutive_existing = 0
            elif db_finished:
                # Ya lo tenemos completo.
                consecutive_existing += 1
                if consecutive_existing >= 3:
                    logger.debug(f"  - Brecha cerrada en partido {gid}")
                    break
            elif not api_finished:
                # El partido está en juego o programado según la API.
                # Lo ignoramos pero lo logueamos a nivel debug para saber qué pasa.
                logger.debug(f"  - Saltando {gid}: Partido en directo o futuro según API (sin WL)")

        if not gap_games:
            return True, [] # Brecha cerrada (estamos al día)

        # FASE 2: Procesamiento de brecha (Viejo -> Nuevo)
        # Le damos la vuelta para que, si falla, al menos los más antiguos se hayan guardado
        gap_games.reverse()
        processed_ids = []
        
        logger.info(f"Detectada brecha de {len(gap_games)} partidos. Procesando del más antiguo al más moderno...")
        if reporter: 
            reporter.set_total(len(gap_games))
            reporter.update(None, f"Procesando {len(gap_games)} partidos nuevos...")

        for j, gd in enumerate(gap_games):
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
                    raise FatalIngestionError(f"Fallo en partido incremental {gid}")
                
                if j % 10 == 0: clear_memory()
                time.sleep(API_DELAY)

            except FatalIngestionError:
                # No necesitamos guardar checkpoints externos porque la DB ya tiene los partidos "viejos" guardados
                logger.error(f"Error fatal en brecha. Procesados {len(processed_ids)}/{len(gap_games)} partidos.")
                raise
            
        # Retornamos True si el escaneo encontró el final de la brecha (consecutive_existing >= 3)
        # o False si revisamos toda la temporada y no encontramos el historial (hay que mirar la anterior)
        return consecutive_existing >= 3, processed_ids
