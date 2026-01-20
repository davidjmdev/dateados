"""Lógica central de ingesta de partidos y temporadas.

Este módulo contiene las clases principales para la ingesta:
- GameIngestion: Ingesta de un partido individual
- SeasonIngestion: Ingesta de una temporada completa
- FullIngestion: Ingesta histórica completa con checkpoints
- IncrementalIngestion: Ingesta de últimos partidos
"""

import logging
import time
import multiprocessing
import os
import random
from datetime import date, timedelta
from typing import Optional, Dict, Any, List, Set, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from db import get_session
from db.models import Game, PlayerGameStats, Player
from ingestion.api_client import NBAApiClient
from ingestion.checkpoints import CheckpointManager
from ingestion.models_sync import TeamSync, PlayerSync, PlayerAwardsSync
from ingestion.derived_tables import DerivedTablesGenerator
from ingestion.config import API_DELAY, AWARDS_WORKERS
from ingestion.utils import (
    FatalIngestionError, normalize_season, 
    get_or_create_player, get_or_create_team, API_TIMEOUT,
    get_max_workers, clear_memory, ProgressReporter,
    parse_date, convert_minutes_to_interval, safe_int,
    get_all_seasons, safe_float
)

from ingestion.parallel import run_worker_with_stagger, run_parallel_task

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
    try:
        # 1. Ingestar partidos
        season_ingest = SeasonIngestion(api_client, ckpt_mgr)
        season_ingest.ingest_season(session, season, resume_game_id)
        
        # 2. Generar tablas derivadas inmediatamente
        derived = DerivedTablesGenerator()
        
        logger.info(f"Generando tablas derivadas para {season}...")
        derived.regenerate_for_seasons(session, [season])
        
        # 3. Limpiar checkpoint al finalizar
        ckpt_mgr.clear()
        logger.info(f"✅ Temporada {season} completada")
        
    except FatalIngestionError:
        raise
    except Exception as e:
        logger.error(f"Error en worker de temporada {season}: {e}")
        raise
    finally:
        session.close()

def season_batch_worker_func(seasons: List[str]):
    """Función de worker para procesar un lote de temporadas (generalmente 2)."""
    for season in seasons:
        season_worker_func(season)

def awards_worker_func(batch_id: int, player_ids: List[int], resume_player_id: Optional[int] = None):
    """Función de worker para procesar un lote de premios."""
    api_client = NBAApiClient()
    ckpt_mgr = CheckpointManager(checkpoint_key=f"awards_batch_{batch_id}")
    
    if not resume_player_id:
        ckpt = ckpt_mgr.load_checkpoint()
        if ckpt:
            resume_player_id = ckpt.get('entity_id')

    session = get_session()
    try:
        awards_sync = PlayerAwardsSync(api_client)
        awards_sync.sync_batch(session, player_ids, ckpt_mgr, resume_player_id=resume_player_id)
        
        ckpt_mgr.clear()
        logger.info(f"✅ Batch de premios {batch_id} completado")
    except FatalIngestionError:
        raise
    except Exception as e:
        logger.error(f"Error en worker de premios batch {batch_id}: {e}")
        raise
    finally:
        session.close()

def player_info_worker_func(batch_id: int, player_ids: List[int], resume_player_id: Optional[int] = None):
    """Función de worker para procesar un lote de biografías de jugadores."""
    api_client = NBAApiClient()
    ckpt_mgr = CheckpointManager(checkpoint_key=f"player_info_batch_{batch_id}")
    
    if not resume_player_id:
        ckpt = ckpt_mgr.load_checkpoint()
        if ckpt:
            resume_player_id = ckpt.get('entity_id')

    session = get_session()
    try:
        player_sync = PlayerSync()
        player_sync.sync_detailed_batch(session, player_ids, api_client, ckpt_mgr)
        
        ckpt_mgr.clear()
        logger.info(f"✅ Batch de biografías {batch_id} completado")
    except FatalIngestionError:
        raise
    except Exception as e:
        logger.error(f"Error en worker de biografías batch {batch_id}: {e}")
        raise
    finally:
        session.close()


def classify_game_type(game_id: str, season: str) -> Dict[str, bool]:

    """Clasifica el tipo de partido basado en su ID y temporada.
    
    Args:
        game_id: ID del partido
        season: Temporada (ej: "2023-24")
        
    Returns:
        Dict con flags rs, po, pi, ist
    """
    prefix = game_id[0:3]
    suffix = game_id[5:]
    season_start_year = int(season.split('-')[0])
    
    is_ist = False
    if season_start_year >= 2023:
        if prefix == '006':
            is_ist = True
        elif prefix == '002':
            if suffix.startswith('000') and not (season == '2023-24' and suffix in ['00001', '00002']):
                is_ist = True
            elif suffix in ['01201', '01202', '01203', '01204', '01229', '01230']:
                is_ist = True
    
    return {
        'rs': prefix == '002',
        'po': prefix == '004',
        'pi': prefix == '005' and season_start_year >= 2020,
        'ist': is_ist
    }


class GameIngestion:
    """Maneja la ingesta de un partido individual."""
    
    def __init__(self, api_client: NBAApiClient):
        """Inicializa el ingestor de partidos.
        
        Args:
            api_client: Cliente API unificado
        """
        self.api = api_client
    
    def ingest_game(self, session: Session, game_id: str, season_fallback: Optional[str] = None) -> Optional[bool]:
        """Ingiere un partido completo desde la API.
        
        Args:
            session: Sesión de SQLAlchemy
            game_id: ID del partido
            season_fallback: Temporada de respaldo si no se encuentra en la respuesta
            
        Returns:
            True si éxito, False si error, None si se saltó (no finalizado)
            
        Raises:
            FatalIngestionError: Si hay errores persistentes de API
        """
        # Verificar si ya existe y está finalizado
        existing = session.query(Game.status, Game.home_score).filter(Game.id == game_id).first()
        if existing:
            is_really_finished = (existing.status == 3) or (existing.status == 1 and existing.home_score is not None and existing.home_score > 0)
            if is_really_finished:
                has_stats = session.query(PlayerGameStats.id).filter(PlayerGameStats.game_id == game_id).first() is not None
                if has_stats:
                    return True
        
        try:
            # Obtener summary (fatal=True)
            summary = self.api.fetch_game_summary(game_id)
            if summary is None:
                return False
            
            summary_dict = summary.get_dict()
            if 'game' in summary_dict:
                data = summary_dict['game']
            elif 'boxScoreSummary' in summary_dict:
                data = summary_dict['boxScoreSummary']
            else:
                logger.error(f"No se encontró información del partido en {game_id}")
                return False
            
            # Verificar si está finalizado
            if data.get('gameStatus') != 3:
                logger.debug(f"Saltando {game_id}: aún en progreso (Status {data.get('gameStatus')})")
                return None
            
            # Usar gameEt si está disponible para tener la fecha local (USA)
            # gameTimeUTC puede caer en el día siguiente para partidos nocturnos
            game_date_str = data.get('gameEt') or data.get('gameTimeUTC')
            game_date = parse_date(game_date_str)
            
            # Deducir temporada
            season = self._deduce_season(game_id, data, season_fallback, game_date)
            if not season:
                logger.error(f"No se pudo determinar la temporada para {game_id}")
                return False
            
            home_team_id, away_team_id = data['homeTeamId'], data['awayTeamId']
            ctype = classify_game_type(game_id, season)
            
            quarter_scores = None
            try:
                # La API V3 usa 'score' en lugar de 'points' para los periodos
                quarter_scores = {
                    'home': [p.get('score', p.get('points')) for p in data['homeTeam']['periods']],
                    'away': [p.get('score', p.get('points')) for p in data['awayTeam']['periods']]
                }
            except:
                pass
            
            # Crear o actualizar Game
            game = session.query(Game).filter(Game.id == game_id).first()
            game_exists = game is not None
            
            if not game_exists:
                game = Game(
                    id=game_id, date=game_date, season=season, status=data.get('gameStatus'),
                    home_team_id=home_team_id, away_team_id=away_team_id,
                    home_score=data['homeTeam']['score'], away_score=data['awayTeam']['score'],
                    winner_team_id=home_team_id if data['homeTeam']['score'] > data['awayTeam']['score'] else away_team_id,
                    rs=ctype['rs'], po=ctype['po'], pi=ctype['pi'], ist=ctype['ist'], 
                    quarter_scores=quarter_scores
                )
                session.add(game)
            else:
                game.date = game_date # Actualizar fecha por si cambió (UTC -> Local)
                game.status = data.get('gameStatus')
                game.home_score = data['homeTeam']['score']
                game.away_score = data['awayTeam']['score']
                game.winner_team_id = home_team_id if game.home_score > game.away_score else away_team_id
                game.quarter_scores = quarter_scores
            
            # Obtener boxscore (fatal=True)
            traditional = self.api.fetch_game_boxscore(game_id)
            if traditional is None:
                return False
            
            dfs = traditional.get_data_frames()
            if len(dfs) == 0 or dfs[0].empty:
                session.commit()
                return None
            
            # Fallback V2 si hay nombres vacíos
            name_fallback = {}
            has_empty = any(not self._get_col(r, 'nameI', 'PLAYER_NAME') for _, r in dfs[0].iterrows())
            if has_empty:
                v2 = self.api.fetch_game_boxscore_v2_fallback(game_id)
                if v2:
                    for _, r in v2.get_data_frames()[0].iterrows():
                        if r.get('PLAYER_ID') and r.get('PLAYER_NAME'):
                            name_fallback[int(r['PLAYER_ID'])] = r['PLAYER_NAME']
            
            # Procesar estadísticas
            self._process_player_stats(session, game_id, dfs[0], game_exists, name_fallback)
            
            session.commit()
            return True
            
        except FatalIngestionError:
            raise
        except Exception as e:
            logger.error(f"Error crítico procesando partido {game_id}: {e}")
            session.rollback()
            raise FatalIngestionError(f"Fallo en partido {game_id}: {e}")
    
    def _deduce_season(self, game_id, data, season_fallback, game_date):
        """Deduce la temporada del partido."""
        season = None
        
        # 1. Del Game ID
        if game_id and len(game_id) == 10 and (game_id.startswith('002') or game_id.startswith('004') or game_id.startswith('005') or game_id.startswith('006')):
            try:

                yy = int(game_id[3:5])
                century = 2000 if yy < 50 else 1900
                year = century + yy
                season = f"{year}-{(year+1)%100:02d}"
            except:
                pass
        
        # 2. De la respuesta API
        if not season:
            season = data.get('seasonYear')
        
        # 3. Fallback del parámetro
        if not season:
            season = season_fallback
        
        # 4. De la fecha
        if not season and game_date:
            year = game_date.year
            month = game_date.month
            if month >= 10:
                season = f"{year}-{(year+1)%100:02d}"
            else:
                season = f"{year-1}-{year%100:02d}"
        
        return season
    
    def _get_col(self, row, *names):
        """Busca un valor en múltiples nombres de columna."""
        for name in names:
            if name in row:
                return row[name]
        return None
    
    def _process_player_stats(self, session, game_id, df, game_exists, name_fallback):
        """Procesa estadísticas de jugadores."""
        for _, row in df.iterrows():
            try:
                player_id = int(self._get_col(row, 'personId', 'PLAYER_ID', 'playerId'))
                team_id = int(self._get_col(row, 'teamId', 'TEAM_ID'))
                
                from ingestion.utils import is_valid_team_id
                if not is_valid_team_id(team_id, session=session):
                    continue
                
                player_name = self._get_col(row, 'nameI', 'PLAYER_NAME', 'playerName', 'name')
                player_min = convert_minutes_to_interval(self._get_col(row, 'minutes', 'MIN', 'min') or '0:00')
                
                if (not player_name or player_name.strip() == '') and player_min.total_seconds() == 0:
                    if not (name_fallback and player_id in name_fallback):
                        if (self._get_col(row, 'points', 'PTS') or 0) == 0:
                            continue
                
                if not player_name or player_name.strip() == '':
                    player_name = name_fallback.get(player_id, f"Player {player_id}")
                
                get_or_create_player(session, player_id, {'full_name': player_name})
                
                existing_stat = session.query(PlayerGameStats).filter(
                    and_(PlayerGameStats.game_id == game_id, PlayerGameStats.player_id == player_id)
                ).first()
                
                # Validar y guardar stats
                stat_data = self._build_stat_data(row, game_id, player_id, team_id, player_min)

                
                if existing_stat:
                    for k, v in stat_data.items():
                        setattr(existing_stat, k, v)
                else:
                    session.add(PlayerGameStats(**stat_data))
                    
            except FatalIngestionError:
                raise
            except Exception as e:
                logger.error(f"Error procesando fila de stats en {game_id}: {e}")
                continue
    
    def _build_stat_data(self, row, game_id, player_id, team_id, player_min):
        """Construye diccionario de estadísticas."""
        fgm = safe_int(self._get_col(row, 'fieldGoalsMade', 'FGM'))
        fga = safe_int(self._get_col(row, 'fieldGoalsAttempted', 'FGA'))
        fg3m = safe_int(self._get_col(row, 'threePointersMade', 'FG3M'))
        fg3a = safe_int(self._get_col(row, 'threePointersAttempted', 'FG3A'))
        ftm = safe_int(self._get_col(row, 'freeThrowsMade', 'FTM'))
        fta = safe_int(self._get_col(row, 'freeThrowsAttempted', 'FTA'))
        
        # Validaciones
        if fgm > fga: fga = fgm
        if fg3m > fg3a: fg3a = fg3m
        if fg3m > fgm: fg3m = fgm
        if fg3a > fga: fg3a = fga
        if ftm > fta: fta = ftm
        
        fg_pct = fgm / fga if fga > 0 else 0.0
        fg3_pct = fg3m / fg3a if fg3a > 0 else 0.0
        ft_pct = ftm / fta if fta > 0 else 0.0
        
        return {
            'game_id': game_id,
            'player_id': player_id,
            'team_id': team_id,
            'min': player_min,
            'pts': safe_int(self._get_col(row, 'points', 'PTS')),
            'reb': safe_int(self._get_col(row, 'reboundsTotal', 'REB')),
            'ast': safe_int(self._get_col(row, 'assists', 'AST')),
            'stl': safe_int(self._get_col(row, 'steals', 'STL')),
            'blk': safe_int(self._get_col(row, 'blocks', 'BLK')),
            'tov': safe_int(self._get_col(row, 'turnovers', 'TOV')),
            'pf': safe_int(self._get_col(row, 'foulsPersonal', 'PF')),
            'plus_minus': safe_float(self._get_col(row, 'plusMinusPoints', 'PLUS_MINUS')),
            'fgm': fgm, 'fga': fga, 'fg_pct': fg_pct,
            'fg3m': fg3m, 'fg3a': fg3a, 'fg3_pct': fg3_pct,
            'ftm': ftm, 'fta': fta, 'ft_pct': ft_pct,
        }


class SeasonIngestion:
    """Maneja la ingesta de una temporada completa."""
    
    def __init__(self, api_client: NBAApiClient, checkpoint_mgr: CheckpointManager):
        """Inicializa el ingestor de temporadas.
        
        Args:
            api_client: Cliente API
            checkpoint_mgr: Manager de checkpoints
        """
        self.api = api_client
        self.checkpoints = checkpoint_mgr
        self.game_ingestion = GameIngestion(api_client)
    
    def ingest_season(
        self, 
        session: Session, 
        season: str, 
        resume_from_game_id: Optional[str] = None
    ) -> Dict[str, int]:
        """Ingiere todos los partidos de una temporada.
        
        Args:
            session: Sesión de SQLAlchemy
            season: Temporada (ej: "2023-24")
            resume_from_game_id: ID del partido desde donde reanudar
            
        Returns:
            Dict con estadísticas (total, success, failed)
            
        Raises:
            FatalIngestionError: Si hay errores persistentes de API
        """
        logger.info(f"Iniciando ingesta de temporada {season}...")
        
        # Obtener lista de partidos con fecha local (fatal=True)
        games_data = self.api.fetch_season_games(season)
        if not games_data:
            return {'total': 0, 'success': 0, 'failed': 0}
        
        # Optimización: Obtener IDs de partidos que ya están finalizados y tienen estadísticas
        completed_games = {
            gid for (gid,) in session.query(Game.id)
            .join(PlayerGameStats, PlayerGameStats.game_id == Game.id)
            .filter(
                Game.season == season,
                or_(
                    Game.status == 3,
                    and_(Game.status == 1, Game.home_score > 0)
                )
            )
            .distinct().all()
        }
        
        # Reanudar si es necesario
        if resume_from_game_id:
            idx = -1
            for i, gd in enumerate(games_data):
                if gd['game_id'] == resume_from_game_id:
                    idx = i
                    break
            if idx != -1:
                games_data = games_data[idx:]
        
        success, failed = 0, 0
        for i, gd in enumerate(games_data):
            gid = gd['game_id']
            local_date = gd['game_date']
            
            # Salto rápido si ya está completado
            if gid in completed_games:
                success += 1
                continue
            
            try:
                # Checkpoint cada 20 partidos
                if i > 0 and i % 20 == 0:
                    self.checkpoints.save_games_checkpoint(season, gid, {'total': len(games_data)})
                    logger.info(f"Progreso {season}: {i}/{len(games_data)}")
                
                # Asegurar que la fecha en DB sea la fecha local correcta
                # Esto corrige fechas que se pudieron guardar como UTC (día siguiente)
                existing = session.query(Game).filter(Game.id == gid).first()
                if existing and existing.date != local_date:
                    logger.debug(f"Corrigiendo fecha para partido {gid}: {existing.date} -> {local_date}")
                    existing.date = local_date
                    session.commit()

                res = self.game_ingestion.ingest_game(session, gid, season_fallback=season)
                if res is True:
                    success += 1
                elif res is False:
                    # Esto no debería ocurrir con el nuevo raise en ingest_game, pero por seguridad:
                    raise FatalIngestionError(f"Fallo no recuperable en partido {gid}")
                
                time.sleep(API_DELAY)
            except FatalIngestionError:
                # Guardar checkpoint exacto antes de reiniciar
                self.checkpoints.save_games_checkpoint(season, gid, {'total': len(games_data)})
                logger.error(f"Error fatal en temporada {season}, partido {gid}. Checkpoint guardado.")
                raise
        
        logger.info(f"Temporada {season} completada: {success} exitosos, {failed} fallidos")
        return {'total': len(games_data), 'success': success, 'failed': failed}


class FullIngestion:
    """Maneja la ingesta histórica completa."""
    
    FATAL_EXIT_CODE = 42

    def __init__(self, api_client: NBAApiClient, checkpoint_mgr: CheckpointManager):
        self.api = api_client
        self.checkpoints = checkpoint_mgr
        self.team_sync = TeamSync()
        self.player_sync = PlayerSync()
    
    def run(self, start_season: str, end_season: str, checkpoint: Optional[Dict] = None):
        """Ejecuta ingesta completa histórica paralela.
        
        Args:
            start_season: Temporada de inicio
            end_season: Temporada final
            checkpoint: Checkpoint global (opcional)
        """
        session = get_session()
        try:
            # 1. Sincronizar entidades base
            self.team_sync.sync_all(session)
            new_players_count = self.player_sync.sync_all(session)
            
            # 2. Obtener lista de temporadas
            seasons = self._get_season_range(start_season, end_season)
            
            # Filtrar temporadas ya completadas (si no hay checkpoint de esa temporada y tiene partidos)
            if checkpoint:
                pending_seasons = []
                for s in seasons:
                    ckpt_mgr = CheckpointManager(checkpoint_key=f"season_{s}")
                    if ckpt_mgr.load_checkpoint():
                        pending_seasons.append(s)
                    else:
                        # Si no hay checkpoint, verificar si tiene partidos. 
                        # Esto es una simplificación, podrías querer re-ingestar si no está completa.
                        # Pero para el modo "resume" masivo, asumimos que si no hay checkpoint y hay datos, está ok.
                        has_games = session.query(Game.id).filter(Game.season == s).first() is not None
                        if not has_games:
                            pending_seasons.append(s)
                seasons = pending_seasons

            if not seasons:
                logger.info("No hay temporadas pendientes de procesar.")
            else:
                logger.info(f"Iniciando ingesta paralela para {len(seasons)} temporadas: {seasons}")
            
            # 3. Lanzar procesos para cada dos temporadas
            processes = {}
            season_batches = [tuple(seasons[i:i + 2]) for i in range(0, len(seasons), 2)]
            
            for batch in season_batches:
                batch_name = "_".join(batch)
                p = multiprocessing.Process(
                    target=run_worker_with_stagger,
                    args=(season_batch_worker_func, f"batch_{batch_name}", list(batch)),
                    name=f"Worker-{batch_name}"
                )
                p.start()
                processes[batch] = p
            
            # 4. Supervisar procesos de temporadas
            self._supervise_processes(processes, season_batch_worker_func, "batch")
            
            logger.info("Fase de temporadas completada. Iniciando sincronización de premios...")
            
            # 5. Sincronizar premios en paralelo
            player_ids = [pid for (pid,) in session.query(Player.id).filter(Player.awards_synced == False).all()]
            if player_ids:
                run_parallel_task(
                    awards_worker_func, 
                    player_ids, 
                    AWARDS_WORKERS, 
                    "awards_batch",
                    lambda bid: f"Awards-Batch-{bid}"
                )
            
            # 6. Sincronizar biografías detalladas en paralelo si hay jugadores nuevos
            if new_players_count > 0:
                pending_bio = [pid for (pid,) in session.query(Player.id).filter(Player.height == None).all()]
                if pending_bio:
                    logger.info(f"Sincronizando biografías para {len(pending_bio)} jugadores...")
                    run_parallel_task(
                        player_info_worker_func,
                        pending_bio,
                        AWARDS_WORKERS,
                        "player_info_batch",
                        lambda bid: f"Bio-Batch-{bid}"
                    )
            else:
                logger.info("No hay jugadores nuevos, saltando sincronización de biografías.")
            
            logger.info("✅ Ingesta completa histórica finalizada")
            
        finally:
            session.close()

    def _supervise_processes(self, processes: Dict[Tuple[str, ...], multiprocessing.Process], worker_func: Any, prefix: str):
        """Supervisa procesos y los relanza si fallan por error fatal."""
        active_processes = processes.copy()
        
        while active_processes:
            for key, p in list(active_processes.items()):
                if not p.is_alive():
                    if p.exitcode == 0:
                        logger.info(f"Process {p.name} finalizado con éxito.")
                        active_processes.pop(key)
                    else:
                        if p.exitcode == 42:
                            logger.error(f"Worker {p.name} falló con ERROR FATAL (partido fallido). Relanzando worker...")
                        else:
                            logger.warning(f"Process {p.name} falló (Code {p.exitcode}). Relanzando...")
                        
                        # key es una tupla de temporadas
                        batch_name = "_".join(key)
                        new_p = multiprocessing.Process(
                            target=run_worker_with_stagger,
                            args=(worker_func, f"{prefix}_{batch_name}", list(key)),
                            name=p.name
                        )
                        new_p.start()
                        active_processes[key] = new_p
                        time.sleep(2) # Evitar saturación en relanzamiento
            
            time.sleep(5)

    def _get_season_range(self, start: str, end: str) -> List[str]:
        """Genera lista de temporadas."""
        from ingestion.utils import normalize_season
        start = normalize_season(start)
        end = normalize_season(end)
        start_year = int(start.split('-')[0])
        end_year = int(end.split('-')[0])
        
        seasons = []
        for year in range(start_year, end_year + 1):
            next_year = (year + 1) % 100
            seasons.append(f"{year}-{next_year:02d}")
        return seasons


class IncrementalIngestion:
    """Maneja la ingesta incremental paralela."""
    
    def __init__(self, api_client: NBAApiClient, checkpoint_mgr: Optional[CheckpointManager] = None):
        self.api = api_client
        self.checkpoints = checkpoint_mgr or CheckpointManager()
        self.game_ingestion = GameIngestion(api_client)
        self.team_sync = TeamSync()
        self.player_sync = PlayerSync()
        
        self.derived = DerivedTablesGenerator()
    
    def run(self, limit_seasons: Optional[int] = None, reporter: Optional[ProgressReporter] = None):
        """Ejecuta la sincronización incremental."""
        limit_text = f"últimas {limit_seasons} temporadas" if limit_seasons else "todas las temporadas necesarias"
        logger.info(f"Iniciando ingesta incremental ({limit_text})...")
        if reporter: reporter.update(5, "Iniciando ingesta incremental...")
        
        session = get_session()
        try:
            # 1. Sincronizar entidades base proactivamente
            msg = "Sincronizando equipos y lista de jugadores..."
            logger.info(msg)
            if reporter: reporter.update(2, msg)
            self.team_sync.sync_all(session)
            self.player_sync.sync_all(session)

            # 2. Identificar temporadas a procesar
            all_seasons = sorted(get_all_seasons(), reverse=True)
            seasons_to_process = all_seasons[:limit_seasons] if limit_seasons else all_seasons
            
            new_seasons = set()
            
            # 3. Procesar cada temporada
            for i, season in enumerate(seasons_to_process):
                msg = f"Procesando temporada {season}..."
                logger.info(msg)
                if reporter:
                    # Progreso estimado (asumiendo que solemos mirar 1 o 2 temporadas en incremental)
                    progress = 5 + int((i / len(seasons_to_process)) * 60)
                    reporter.update(progress, msg)

                stop = False
                games_data = self.api.fetch_season_games(season)
                if not games_data: continue
                
                today_str = date.today().isoformat()
                for j, gd in enumerate(games_data):
                    gid = gd['game_id']
                    local_date = gd['game_date']
                    
                    existing = session.query(Game.status, Game.home_score, Game.date).filter(Game.id == gid).first()
                    is_really_finished = existing and (
                        (existing.status == 3) or 
                        (existing.status == 1 and existing.home_score is not None and 
                         existing.home_score > 0 and str(existing.date) < today_str)
                    )
                    
                    if is_really_finished:
                        if existing.date != local_date:
                            session.query(Game).filter(Game.id == gid).update({"date": local_date})
                            session.commit()
                        stop = True
                        break
                    
                    res = self.game_ingestion.ingest_game(session, gid, season_fallback=season)
                    if res is True:
                        new_seasons.add(season)
                    elif res is False:
                        raise FatalIngestionError(f"Fallo no recuperable en partido incremental {gid}")
                    
                    if j % 10 == 0:
                        clear_memory() # Liberar RAM periódicamente

                    time.sleep(API_DELAY)
                
                if stop:
                    logger.info("Detectado partido ya existente y finalizado. Deteniendo búsqueda incremental.")
                    break
            
            # 3. Regenerar tablas derivadas
            if new_seasons:
                msg = "Recalculando tablas de estadísticas agregadas..."
                logger.info(msg)
                if reporter: reporter.update(70, msg)
                self.derived.regenerate_for_seasons(session, list(new_seasons))
            
            # 4. Actualizar premios en paralelo (o secuencial en Render)
            active_player_ids = [pid for (pid,) in session.query(Player.id).filter(Player.is_active == True).order_by(Player.id).all()]
            if active_player_ids:
                msg = f"Sincronizando premios para {len(active_player_ids)} jugadores activos..."
                logger.info(msg)
                if reporter: reporter.update(80, msg)
                
                workers = get_max_workers(AWARDS_WORKERS)
                run_parallel_task(
                    awards_worker_func, 
                    active_player_ids, 
                    workers, 
                    "incremental_awards_batch",
                    lambda bid: f"Inc-Awards-Batch-{bid}"
                )
            
            # 5. Actualizar biografías de jugadores si hay nuevos
            pending_bio = [pid for (pid,) in session.query(Player.id).filter(
                Player.height == None
            ).all()]
            if pending_bio:
                msg = f"Sincronizando biografías para {len(pending_bio)} jugadores nuevos..."
                logger.info(msg)
                if reporter: reporter.update(90, msg)
                
                workers = get_max_workers(AWARDS_WORKERS)
                run_parallel_task(
                    player_info_worker_func,
                    pending_bio,
                    workers,
                    "incremental_player_info_batch",
                    lambda bid: f"Inc-Bio-Batch-{bid}"
                )
            
            if reporter: reporter.complete("Base de datos actualizada con éxito.")
            logger.info("✅ Ingesta incremental finalizada")
            
        except Exception as e:
            if reporter: reporter.fail(str(e))
            logger.error(f"Error en ingesta incremental: {e}")
            raise
        finally:
            session.close()
            clear_memory()

