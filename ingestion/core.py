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
from ingestion.config import API_DELAY
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
    reporter = None
    try:
        from ingestion.utils import ProgressReporter
        reporter = ProgressReporter(f"Season-{season}", session_factory=get_session)
        reporter.update(0, f"Iniciando temporada {season}...")
        
        # 1. Ingestar partidos
        season_ingest = SeasonIngestion(api_client, ckpt_mgr)
        season_ingest.ingest_season(session, season, resume_game_id, reporter=reporter)
        
        # 2. Generar tablas derivadas inmediatamente
        derived = DerivedTablesGenerator()
        
        # CAMBIO: Solo actualizar reporter, no loguear (evita duplicación)
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
    """Función de worker para procesar un lote de temporadas (generalmente 2)."""
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
        from ingestion.utils import ProgressReporter
        reporter = ProgressReporter(actual_task_name, session_factory=get_session)
        
        awards_sync = PlayerAwardsSync(api_client)
        # Adaptar sync_batch para que informe al reporter si fuera necesario, 
        # o simplemente reportar aquí el inicio/fin del batch.
        # Por simplicidad ahora, reportamos progreso general del batch:
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
        from ingestion.utils import ProgressReporter
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


class GameIngestion:
    """Maneja la ingesta de un partido individual."""
    
    def __init__(self, api_client: NBAApiClient):
        """Inicializa el ingestor de partidos.
        
        Args:
            api_client: Cliente API unificado
        """
        self.api = api_client
    
    def ingest_game(
        self, 
        session: Session, 
        game_id: str, 
        is_rs: bool,
        is_po: bool,
        is_pi: bool,
        is_ist: bool,
        season_fallback: Optional[str] = None
    ) -> Optional[bool]:
        """Ingiere un partido completo desde la API.
        
        Args:
            session: Sesión de SQLAlchemy
            game_id: ID del partido
            is_rs: True si es Regular Season
            is_po: True si es Playoffs
            is_pi: True si es PlayIn
            is_ist: True si es In-Season Tournament (NBA Cup)
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
            game_date_str = data.get('gameEt') or data.get('gameTimeUTC')
            game_date = parse_date(game_date_str)
            
            # Deducir temporada
            season = self._deduce_season(game_id, data, season_fallback, game_date)
            if not season:
                logger.error(f"No se pudo determinar la temporada para {game_id}")
                return False
            
            # Determinar si es un partido de la NBA Cup (IST) usando el subtipo oficial de la API
            # in-season: Fase de Grupos
            # in-season-knockout: Cuartos, Semis y Final
            game_subtype = data.get('gameSubtype') or ""
            api_is_ist = game_subtype.startswith('in-season')
            
            # Usar la verdad de la API si está disponible, si no los flags pasados
            actual_ist = api_is_ist if game_subtype else is_ist
            
            home_team_id, away_team_id = data['homeTeamId'], data['awayTeamId']
            
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
            
            # Obtener marcadores de forma segura
            h_score = safe_int(data['homeTeam'].get('score', 0), default=0)
            a_score = safe_int(data['awayTeam'].get('score', 0), default=0)
            
            if not game_exists:
                game = Game(
                    id=game_id, date=game_date, season=season, status=data.get('gameStatus'),
                    home_team_id=home_team_id, away_team_id=away_team_id,
                    home_score=h_score, away_score=a_score,
                    winner_team_id=home_team_id if h_score > a_score else away_team_id,
                    rs=is_rs, po=is_po, pi=is_pi, ist=actual_ist, 
                    quarter_scores=quarter_scores
                )
                session.add(game)
            else:
                game.date = game_date # Actualizar fecha por si cambió (UTC -> Local)
                game.status = data.get('gameStatus')
                game.home_score = h_score
                game.away_score = a_score
                game.winner_team_id = home_team_id if h_score > a_score else away_team_id
                game.quarter_scores = quarter_scores
                # Actualizar flags de tipo siempre (la API manda)
                game.rs = is_rs
                game.po = is_po
                game.pi = is_pi
                game.ist = actual_ist
            
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
                        p_id = safe_int(r.get('PLAYER_ID'), default=-1)
                        if p_id > 0 and r.get('PLAYER_NAME'):
                            name_fallback[p_id] = r['PLAYER_NAME']
            
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
        if game_id and len(game_id) == 10:
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
                # Usar safe_int para evitar el crash con NaN en datos históricos
                player_id = safe_int(self._get_col(row, 'personId', 'PLAYER_ID', 'playerId'), default=-1)
                team_id = safe_int(self._get_col(row, 'teamId', 'TEAM_ID'), default=-1)
                
                # Si no hay IDs válidos, es probable que sea una fila de totales o datos corruptos
                if player_id <= 0 or team_id <= 0:
                    continue
                
                from ingestion.utils import is_valid_team_id
                if not is_valid_team_id(team_id, session=session):
                    continue
                
                # Intentar obtener nombre completo primero
                first_name = self._get_col(row, 'firstName')
                last_name = self._get_col(row, 'familyName')
                
                if first_name and last_name:
                    player_name = f"{first_name} {last_name}".strip()
                else:
                    player_name = self._get_col(row, 'PLAYER_NAME', 'playerName', 'name', 'nameI')
                
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
        resume_from_game_id: Optional[str] = None,
        reporter: Optional[Any] = None
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
        
        # Obtener lista de partidos con flags de tipo oficiales (fatal=True)
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
                    # CAMBIO: Solo reportar, no loguear (reporter ya loguea internamente cada N segundos)
                    if reporter:
                        progress = int((i / len(games_data)) * 90)
                        reporter.update(progress, f"{i}/{len(games_data)} partidos")
                
                # Actualizar progreso después de CADA partido (no solo cada 20)
                if reporter and i > 0:
                    reporter.increment(f"Partido {gid[:8]}")
                
                # Asegurar que la fecha en DB sea la fecha local correcta
                existing = session.query(Game).filter(Game.id == gid).first()
                if existing and existing.date != local_date:
                    logger.debug(f"Corrigiendo fecha para partido {gid}: {existing.date} -> {local_date}")
                    existing.date = local_date
                    session.commit()

                # Ingestar con flags obligatorios desde la API
                res = self.game_ingestion.ingest_game(
                    session, gid, 
                    is_rs=gd.get('is_rs', False),
                    is_po=gd.get('is_po', False),
                    is_pi=gd.get('is_pi', False),
                    is_ist=gd.get('is_ist', False),
                    season_fallback=season
                )
                if res is True:
                    success += 1
                elif res is False:
                    raise FatalIngestionError(f"Fallo no recuperable en partido {gid}")
                
                time.sleep(API_DELAY)
            except FatalIngestionError:
                # Guardar checkpoint exacto antes de reiniciar
                self.checkpoints.save_games_checkpoint(season, gid, {'total': len(games_data)})
                logger.error(f"Error fatal en temporada {season}, partido {gid}. Checkpoint guardado.")
                raise
        
        logger.info(f"Temporada {season} completada: {success} exitosos, {failed} fallidos")
        return {'total': len(games_data), 'success': success, 'failed': failed}


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
        from ingestion.utils import normalize_season
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

