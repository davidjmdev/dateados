"""Ingestores de datos individuales y por temporada.

Este módulo contiene la lógica para ingestar partidos individuales y temporadas completas.
"""
import logging
import time
from typing import Optional, Dict, Any, List
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from db.models import Game, PlayerGameStats
from db.services import get_or_create_player
from ingestion.api_client import NBAApiClient
from ingestion.checkpoints import CheckpointManager
from ingestion.config import API_DELAY
from ingestion.api_common import FatalIngestionError
from ingestion.utils import (
    parse_date, convert_minutes_to_interval, safe_int, safe_float
)

logger = logging.getLogger(__name__)

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
        """Ingiere un partido completo desde la API."""
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
            
            # Determinar si es un partido de la NBA Cup (IST)
            game_subtype = data.get('gameSubtype') or ""
            api_is_ist = game_subtype.startswith('in-season')
            actual_ist = api_is_ist if game_subtype else is_ist
            
            home_team_id, away_team_id = data['homeTeamId'], data['awayTeamId']
            
            quarter_scores = None
            try:
                quarter_scores = {
                    'home': [p.get('score', p.get('points')) for p in data['homeTeam']['periods']],
                    'away': [p.get('score', p.get('points')) for p in data['awayTeam']['periods']]
                }
            except:
                pass
            
            # Crear o actualizar Game
            game = session.query(Game).filter(Game.id == game_id).first()
            game_exists = game is not None
            
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
                game.date = game_date
                game.status = data.get('gameStatus')
                game.home_score = h_score
                game.away_score = a_score
                game.winner_team_id = home_team_id if h_score > a_score else away_team_id
                game.quarter_scores = quarter_scores
                game.rs = is_rs
                game.po = is_po
                game.pi = is_pi
                game.ist = actual_ist
            
            # Obtener boxscore
            traditional = self.api.fetch_game_boxscore(game_id)
            if traditional is None:
                return False
            
            dfs = traditional.get_data_frames()
            if len(dfs) == 0 or dfs[0].empty:
                session.commit()
                return None
            
            # Fallback V2
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
        if game_id and len(game_id) == 10:
            try:
                yy = int(game_id[3:5])
                century = 2000 if yy < 50 else 1900
                year = century + yy
                season = f"{year}-{(year+1)%100:02d}"
            except: pass
        
        if not season: season = data.get('seasonYear')
        if not season: season = season_fallback
        if not season and game_date:
            year = game_date.year
            month = game_date.month
            if month >= 10: season = f"{year}-{(year+1)%100:02d}"
            else: season = f"{year-1}-{year%100:02d}"
        return season
    
    def _get_col(self, row, *names):
        """Busca un valor en múltiples nombres de columna."""
        for name in names:
            if name in row: return row[name]
        return None
    
    def _process_player_stats(self, session, game_id, df, game_exists, name_fallback):
        """Procesa estadísticas de jugadores."""
        for _, row in df.iterrows():
            try:
                player_id = safe_int(self._get_col(row, 'personId', 'PLAYER_ID', 'playerId'), default=-1)
                team_id = safe_int(self._get_col(row, 'teamId', 'TEAM_ID'), default=-1)
                
                if player_id <= 0 or team_id <= 0: continue
                
                from db.services import is_valid_team_id
                if not is_valid_team_id(team_id, session=session): continue
                
                first_name = self._get_col(row, 'firstName')
                last_name = self._get_col(row, 'familyName')
                
                if first_name and last_name:
                    player_name = f"{first_name} {last_name}".strip()
                else:
                    player_name = self._get_col(row, 'PLAYER_NAME', 'playerName', 'name', 'nameI')
                
                player_min = convert_minutes_to_interval(self._get_col(row, 'minutes', 'MIN', 'min') or '0:00')
                
                if (not player_name or player_name.strip() == '') and player_min.total_seconds() == 0:
                    if not (name_fallback and player_id in name_fallback):
                        if (self._get_col(row, 'points', 'PTS') or 0) == 0: continue
                
                if not player_name or player_name.strip() == '':
                    player_name = name_fallback.get(player_id, f"Player {player_id}")
                
                get_or_create_player(session, player_id, {'full_name': player_name})
                
                existing_stat = session.query(PlayerGameStats).filter(
                    and_(PlayerGameStats.game_id == game_id, PlayerGameStats.player_id == player_id)
                ).first()
                
                stat_data = self._build_stat_data(row, game_id, player_id, team_id, player_min)

                if existing_stat:
                    for k, v in stat_data.items():
                        setattr(existing_stat, k, v)
                else:
                    session.add(PlayerGameStats(**stat_data))
                    
            except FatalIngestionError: raise
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
        """Ingiere todos los partidos de una temporada."""
        logger.info(f"Iniciando ingesta de temporada {season}...")
        
        games_data = self.api.fetch_season_games(season)
        if not games_data:
            return {'total': 0, 'success': 0, 'failed': 0}
        
        # Optimización: Obtener IDs completados
        completed_games = {
            gid for (gid,) in session.query(Game.id)
            .join(PlayerGameStats, PlayerGameStats.game_id == Game.id)
            .filter(
                Game.season == season,
                or_(Game.status == 3, and_(Game.status == 1, Game.home_score > 0))
            )
            .distinct().all()
        }
        
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
            
            if gid in completed_games:
                success += 1
                continue
            
            try:
                # Checkpoint y reporte
                if i > 0 and i % 20 == 0:
                    self.checkpoints.save_games_checkpoint(season, gid, {'total': len(games_data)})
                    if reporter:
                        progress = int((i / len(games_data)) * 90)
                        reporter.update(progress, f"{i}/{len(games_data)} partidos")
                
                if reporter and i > 0:
                    reporter.increment(f"Partido {gid[:8]}")
                
                # Corregir fecha
                existing = session.query(Game).filter(Game.id == gid).first()
                if existing and existing.date != local_date:
                    existing.date = local_date
                    session.commit()

                res = self.game_ingestion.ingest_game(
                    session, gid, 
                    is_rs=gd.get('is_rs', False),
                    is_po=gd.get('is_po', False),
                    is_pi=gd.get('is_pi', False),
                    is_ist=gd.get('is_ist', False),
                    season_fallback=season
                )
                if res is True: success += 1
                elif res is False: raise FatalIngestionError(f"Fallo no recuperable en partido {gid}")
                
                time.sleep(API_DELAY)
            except FatalIngestionError:
                self.checkpoints.save_games_checkpoint(season, gid, {'total': len(games_data)})
                logger.error(f"Error fatal en temporada {season}, partido {gid}. Checkpoint guardado.")
                raise
        
        logger.info(f"Temporada {season} completada: {success} exitosos, {failed} fallidos")
        return {'total': len(games_data), 'success': success, 'failed': failed}
