"""Sincronización de equipos, jugadores y biografías.

Este módulo maneja la sincronización de entidades base desde la API de la NBA:
- Equipos (desde lista estática)
- Jugadores (desde lista estática y API detallada)
- Premios de jugadores (desde PlayerAwards)
- Dorsales (desde CommonTeamRoster)
"""

import logging
import time
import math
from datetime import timedelta
from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, or_, func

from nba_api.stats.static import teams as nba_teams, players as nba_players

from db.models import Player, Team, PlayerTeamSeason, PlayerAward, Game, PlayerGameStats
from ingestion.api_client import NBAApiClient
from ingestion.checkpoints import CheckpointManager
from ingestion.config import API_DELAY
from ingestion.api_common import FatalIngestionError
from ingestion.utils import (
    safe_int, safe_float, safe_int_or_none, parse_date, 
    normalize_season
)
from db.logging import log_step, log_success

logger = logging.getLogger("dateados.ingestion.sync")


class TeamSync:
    """Sincroniza equipos desde lista estática."""
    
    def sync_all(self, session: Session):
        """Sincroniza todos los equipos desde nba_api.stats.static.
        
        Args:
            session: Sesión de SQLAlchemy
        """
        logger.info("Sincronizando equipos...")
        # nba_teams.get_teams() retorna solo las 30 franquicias activas actuales
        all_teams = nba_teams.get_teams()
        
        for team_data in all_teams:
            team_id = team_data['id']
            abbr = team_data['abbreviation']
            
            # Verificar si la abreviatura está tomada por OTRA ID
            conflict = session.query(Team).filter(
                and_(Team.abbreviation == abbr, Team.id != team_id)
            ).first()
            
            if conflict:
                logger.warning(f"Conflicto: Abreviatura {abbr} ya usada por ID {conflict.id}. Renombrando nueva entrada para ID {team_id}...")
                abbr = f"{abbr}_{team_id}"
 
            existing = session.query(Team).filter(Team.id == team_id).first()
            
            team_info = {
                'full_name': team_data['full_name'],
                'abbreviation': abbr,
                'nickname': team_data['nickname'],
                'city': team_data['city'],
                'state': team_data['state'],
                'year_founded': team_data['year_founded'],
            }
            
            if existing:
                for key, value in team_info.items():
                    setattr(existing, key, value)
            else:
                team = Team(id=team_id, **team_info)
                session.add(team)
        
        session.commit()
        
        # Sincronizar conferencias y divisiones (no vienen en el static)
        self.sync_conferences(session)
        
        logger.info(f"Sincronizados {len(all_teams)} equipos")

    def sync_conferences(self, session: Session):
        """Sincroniza conferencias y divisiones desde LeagueStandings.
        
        Args:
            session: Sesión de SQLAlchemy
        """
        from nba_api.stats.endpoints import LeagueStandingsV3
        from datetime import date
        
        logger.info("Sincronizando conferencias y divisiones de equipos...")
        try:
            # Usar la temporada actual para obtener la info más reciente
            today = date.today()
            year = today.year if today.month >= 10 else today.year - 1
            current_season = f"{year}-{(year + 1) % 100:02d}"
            
            standings = LeagueStandingsV3(season=current_season).get_data_frames()[0]
            
            updated_count = 0
            for _, row in standings.iterrows():
                team_id = int(row['TeamID'])
                conf = row['Conference']
                div = row['Division']
                
                team = session.query(Team).filter(Team.id == team_id).first()
                if team:
                    team.conference = conf
                    team.division = div
                    updated_count += 1
            
            session.commit()
            logger.info(f"Sincronizadas conferencias para {updated_count} equipos")
        except Exception as e:
            logger.warning(f"No se pudieron sincronizar conferencias: {e}")
            session.rollback()


def safe_str(value: Any, default: Optional[str] = None) -> Optional[str]:
    """Convierte a string de forma segura manejando None y NaN."""
    if value is None:
        return default
    s = str(value).strip()
    if s.lower() in ['none', 'nan', 'null', '']:
        return default
    return s


class PlayerSync:
    """Sincroniza jugadores desde lista estática."""
    
    def sync_all(self, session: Session, update_existing: bool = False) -> int:
        """Sincroniza jugadores desde lista estática.
        
        Args:
            session: Sesión de SQLAlchemy
            update_existing: Si True, actualiza jugadores existentes
            
        Returns:
            int: Número de nuevos jugadores añadidos
        """
        logger.info("Sincronizando base de datos de jugadores...")
        all_players = nba_players.get_players()
        
        count = 0
        for player_data in all_players:
            player_id = player_data['id']
            
            existing = session.query(Player).filter(Player.id == player_id).first()
            
            player_info = {
                'full_name': player_data['full_name'],
                'is_active': player_data.get('is_active', False)
            }
            
            if existing:
                # Siempre actualizamos el estado de actividad
                existing.is_active = player_info['is_active']
                
                # REPARACIÓN AUTOMÁTICA: Si el nombre actual parece una inicial (ej: "S. Gilgeous-Alexander")
                # lo actualizamos con el nombre completo de la lista estática.
                current_name = existing.full_name or ""
                is_initial_format = (
                    len(current_name) > 2 and 
                    current_name[1] == '.' and 
                    current_name[2] == ' '
                )
                
                if update_existing or is_initial_format:
                    existing.full_name = player_info['full_name']
            elif player_info['is_active']:
                # Solo añadimos jugadores nuevos si están activos.
                # Los jugadores históricos se añadirán dinámicamente al ingestar partidos.
                player = Player(id=player_id, **player_info)
                session.add(player)
                count += 1
            else:
                # También añadimos jugadores históricos para tener la base de datos poblada
                player = Player(id=player_id, **player_info)
                session.add(player)
                count += 1
        
        session.commit()
        logger.info(f"Añadidos {count} nuevos jugadores")
        return count

    def sync_detailed_batch(
        self, 
        session: Session, 
        player_ids: List[int], 
        api: NBAApiClient,
        checkpoint_mgr: CheckpointManager,
        show_progress: bool = True,
        reporter: Optional[Any] = None
    ):
        """Sincroniza información detallada (biografía) para un lote de jugadores.
        
        Args:
            session: Sesión de SQLAlchemy
            player_ids: Lista de IDs de jugadores
            api: Cliente de la API
            checkpoint_mgr: Manager de checkpoints
            show_progress: Si True, registra progreso en log
            reporter: Opcional, reporter de progreso
        """
        total = len(player_ids)
        if show_progress:
            logger.info(f"Sincronizando biografía detallada para {total} jugadores...")
        
        # NUEVO: Configurar total para métricas automáticas
        if reporter:
            reporter.set_total(total)
            reporter.update(0, f"Iniciando {total} jugadores...")
            
        for i, player_id in enumerate(player_ids):
            try:
                # Checkpoint cada 20 jugadores
                if i % 20 == 0 and i > 0:
                    checkpoint_mgr.save_sync_checkpoint('player_info', player_id)
                
                # NUEVO: Actualizar progreso DESPUÉS DE CADA JUGADOR
                if reporter:
                    reporter.increment(f"ID {player_id}")
 
                # Obtener información detallada (fatal=True para relanzar si hay bloqueo)
                info_obj = api.fetch_player_info(player_id, fatal=True)
                if not info_obj:
                    continue
                
                df = info_obj.get_data_frames()[0]
                if df.empty:
                    continue
                
                row = df.iloc[0]
                player = session.query(Player).filter(Player.id == player_id).first()
                
                if player:
                    # Mapeo de campos
                    # Actualizar nombre completo (importante para búsqueda)
                    player.full_name = safe_str(row.get('DISPLAY_FIRST_LAST'), player.full_name)
                    
                    player.birthdate = parse_date(row.get('BIRTHDATE'))
                    player.height = safe_str(row.get('HEIGHT'))
                    player.weight = safe_int_or_none(row.get('WEIGHT'))
                    player.school = safe_str(row.get('SCHOOL'))
                    player.country = safe_str(row.get('COUNTRY'))
                    player.jersey = safe_str(row.get('JERSEY'))
                    player.position = safe_str(row.get('POSITION'))
                    
                    # Draft info
                    player.draft_year = safe_int_or_none(row.get('DRAFT_YEAR'))
                    player.draft_round = safe_int_or_none(row.get('DRAFT_ROUND'))
                    player.draft_number = safe_int_or_none(row.get('DRAFT_NUMBER'))
                    
                    # Career years
                    player.from_year = safe_int_or_none(row.get('FROM_YEAR'))
                    player.to_year = safe_int_or_none(row.get('TO_YEAR'))
                    player.season_exp = safe_int_or_none(row.get('SEASON_EXP'))
                    
                    # Marcar como sincronizado
                    player.bio_synced = True
                    
                session.commit()
                time.sleep(API_DELAY)
                
            except FatalIngestionError as e:
                if 'resultSet' in str(e):
                    # Error de datos faltantes en la API para este jugador específico.
                    # Lo marcamos como procesado para no intentar sincronizarlo de nuevo.
                    player = session.query(Player).filter(Player.id == player_id).first()
                    if player:
                        player.bio_synced = True
                    session.commit()
                    logger.warning(f"Jugador {player_id} no tiene biografía en la API. Marcado como sincronizado.")
                    continue
                raise
            except Exception as e:
                logger.error(f"Error sincronizando biografía para {player_id}: {e}")
                session.rollback()
                continue
        
        if show_progress:
            logger.info(f"Sincronización de biografías completada para batch {checkpoint_mgr.checkpoint_key}")



def get_players_needing_award_sync(
    session: Session, 
    force_all: bool = False,
    days_threshold: int = 15
) -> List[int]:
    """Retorna IDs de jugadores que necesitan sincronización de premios.
    
    Estrategia:
    - Temporada de premios (Abril-Junio): todos los activos
    - Resto del año: solo activos con actividad reciente Y desactualizados
    
    Args:
        session: Sesión de SQLAlchemy
        force_all: Si True, ignora filtros y retorna todos los activos
        days_threshold: Días desde última sincronización para considerar desactualizado
        
    Returns:
        Lista de IDs de jugadores
    """
    from datetime import datetime, timedelta
    
    current_date = datetime.now()
    current_month = current_date.month
    
    # Modo force: todos los activos
    if force_all:
        logger.info("Modo full: sincronizando todos los jugadores activos")
        query = session.query(Player.id).filter(Player.is_active == True)
        return [pid for (pid,) in query.all()]
    
    # Temporada de premios (Abril-Junio): todos los activos
    if current_month in [4, 5, 6]:
        logger.info("Temporada de premios detectada: sincronizando todos los activos")
        query = session.query(Player.id).filter(Player.is_active == True)
        return [pid for (pid,) in query.all()]
    
    # Resto del año: filtrado inteligente
    threshold_date = current_date - timedelta(days=days_threshold)
    
    # Subquery: jugadores con partidos recientes (últimos N días)
    recent_players_subq = (
        session.query(PlayerGameStats.player_id)
        .join(Game, PlayerGameStats.game_id == Game.id)
        .filter(Game.date >= threshold_date.date())
        .distinct()
        .subquery()
    )
    
    # Query principal: activos + (nunca sincronizados O desactualizados con actividad)
    query = session.query(Player.id).filter(
        and_(
            Player.is_active == True,
            or_(
                # Nunca sincronizados
                Player.last_award_sync == None,
                # Desactualizados Y con actividad reciente
                and_(
                    Player.last_award_sync < threshold_date,
                    Player.id.in_(session.query(recent_players_subq))
                )
            )
        )
    )
    
    player_ids = [pid for (pid,) in query.all()]
    logger.info(f"Filtrado inteligente: {len(player_ids)} jugadores necesitan sync de premios")
    return player_ids


class PlayerAwardsSync:
    """Sincroniza premios de jugadores."""
    
    # Premios que no queremos importar por ser irrelevantes o duplicados
    AWARD_BLACKLIST = [
        'Sporting News',
        'IBM Award',
        'Olympic Appearance',
        'NBA Sportsmanship',
        'J. Walter Kennedy Citizenship',
        'NBA Comeback Player of the Year'
    ]
    
    # Mapeo de nombres de premios a categorías simplificadas
    AWARD_MAP = {
        'NBA Finals Most Valuable Player': 'Finals MVP',
        'NBA All-Star Most Valuable Player': 'All-Star MVP',
        'NBA Most Valuable Player': 'MVP',
        'NBA Champion': 'Champion',
        'All-Defensive Team': 'All-Defensive',
        'All-Rookie Team': 'All-Rookie',
        'All-NBA': 'All-NBA',
        'Olympic Gold Medal': 'Olympic Gold',
        'Olympic Silver Medal': 'Olympic Silver',
        'Olympic Bronze Medal': 'Olympic Bronze',
        'NBA Rookie of the Year': 'ROY',
        'NBA Defensive Player of the Year': 'DPOY',
        'NBA Sixth Man of the Year': '6MOY',
        'NBA Most Improved Player': 'MIP',
        'NBA Coach of the Year': 'COY',
        'NBA All-Star': 'All-Star',
        'NBA Cup Most Valuable Player': 'NBA Cup MVP',
        'NBA Cup All-Tournament Team': 'NBA Cup Team',
        'NBA Cup Champion': 'NBA Cup',
        'NBA Player of the Month': 'POM',
        'NBA Player of the Week': 'POW',
        'NBA Rookie of the Month': 'ROM',
    }
    
    def __init__(self, api_client: NBAApiClient):
        self.api = api_client
    
    def sync_batch(
        self, 
        session: Session, 
        player_ids: List[int], 
        checkpoint_mgr: CheckpointManager,
        checkpoint_context: Optional[Dict[str, Any]] = None,
        resume_player_id: Optional[int] = None,
        show_progress: bool = True,
        reporter: Optional[Any] = None
    ):
        """Sincroniza premios para un batch de jugadores.
        
        Args:
            session: Sesión de SQLAlchemy
            player_ids: Lista de IDs de jugadores
            checkpoint_mgr: Manager de checkpoints
            checkpoint_context: Contexto adicional para el checkpoint
            resume_player_id: ID del jugador desde donde reanudar
            show_progress: Si True, registra progreso en log
            reporter: Opcional, reporter de progreso
            
        Raises:
            FatalIngestionError: Si hay errores persistentes de API
        """
        if resume_player_id and resume_player_id in player_ids:
            idx = player_ids.index(resume_player_id)
            player_ids = player_ids[idx:]
            logger.info(f"Reanudando sincronización de premios desde jugador {resume_player_id}")

        total = len(player_ids)
        if show_progress:
            logger.info(f"Sincronizando premios para {total} jugadores...")
        
        # NUEVO: Configurar total en el reporter para métricas automáticas
        if reporter:
            reporter.set_total(total)
            reporter.update(0, f"Iniciando {total} jugadores...")
        
        for i, player_id in enumerate(player_ids):
            try:
                # Cada 10 jugadores: guardar checkpoint y loguear progreso
                if i % 10 == 0 and i > 0:
                    checkpoint_mgr.save_sync_checkpoint('awards', player_id, checkpoint_context)
                    logger.info(f"📦 [Lote Awards] Procesados {i}/{total} jugadores...")
                
                # NUEVO: Actualizar progreso DESPUÉS DE CADA JUGADOR (no solo cada 10)
                if reporter:
                    reporter.increment(f"ID {player_id}")
                
                # Obtener premios desde API (fatal=True)
                player_awards = self.api.fetch_player_awards(player_id, fatal=True)
                
                if not player_awards:
                    continue
                
                df = player_awards.get_data_frames()[0]
                if df.empty:
                    # Marcar como sincronizado aunque esté vacío para no reintentar
                    from datetime import datetime
                    player = session.query(Player).filter(Player.id == player_id).first()
                    if player:
                        player.awards_synced = True
                        player.last_award_sync = datetime.now()
                    session.commit()
                    time.sleep(API_DELAY) # Respetar delay incluso si está vacío
                    continue
                
                # Procesar premios
                self._process_awards(session, player_id, df)
                
                # Marcar como sincronizado
                from datetime import datetime
                player = session.query(Player).filter(Player.id == player_id).first()
                if player:
                    player.awards_synced = True
                    player.last_award_sync = datetime.now()
                
                session.commit()
                time.sleep(API_DELAY)
                
            except FatalIngestionError:
                # Guardar checkpoint exacto antes de fallar
                checkpoint_mgr.save_sync_checkpoint('awards', player_id, checkpoint_context)
                logger.error(f"Error fatal sincronizando premios para jugador {player_id}. Checkpoint guardado.")
                raise
            except Exception as e:
                logger.error(f"Error sincronizando premios para {player_id}: {e}")
                session.rollback()
                continue
        
        if show_progress:
            logger.info(f"Sincronización de premios completada para batch {checkpoint_mgr.checkpoint_key}")
    
    def _process_awards(self, session: Session, player_id: int, df):
        """Procesa y guarda los premios de un jugador.
        
        Args:
            session: Sesión de SQLAlchemy
            player_id: ID del jugador
            df: DataFrame con premios desde la API
        """
        # Ordenar claves por longitud para coincidir con la más específica primero
        sorted_keys = sorted(self.AWARD_MAP.keys(), key=len, reverse=True)
        
        for _, row in df.iterrows():
            description = str(row.get('DESCRIPTION', '')).strip()
            season = str(row.get('SEASON', '')).strip()
            
            # 1. Filtrar premios en la lista negra
            if any(blacklisted in description for blacklisted in self.AWARD_BLACKLIST):
                continue
                
            all_nba_team = str(row.get('ALL_NBA_TEAM_NUMBER', '')).strip()
            
            # 2. Construir nombre completo de forma robusta
            full_award_name = description
            if all_nba_team and all_nba_team.isdigit() and all_nba_team in ['1', '2', '3']:
                suffix = 'st' if all_nba_team == '1' else ('nd' if all_nba_team == '2' else 'rd')
                full_award_name = f"{description} {all_nba_team}{suffix} Team"
            
            # 3. Determinar tipo simplificado
            award_type = self._classify_award(description, sorted_keys)
            
            # Guardar en BD
            try:
                existing = session.query(PlayerAward).filter(
                    and_(
                        PlayerAward.player_id == player_id,
                        PlayerAward.season == season,
                        PlayerAward.award_name == full_award_name
                    )
                ).first()
                
                if not existing:
                    award = PlayerAward(
                        player_id=player_id,
                        season=season,
                        award_type=award_type,
                        award_name=full_award_name,
                        description=None
                    )
                    session.add(award)
            except Exception:
                session.rollback()
                continue
    
    def _classify_award(self, description: str, sorted_keys: List[str]) -> str:
        """Clasifica un premio en una categoría simplificada.
        
        Args:
            description: Descripción del premio
            sorted_keys: Claves del mapa ordenadas por longitud
            
        Returns:
            Tipo de premio simplificado
        """
        # Coincidencias muy específicas primero
        if 'All-Star' in description and ('Most Valuable Player' in description or 'MVP' in description):
            return 'All-Star MVP'
        elif 'Finals' in description and ('Most Valuable Player' in description or 'MVP' in description):
            return 'Finals MVP'
        elif 'NBA Cup' in description and ('Most Valuable Player' in description or 'MVP' in description):
            return 'NBA Cup MVP'
        elif 'Most Valuable Player' in description or 'MVP' in description:
            if 'Cup' not in description and 'All-Star' not in description and 'Finals' not in description:
                return 'MVP'
        
        # Usar el mapa
        for key in sorted_keys:
            if key.lower() in description.lower():
                return self.AWARD_MAP[key]
        
        # Caso especial All-Star
        if 'all-star' in description.lower():
            if 'mvp' not in description.lower() and 'most valuable player' not in description.lower():
                return 'All-Star'
        
        return 'Other'


def update_champions(session: Session, season: str):
    """Identifica al campeón de la temporada y de la NBA Cup.
    
    Args:
        session: Sesión de SQLAlchemy
        season: Temporada (ej: "2023-24")
    """
    try:
        # NBA Champion (último partido de Playoffs)
        last_po_game = session.query(Game).filter(
            and_(Game.season == season, Game.po == True)
        ).order_by(desc(Game.date)).first()
        
        if last_po_game and last_po_game.winner_team_id:
            winner_team_id = last_po_game.winner_team_id
            champion_players = session.query(PlayerTeamSeason).filter(
                and_(
                    PlayerTeamSeason.season == season,
                    PlayerTeamSeason.team_id == winner_team_id
                )
            ).all()
            
            for cp in champion_players:
                existing = session.query(PlayerAward).filter(
                    and_(
                        PlayerAward.player_id == cp.player_id,
                        PlayerAward.season == season,
                        PlayerAward.award_type == 'Champion'
                    )
                ).first()
                
                if not existing:
                    award = PlayerAward(
                        player_id=cp.player_id,
                        season=season,
                        award_type='Champion',
                        award_name='NBA Champion'
                    )
                    session.add(award)
            
            logger.info(f"NBA Champion {season} procesado")
        
        # NBA Cup Champion (Final de Copa ID 006...)
        cup_final = session.query(Game).filter(
            and_(Game.season == season, Game.id.like('006%'))
        ).order_by(desc(Game.date)).first()
        
        if cup_final and cup_final.winner_team_id:
            winner_team_id = cup_final.winner_team_id
            cup_champion_players = session.query(PlayerTeamSeason).filter(
                and_(
                    PlayerTeamSeason.season == season,
                    PlayerTeamSeason.team_id == winner_team_id
                )
            ).all()
            
            for cp in cup_champion_players:
                existing = session.query(PlayerAward).filter(
                    and_(
                        PlayerAward.player_id == cp.player_id,
                        PlayerAward.season == season,
                        PlayerAward.award_type == 'NBA Cup'
                    )
                ).first()
                
                if not existing:
                    award = PlayerAward(
                        player_id=cp.player_id,
                        season=season,
                        award_type='NBA Cup',
                        award_name='NBA Cup Champion'
                    )
                    session.add(award)
            
            logger.info(f"NBA Cup Champion {season} procesado")
        
        session.commit()
        
    except Exception as e:
        logger.error(f"Error actualizando campeones for {season}: {e}")
        session.rollback()
