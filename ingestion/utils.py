import logging
import time
from datetime import date, datetime, timedelta
from typing import Optional, Dict, Any, List, Set, Tuple
from sqlalchemy import and_
from sqlalchemy.orm import Session
from dateutil import parser as date_parser

from db.models import Team, Player
from ingestion.config import (
    API_DELAY, MAX_RETRIES, API_TIMEOUT, 
    SPECIAL_EVENT_TEAM_IDS
)

logger = logging.getLogger(__name__)


class FatalIngestionError(Exception):
    """Excepción para errores fatales que requieren reinicio del proceso."""
    pass


def fetch_with_retry(api_call_func, max_retries=MAX_RETRIES, timeout=API_TIMEOUT, error_context="", fatal=True):
    """Ejecuta una llamada API con reintentos simplificados."""
    for attempt in range(max_retries):
        try:
            result = api_call_func()
            time.sleep(API_DELAY)
            return result
        except Exception as e:
            error_msg = str(e)
            logger.warning(
                f"Error en {error_context} (intento {attempt + 1}/{max_retries}): {error_msg}. "
                f"Esperando {timeout}s para reintentar..."
            )
            time.sleep(timeout)
            if attempt == max_retries - 1:
                logger.error(f"Fallo persistente en {error_context} tras {max_retries} intentos.")
                if fatal:
                    raise FatalIngestionError(f"Agotados reintentos en {error_context}: {error_msg}")
                return None
    return None


def normalize_season(season: str) -> str:
    """Normaliza el formato de temporada a "YYYY-YY"."""
    season = str(season).replace(' ', '').replace('-', '')
    if len(season) == 6:
        return f"{season[:4]}-{season[4:]}"
    if len(season) == 4:
        year = int(season)
        return f"{year}-{(year + 1) % 100:02d}"
    return season


def parse_game_id(game_id: str) -> Dict[str, Any]:
    """Extrae información del game_id."""
    if not game_id or len(game_id) < 3:
        return {'type': 'unknown', 'date': None, 'season': None}
    
    prefix = game_id[:3]
    game_type_map = {
        '001': 'PRESEASON', '002': 'RS', '003': 'ALLSTAR', 
        '004': 'PO', '005': 'PI', '006': 'IST'
    }
    game_type = game_type_map.get(prefix, 'unknown')
    
    if len(game_id) >= 11:
        try:
            year, month, day = int(game_id[3:7]), int(game_id[7:9]), int(game_id[9:11])
            game_date = date(year, month, day)
            season_start = year if month >= 10 else year - 1
            return {
                'type': game_type, 'date': game_date, 
                'season': f"{season_start}-{(season_start + 1) % 100:02d}"
            }
        except: pass
    return {'type': game_type, 'date': None, 'season': None}


def convert_minutes_to_interval(min_str: str) -> timedelta:
    """Convierte string de minutos a timedelta."""
    if not min_str or min_str == '': return timedelta(seconds=0)
    try:
        if ':' not in str(min_str):
            return timedelta(seconds=int(float(min_str) * 60))
        parts = str(min_str).split(':')
        if len(parts) == 2:
            return timedelta(seconds=int(parts[0]) * 60 + int(parts[1]))
        if len(parts) == 3:
            return timedelta(seconds=int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2]))
    except: pass
    return timedelta(seconds=0)


def is_valid_team_id(team_id: int, allow_special_events: bool = False, session: Optional[Session] = None) -> bool:
    """Verifica si un team_id es válido."""
    if allow_special_events and team_id in SPECIAL_EVENT_TEAM_IDS: return True
    if session and session.query(Team).filter(Team.id == team_id).first(): return True
    try:
        from nba_api.stats.static import teams as nba_teams_static
        if any(t['id'] == team_id for t in nba_teams_static.get_teams()): return True
    except: pass
    return 1610612737 <= team_id <= 1610612766


def get_or_create_team(session: Session, team_id: int, team_data: Optional[Dict[str, Any]] = None) -> Team:
    """Obtiene un equipo de la BD o lo crea si no existe."""
    team = session.query(Team).filter(Team.id == team_id).first()
    if team:
        if team_data:
            for k, v in team_data.items():
                if v and hasattr(team, k): setattr(team, k, v)
        return team
    
    # Intento de creación atómico para evitar race conditions
    savepoint = session.begin_nested()
    try:
        final_data = team_data.copy() if team_data else {}
        if not final_data.get('full_name') or not final_data.get('abbreviation'):
            try:
                from nba_api.stats.static import teams as nba_teams_static
                for t in nba_teams_static.get_teams():
                    if t['id'] == team_id:
                        if not final_data.get('full_name'): final_data['full_name'] = t['full_name']
                        if not final_data.get('abbreviation'): final_data['abbreviation'] = t['abbreviation']
                        if not final_data.get('city'): final_data['city'] = t['city']
                        if not final_data.get('nickname'): final_data['nickname'] = t['nickname']
                        break
            except: pass

        if not final_data.get('abbreviation'):
            final_data['abbreviation'] = f"TM_{team_id}"
        if not final_data.get('full_name'):
            final_data['full_name'] = f"Team {team_id}"

        new_team = Team(id=team_id, **final_data)
        session.add(new_team)
        session.flush()
        savepoint.commit()
        return new_team
    except Exception:
        savepoint.rollback()
        # Si falló, es que otro worker lo creó justo antes
        return session.query(Team).filter(Team.id == team_id).first()


def get_or_create_player(session: Session, player_id: int, player_data: Optional[Dict[str, Any]] = None) -> Player:
    """Obtiene un jugador de la BD o lo crea si no existe."""
    player = session.query(Player).filter(Player.id == player_id).first()
    
    if player:
        if player_data:
            for k, v in player_data.items():
                if v is not None and hasattr(player, k):
                    setattr(player, k, v)
        return player

    # Intento de creación atómico
    savepoint = session.begin_nested()
    try:
        # Creación rápida sin llamada a API de biografía (se delega a la fase final)
        name = player_data.get('full_name') if player_data else f'Player {player_id}'
        new_player = Player(id=player_id, full_name=name)
        
        # Si vienen datos en player_data (como jersey o posición del boxscore), los usamos
        if player_data:
            for k in ['position', 'jersey']:
                if k in player_data and player_data[k] is not None:
                    setattr(new_player, k, player_data[k])
                    
        session.add(new_player)
        session.flush()
        savepoint.commit()
        return new_player
    except Exception:
        savepoint.rollback()
        # Si falló, es que otro worker lo creó justo antes
        return session.query(Player).filter(Player.id == player_id).first()


def parse_date(date_str: Any) -> Optional[date]:

    """Parsea una fecha desde string o objeto date."""
    if not date_str: return None
    if isinstance(date_str, date): return date_str
    if isinstance(date_str, datetime): return date_str.date()
    try: return date_parser.parse(str(date_str)).date()
    except: return None


def safe_int(value: Any, default: int = 0) -> int:
    """Convierte un valor a int de forma segura."""
    try:
        import math
        f_val = float(value)
        return int(f_val) if not (math.isnan(f_val) or math.isinf(f_val)) else default
    except: return default


def safe_float(value: Any, default: float = 0.0) -> float:
    """Convierte un valor a float de forma segura."""
    try:
        import math
        f_val = float(value)
        return f_val if not (math.isnan(f_val) or math.isinf(f_val)) else default
    except: return default


def safe_int_or_none(value: Any) -> Optional[int]:
    """Convierte un valor a int o None si es 0, None o inválido."""
    try:
        val = int(float(value))
        return val if val > 0 else None
    except: return None


class ProgressReporter:
    """Maneja el reporte de progreso a la base de datos y logs."""
    
    def __init__(self, task_name: str, session_factory=None):
        self.task_name = task_name
        self.session_factory = session_factory
        self.current_progress = 0
        self.last_message = ""

    def update(self, progress: int, message: str, status: str = "running"):
        """Actualiza el estado en la base de datos."""
        self.current_progress = progress
        self.last_message = message
        logger.info(f"[{self.task_name}] {progress}% - {message}")
        
        if self.session_factory:
            from db.models import SystemStatus
            from sqlalchemy.sql import func
            session = self.session_factory()
            try:
                task = session.query(SystemStatus).filter_by(task_name=self.task_name).first()
                if not task:
                    task = SystemStatus(task_name=self.task_name)
                    session.add(task)
                
                task.status = status
                task.progress = progress
                task.message = message
                task.last_run = datetime.now()
                session.commit()
            except Exception as e:
                logger.error(f"Error actualizando SystemStatus: {e}")
                session.rollback()
            finally:
                session.close()

    def complete(self, message: str = "Tarea completada con éxito"):
        self.update(100, message, status="completed")

    def fail(self, message: str):
        self.update(self.current_progress, f"ERROR: {message}", status="failed")


def get_max_workers(default_count: int) -> int:
    """Determina el número máximo de workers basado en el entorno."""
    import os
    # En Render, solemos tener la variable RENDER=true
    if os.getenv("RENDER") == "true" or os.getenv("WEB_MODE") == "true":
        from ingestion.config import WEB_AWARDS_WORKERS
        return WEB_AWARDS_WORKERS
    return default_count


def clear_memory():
    """Limpia memoria y recolecta basura."""
    import gc
    gc.collect()


def get_all_seasons(start_year: int = 1983) -> List[str]:
    """Genera una lista de todas las temporadas de la NBA desde start_year hasta hoy."""
    from datetime import datetime
    current_year = datetime.now().year
    # Si estamos en octubre o después, la temporada actual ya ha empezado (ej: 2024-25)
    if datetime.now().month >= 10:
        end_year = current_year
    else:
        end_year = current_year - 1
        
    seasons = []
    for year in range(start_year, end_year + 1):
        next_year = (year + 1) % 100
        seasons.append(f"{year}-{next_year:02d}")
    return seasons
