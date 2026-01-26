import logging
import time
import gc
from datetime import date, datetime, timedelta
from typing import Optional, Dict, Any, List, Set, Tuple
from dateutil import parser as date_parser

from ingestion.config import (
    API_DELAY, API_TIMEOUT, MAX_WORKERS_LOCAL, MAX_WORKERS_CLOUD
)
from db.logging import PROGRESS_LOG_EVERY_N_SECONDS

logger = logging.getLogger("dateados.ingestion.utils")


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
    """Maneja el reporte de progreso con métricas temporales y actualizaciones inteligentes."""
    
    def __init__(self, task_name: str, session_factory=None):
        self.task_name = task_name
        self.session_factory = session_factory
        self.current_progress = 0
        self.last_message = ""
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.last_log_time = self.start_time
        self.items_processed = 0
        self.total_items = 0
    
    def set_total(self, total: int):
        """Define el total de items a procesar para cálculos automáticos."""
        self.total_items = total
    
    def increment(self, message: str = "", delta: int = 1):
        """Incrementa el contador de items procesados y actualiza progreso automáticamente."""
        self.items_processed += delta
        
        if self.total_items > 0:
            progress = min(100, int((self.items_processed / self.total_items) * 100))
        else:
            progress = self.current_progress
        
        elapsed = time.time() - self.start_time
        metrics_msg = self._build_metrics_message(message, elapsed)
        
        self.update(progress, metrics_msg)
    
    def _build_metrics_message(self, base_message: str, elapsed: float) -> str:
        """Construye un mensaje con métricas de rendimiento."""
        msg_parts = []
        if base_message: msg_parts.append(base_message)
        elif self.total_items > 0: msg_parts.append(f"{self.items_processed}/{self.total_items}")
        
        if self.items_processed > 0 and elapsed > 1:
            rate = self.items_processed / elapsed
            msg_parts.append(f"{rate:.1f} items/s" if rate >= 1 else f"{rate:.2f} items/s")
        
        if self.total_items > 0 and self.items_processed > 0 and elapsed > 10:
            remaining = self.total_items - self.items_processed
            if remaining > 0:
                rate = self.items_processed / elapsed
                if rate > 0:
                    eta_seconds = remaining / rate
                    msg_parts.append(f"ETA: ~{int(eta_seconds/60)}min" if eta_seconds > 60 else f"ETA: ~{int(eta_seconds)}s")
        
        return " | ".join(msg_parts) if msg_parts else "En progreso..."

    def update(self, progress: Optional[int], message: str, status: str = "running"):
        """Actualiza el estado en la base de datos."""
        if progress is not None:
            self.current_progress = progress
        
        display_progress = self.current_progress
        self.last_message = message
        
        now = time.time()
        should_log = (now - self.last_log_time) >= PROGRESS_LOG_EVERY_N_SECONDS
        
        if should_log or status != "running":
            elapsed = int(now - self.start_time)
            elapsed_str = f"{elapsed//60}m{elapsed%60}s" if elapsed > 60 else f"{elapsed}s"
            # DEBUG para no duplicar en consola con los logs INFO de la estrategia
            logger.debug(f"[{self.task_name}] {display_progress}% - {message} ({elapsed_str})")
            self.last_log_time = now
        
        if self.session_factory:
            from db.models import SystemStatus
            session = self.session_factory()
            try:
                task = session.query(SystemStatus).filter_by(task_name=self.task_name).first()
                if not task:
                    task = SystemStatus(task_name=self.task_name)
                    session.add(task)
                
                task.status = status
                task.progress = display_progress
                task.message = message
                if task.last_run is None: task.last_run = datetime.now()
                session.commit()
                self.last_update_time = now
            except Exception: session.rollback()
            finally: session.close()

    def complete(self, message: str = "Tarea completada con éxito"):
        """Marca la tarea como completada."""
        elapsed = int(time.time() - self.start_time)
        elapsed_str = f" en {elapsed//60}m{elapsed%60}s" if elapsed > 60 else f" en {elapsed}s"
        self.update(100, message + elapsed_str, status="completed")

    def fail(self, message: str):
        """Marca la tarea como fallida."""
        self.update(self.current_progress, f"ERROR: {message}", status="failed")


def get_max_workers(requested_count: Optional[int] = None) -> int:
    """Determina el número máximo de workers basado en el entorno."""
    import os
    if os.getenv("RENDER") == "true" or os.getenv("WEB_MODE") == "true" or os.getenv("CLOUD_MODE") == "true":
        return MAX_WORKERS_CLOUD
    return requested_count if requested_count is not None else MAX_WORKERS_LOCAL


def clear_memory():
    """Limpia memoria y recolecta basura."""
    gc.collect()


def get_all_seasons(start_year: int = 1983) -> List[str]:
    """Genera una lista de todas las temporadas de la NBA."""
    current_year = datetime.now().year
    end_year = current_year if datetime.now().month >= 10 else current_year - 1
    seasons = []
    for year in range(start_year, end_year + 1):
        seasons.append(f"{year}-{(year + 1) % 100:02d}")
    return seasons
