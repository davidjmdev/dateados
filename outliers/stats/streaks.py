"""Detector de rachas históricas de rendimiento excepcional.

Rastrea secuencias consecutivas de partidos donde un jugador cumple criterios
específicos (pts>=20, triple-dobles, etc.). Cuando una racha supera umbrales
históricos notables, se marca como outlier histórico.

Tipos de racha soportados:
- pts_20, pts_30, pts_40: Partidos consecutivos con X+ puntos
- triple_double: Triple-dobles consecutivos
- reb_10, ast_10: Partidos consecutivos con 10+ rebotes/asistencias
- fg_pct_60: Partidos consecutivos con 60%+ FG (min 5 FGA)

Se soportan 3 contextos competitivos: Temporada Regular, Playoffs y NBA Cup.
Utiliza los flags 'rs', 'po' e 'ist' de la tabla 'games' para la clasificación.
"""

import logging
from typing import List, Optional, Dict, Callable
from datetime import date

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from db.models import PlayerGameStats, Game, Player
from outliers.base import BaseDetector, OutlierResult
from outliers.models import StreakRecord, StreakAllTimeRecord, STREAK_HISTORICAL_PERCENTAGE

logger = logging.getLogger(__name__)

# Umbral mínimo de minutos para considerar un partido
MIN_MINUTES_THRESHOLD = 15.0

# Mínimo de FGA para considerar el porcentaje de tiro
MIN_FGA_FOR_PERCENTAGE = 5


class StreakCriteria:
    """Define los criterios para cada tipo de racha.
    
    Retorna:
    - True: Criterio cumplido (extender racha).
    - False: Criterio no cumplido (romper racha).
    - None: Sin actividad en la categoría (congelar racha).
    """
    
    @staticmethod
    def pts_20(stats: PlayerGameStats) -> bool:
        """20+ puntos."""
        return (stats.pts or 0) >= 20
    
    @staticmethod
    def pts_30(stats: PlayerGameStats) -> bool:
        """30+ puntos."""
        return (stats.pts or 0) >= 30
    
    @staticmethod
    def pts_40(stats: PlayerGameStats) -> bool:
        """40+ puntos."""
        return (stats.pts or 0) >= 40
    
    @staticmethod
    def triple_double(stats: PlayerGameStats) -> bool:
        """Triple-doble (10+ en 3 categorías)."""
        counts = [
            (stats.pts or 0) >= 10,
            (stats.reb or 0) >= 10,
            (stats.ast or 0) >= 10,
            (stats.stl or 0) >= 10,
            (stats.blk or 0) >= 10,
        ]
        return sum(counts) >= 3
    
    @staticmethod
    def reb_10(stats: PlayerGameStats) -> bool:
        """10+ rebotes."""
        return (stats.reb or 0) >= 10
    
    @staticmethod
    def ast_10(stats: PlayerGameStats) -> bool:
        """10+ asistencias."""
        return (stats.ast or 0) >= 10
    
    @staticmethod
    def fg_pct_60(stats: PlayerGameStats) -> Optional[bool]:
        """60%+ FG (se congela si hay 0 intentos)."""
        fga = stats.fga or 0
        if fga == 0:
            return None
        return (stats.fg_pct or 0) >= 0.60
    
    @staticmethod
    def fg3_pct_50(stats: PlayerGameStats) -> Optional[bool]:
        """50%+ 3P (se congela si hay 0 intentos)."""
        fg3a = stats.fg3a or 0
        if fg3a == 0:
            return None
        return (stats.fg3_pct or 0) >= 0.50
    
    @staticmethod
    def ft_pct_90(stats: PlayerGameStats) -> Optional[bool]:
        """90%+ FT (se congela si hay 0 intentos)."""
        fta = stats.fta or 0
        if fta == 0:
            return None
        return (stats.ft_pct or 0) >= 0.90
    
    @classmethod
    def get_all_criteria(cls) -> Dict[str, Callable[[PlayerGameStats], Optional[bool]]]:
        """Retorna todos los criterios disponibles."""
        return {
            'pts_20': cls.pts_20,
            'pts_30': cls.pts_30,
            'pts_40': cls.pts_40,
            'triple_double': cls.triple_double,
            'reb_10': cls.reb_10,
            'ast_10': cls.ast_10,
            'fg_pct_60': cls.fg_pct_60,
            'fg3_pct_50': cls.fg3_pct_50,
            'ft_pct_90': cls.ft_pct_90,
        }


class StreakDetector(BaseDetector):
    """Detector de rachas históricas de rendimiento.
    
    Mantiene el estado de las rachas activas por jugador y las actualiza
    con cada partido procesado.
    """
    
    def __init__(self, streak_types: Optional[List[str]] = None):
        """Inicializa el detector.
        
        Args:
            streak_types: Lista de tipos de racha a rastrear.
        """
        all_criteria = StreakCriteria.get_all_criteria()
        
        if streak_types is None:
            self.streak_types = list(all_criteria.keys())
        else:
            invalid = set(streak_types) - set(all_criteria.keys())
            if invalid:
                raise ValueError(f"Tipos de racha inválidos: {invalid}")
            self.streak_types = streak_types
        
        self.criteria = {k: all_criteria[k] for k in self.streak_types}
    
    def detect(
        self,
        session: Session,
        game_stats: List[PlayerGameStats],
        active_only: bool = True,
        commit: bool = True
    ) -> List[OutlierResult]:
        """Detecta y actualiza rachas para una lista de estadísticas."""
        results = []
        
        active_ids = set()
        if active_only:
            active_ids = {p_id for p_id, in session.query(Player.id).filter(Player.is_active == True).all()}
        
        by_player: Dict[int, List[PlayerGameStats]] = {}
        for stats in game_stats:
            if active_only and stats.player_id not in active_ids:
                continue
                
            player_id = stats.player_id
            if player_id not in by_player:
                by_player[player_id] = []
            by_player[player_id].append(stats)
        
        for player_id, player_stats in by_player.items():
            player_stats_sorted = self._sort_by_date(session, player_stats)
            for stats in player_stats_sorted:
                player_results = self._process_game(session, stats, commit=commit)
                results.extend(player_results)
        
        return results
    
    def _sort_by_date(
        self,
        session: Session,
        stats_list: List[PlayerGameStats]
    ) -> List[PlayerGameStats]:
        """Ordena las estadísticas por fecha del partido."""
        game_ids = [s.game_id for s in stats_list]
        games = session.query(Game.id, Game.date).filter(Game.id.in_(game_ids)).all()
        game_dates = {g.id: g.date for g in games}
        return sorted(stats_list, key=lambda s: game_dates.get(s.game_id, date.min))
    
    def _process_game(
        self,
        session: Session,
        stats: PlayerGameStats,
        commit: bool = True
    ) -> List[OutlierResult]:
        """Procesa un partido para actualizar las rachas."""
        results = []
        
        minutes = self._get_minutes_float(stats)
        if minutes < 1.0:
            return results
        
        game = session.query(Game).filter(Game.id == stats.game_id).first()
        if not game:
            return results
        
        # Clasificación basada en flags de la base de datos
        comp_types = []
        if game.rs: comp_types.append('regular')
        if game.po: comp_types.append('playoffs')
        if game.ist: comp_types.append('nba_cup')
        
        if not comp_types:
            return results
        
        for streak_type, criterion in self.criteria.items():
            meets = criterion(stats)
            
            # Si meets es None, significa que no hubo intentos en esa categoría (congelar racha)
            if meets is None:
                continue
            
            # Si los minutos son bajos y NO cumplió el criterio, rompemos la racha
            # Si los minutos son bajos pero SÍ lo cumplió (ej: 20 pts en 10 min), la extendemos
            is_valid_extension = meets and minutes >= MIN_MINUTES_THRESHOLD
            is_extraordinary_extension = meets and minutes < MIN_MINUTES_THRESHOLD
            is_break = not meets # Cualquier partido jugado donde no se alcance el hito rompe la racha
            
            for ctype in comp_types:
                if is_valid_extension or is_extraordinary_extension:
                    result = self._extend_or_start_streak(
                        session, stats, game, streak_type, competition_type=ctype, commit=commit
                    )
                    if result:
                        results.append(result)
                elif is_break:
                    self._end_streak(session, stats.player_id, streak_type, game, competition_type=ctype, commit=commit)
        
        return results
    
    def _extend_or_start_streak(
        self,
        session: Session,
        stats: PlayerGameStats,
        game: Game,
        streak_type: str,
        competition_type: str = 'regular',
        commit: bool = True
    ) -> Optional[OutlierResult]:
        """Extiende o inicia una racha con lógica de blindaje anti-duplicados."""
        player_id = stats.player_id
        
        # 1. Intentar extender racha activa existente
        active_streak = session.query(StreakRecord).filter(
            and_(
                StreakRecord.player_id == player_id,
                StreakRecord.streak_type == streak_type,
                StreakRecord.competition_type == competition_type,
                StreakRecord.is_active == True
            )
        ).first()
        
        if active_streak:
            active_streak.length += 1
            active_streak.ended_at = game.date
            active_streak.last_game_id = game.id
            
            if commit:
                session.commit()
            
            self._check_and_update_all_time_record(session, active_streak, commit=commit)
            return self._verify_historical_status(session, active_streak, stats, commit)
            
        # 2. Si no hay activa, verificar si esta racha ya existe (evitar duplicado físico)
        # Esto ocurre si el proceso se reinicia y volvemos a procesar el mismo partido
        existing_event = session.query(StreakRecord).filter(
            and_(
                StreakRecord.player_id == player_id,
                StreakRecord.streak_type == streak_type,
                StreakRecord.competition_type == competition_type,
                StreakRecord.started_at == game.date
            )
        ).first()
        
        if existing_event:
            # Ya existe un registro que empezó hoy, no hacemos nada (idempotencia)
            return None
            
        # 3. Crear racha nueva
        new_streak = StreakRecord(
            player_id=player_id,
            streak_type=streak_type,
            competition_type=competition_type,
            length=1,
            is_active=True,
            is_historical_outlier=False,
            started_at=game.date,
            ended_at=None,
            first_game_id=game.id,
            last_game_id=None
        )
        session.add(new_streak)
        if commit:
            session.commit()
        
        return None

    def _verify_historical_status(self, session: Session, streak: StreakRecord, stats: PlayerGameStats, commit: bool) -> Optional[OutlierResult]:
        """Verifica si una racha ha alcanzado el estatus de HISTÓRICA."""
        if streak.is_historical_outlier:
            return None
            
        # Obtener el récord actual para esta categoría y competición
        record = session.query(StreakAllTimeRecord).filter(
            and_(
                StreakAllTimeRecord.streak_type == streak.streak_type,
                StreakAllTimeRecord.competition_type == streak.competition_type
            )
        ).first()
        
        # Usar récord actual o suelo de 2 si no hay registro
        all_time_length = record.length if record else 2
        threshold = max(2, int(all_time_length * STREAK_HISTORICAL_PERCENTAGE))
        
        if streak.length >= threshold:
            streak.is_historical_outlier = True
            if commit:
                session.commit()
            
            return OutlierResult(
                player_game_stat_id=stats.id,
                is_outlier=True,
                outlier_data={
                    'streak_type': streak.streak_type,
                    'competition_type': streak.competition_type,
                    'length': streak.length,
                    'threshold': threshold,
                    'started_at': str(streak.started_at),
                    'player_id': streak.player_id,
                    'streak_id': streak.id
                }
            )
        return None
    
    def _check_and_update_all_time_record(
        self,
        session: Session,
        streak: StreakRecord,
        commit: bool = True
    ) -> None:
        """Actualiza el récord histórico."""
        record = session.query(StreakAllTimeRecord).filter(
            and_(
                StreakAllTimeRecord.streak_type == streak.streak_type,
                StreakAllTimeRecord.competition_type == streak.competition_type
            )
        ).first()
        
        if not record or streak.length > record.length:
            if not record:
                record = StreakAllTimeRecord(
                    streak_type=streak.streak_type,
                    competition_type=streak.competition_type
                )
                session.add(record)
            
            record.player_id = streak.player_id
            record.length = streak.length
            record.started_at = streak.started_at
            record.ended_at = streak.ended_at
            record.game_id_start = streak.first_game_id
            record.game_id_end = streak.last_game_id
            
            if commit:
                session.commit()
            logger.info(f"¡NUEVO RÉCORD ({streak.competition_type})! {streak.streak_type}: {streak.length}")

    def _end_streak(
        self,
        session: Session,
        player_id: int,
        streak_type: str,
        game: Game,
        competition_type: str = 'regular',
        commit: bool = True
    ) -> None:
        """Finaliza una racha activa."""
        active_streak = session.query(StreakRecord).filter(
            and_(
                StreakRecord.player_id == player_id,
                StreakRecord.streak_type == streak_type,
                StreakRecord.competition_type == competition_type,
                StreakRecord.is_active == True
            )
        ).first()
        
        if active_streak:
            active_streak.is_active = False
            active_streak.ended_at = game.date
            active_streak.last_game_id = game.id
            if commit:
                session.commit()
    
    def _end_all_streaks(
        self,
        session: Session,
        player_id: int,
        stats: PlayerGameStats,
        competition_type: Optional[str] = None,
        commit: bool = True
    ) -> None:
        """Finaliza todas las rachas activas."""
        game = session.query(Game).filter(Game.id == stats.game_id).first()
        if not game:
            return
        
        filters = [StreakRecord.player_id == player_id, StreakRecord.is_active == True]
        if competition_type:
            filters.append(StreakRecord.competition_type == competition_type)
            
        active_streaks = session.query(StreakRecord).filter(and_(*filters)).all()
        for streak in active_streaks:
            streak.is_active = False
            streak.ended_at = game.date
            streak.last_game_id = game.id
        
        if active_streaks and commit:
            session.commit()
    
    def backfill(
        self,
        session: Session,
        season: Optional[str] = None
    ) -> int:
        """Procesa datos históricos de forma optimizada."""
        logger.info(f"Iniciando backfill multi-competición: {season or 'todas'}")
        
        if season is None:
            session.query(StreakRecord).delete()
            session.query(StreakAllTimeRecord).delete()
            session.commit()
        else:
            self._clear_season_streaks(session, season)

        players = session.query(Player.id, Player.full_name, Player.is_active).all()
        total_players = len(players)
        notable_count = 0
        
        competition_types = ['regular', 'playoffs', 'nba_cup']

        for idx, (player_id, player_name, is_active_player) in enumerate(players):
            if idx % 500 == 0 and idx > 0:
                logger.info(f"Procesando {idx}/{total_players}...")
                session.commit()

            # Consulta usando flags reales de la base de datos
            stats_query = session.query(
                PlayerGameStats.id, PlayerGameStats.pts, PlayerGameStats.reb, 
                PlayerGameStats.ast, PlayerGameStats.stl, PlayerGameStats.blk, 
                PlayerGameStats.fga, PlayerGameStats.fg_pct, 
                PlayerGameStats.fg3a, PlayerGameStats.fg3_pct,
                PlayerGameStats.fta, PlayerGameStats.ft_pct,
                PlayerGameStats.min,
                Game.date, Game.id.label('game_id'), Game.rs, Game.ist, Game.po
            ).join(Game).filter(PlayerGameStats.player_id == player_id)
            
            if season:
                stats_query = stats_query.filter(Game.season == season)
            
            stats_list = stats_query.order_by(Game.date).all()
            if not stats_list:
                continue

            # Para saber si una racha sigue activa, necesitamos el último partido de esa competición para el jugador
            last_game_ids_by_comp = {
                'regular': next((s.game_id for s in reversed(stats_list) if s.rs), None),
                'playoffs': next((s.game_id for s in reversed(stats_list) if s.po), None),
                'nba_cup': next((s.game_id for s in reversed(stats_list) if s.ist), None)
            }
            
            active_trackers = {ct: {st: None for st in self.streak_types} for ct in competition_types}
            
            for row in stats_list:
                mins = self._get_minutes_float(row)
                if mins < 1.0:
                    continue
                
                # Clasificar usando flags de la BD
                row_comp_types = []
                if row.rs: row_comp_types.append('regular')
                if row.po: row_comp_types.append('playoffs')
                if row.ist: row_comp_types.append('nba_cup')
                
                if not row_comp_types:
                    continue
                
                for stype in self.streak_types:
                    meets = self.criteria[stype](row)
                    
                    if meets is None:
                        continue
                    
                    for ctype in row_comp_types:
                        tracker = active_trackers[ctype][stype]
                        if meets:
                            if tracker is None:
                                active_trackers[ctype][stype] = {
                                    'len': 1, 'start': row.date, 'start_id': row.game_id, 
                                    'last': row.date, 'last_id': row.game_id
                                }
                            else:
                                tracker['len'] += 1
                                tracker['last'] = row.date
                                tracker['last_id'] = row.game_id
                        else:
                            if tracker is not None:
                                # Guardar racha finalizada (is_historical_outlier se actualizará al final)
                                streak_obj = StreakRecord(
                                    player_id=player_id, streak_type=stype, 
                                    competition_type=ctype, length=tracker['len'],
                                    is_active=False, is_historical_outlier=False,
                                    started_at=tracker['start'], 
                                    ended_at=row.date, 
                                    first_game_id=tracker['start_id'], 
                                    last_game_id=tracker['last_id']
                                )
                                session.add(streak_obj)
                                self._check_and_update_all_time_record(session, streak_obj, commit=False)
                                active_trackers[ctype][stype] = None

            # Manejar rachas activas al final del historial
            for ctype in competition_types:
                for stype in self.streak_types:
                    tracker = active_trackers[ctype][stype]
                    if tracker is not None:
                        # Una racha es activa si el jugador sigue activo Y terminó en el último partido de esa competición
                        is_active_streak = (is_active_player and tracker['last_id'] == last_game_ids_by_comp[ctype])
                        
                        streak_obj = StreakRecord(
                            player_id=player_id, streak_type=stype, 
                            competition_type=ctype, length=tracker['len'],
                            is_active=is_active_streak, is_historical_outlier=False,
                            started_at=tracker['start'], 
                            ended_at=tracker['last'] if not is_active_streak else None,
                            first_game_id=tracker['start_id'], last_game_id=tracker['last_id']
                        )
                        session.add(streak_obj)
                        self._check_and_update_all_time_record(session, streak_obj, commit=False)

        session.commit()
        
        # Paso final: Actualizar is_historical_outlier dinámicamente para todas las rachas
        logger.info("Actualizando distintivos de 'HISTÓRICA' basados en los récords finales...")
        self._update_historical_badges(session)
        
        return 0
    
    def _update_historical_badges(self, session: Session) -> None:
        """Actualiza el flag is_historical_outlier basado en el 70% del récord actual."""
        from sqlalchemy import update
        
        records = session.query(StreakAllTimeRecord).all()
        for r in records:
            threshold = max(2, int(r.length * STREAK_HISTORICAL_PERCENTAGE))
            session.execute(
                update(StreakRecord)
                .where(and_(
                    StreakRecord.streak_type == r.streak_type,
                    StreakRecord.competition_type == r.competition_type,
                    StreakRecord.length >= threshold
                ))
                .values(is_historical_outlier=True)
            )
        session.commit()
    
    def _clear_season_streaks(self, session: Session, season: str) -> None:
        """Elimina rachas de una temporada específica."""
        dates = session.query(Game.date).filter(Game.season == season).all()
        if not dates: return
        min_date, max_date = min(d[0] for d in dates), max(d[0] for d in dates)
        session.query(StreakRecord).filter(and_(StreakRecord.started_at >= min_date, StreakRecord.started_at <= max_date)).delete(synchronize_session=False)
        session.commit()
    
    def get_active_streaks(self, session: Session, player_id: Optional[int] = None) -> List[StreakRecord]:
        """Obtiene las rachas activas."""
        query = session.query(StreakRecord).filter(StreakRecord.is_active == True)
        if player_id: query = query.filter(StreakRecord.player_id == player_id)
        return query.order_by(StreakRecord.length.desc()).all()
    
    def get_historical_streaks(self, session: Session, streak_type: Optional[str] = None, limit: int = 20) -> List[StreakRecord]:
        """Obtiene las rachas históricas más largas."""
        query = session.query(StreakRecord)
        if streak_type: query = query.filter(StreakRecord.streak_type == streak_type)
        return query.order_by(StreakRecord.length.desc()).limit(limit).all()


def get_streak_summary(session: Session, competition_type: str = 'regular') -> Dict[str, Dict]:
    """Obtiene un resumen de las rachas."""
    from sqlalchemy import func, Integer
    summary = {}
    
    # Obtener récords para calcular umbrales dinámicos en el resumen
    records = {r.streak_type: r.length for r in session.query(StreakAllTimeRecord).filter(
        StreakAllTimeRecord.competition_type == competition_type
    ).all()}
    
    streak_types = StreakCriteria.get_all_criteria().keys()
    
    for streak_type in streak_types:
        stats = session.query(
            func.count(StreakRecord.id).label('total'),
            func.max(StreakRecord.length).label('max_length'),
            func.avg(StreakRecord.length).label('avg_length'),
            func.sum(func.cast(StreakRecord.is_historical_outlier, Integer)).label('notable_count')
        ).filter(and_(StreakRecord.streak_type == streak_type, StreakRecord.competition_type == competition_type)).first()
        
        all_time = records.get(streak_type, 2)
        
        summary[streak_type] = {
            'total_streaks': stats.total or 0,
            'max_length': stats.max_length or 0,
            'avg_length': round(stats.avg_length or 0, 1),
            'notable_count': stats.notable_count or 0,
            'notable_threshold': max(2, int(all_time * STREAK_HISTORICAL_PERCENTAGE))
        }
    return summary


def _get_minutes_float(row: any) -> float:
    """Helper para obtener minutos como float."""
    m = getattr(row, 'min', None)
    if m is None: return 0.0
    from datetime import timedelta
    if isinstance(m, timedelta): return m.total_seconds() / 60.0
    try:
        s = str(m)
        if ':' in s:
            parts = s.split(':')
            return float(parts[0]) + float(parts[1])/60.0
        return float(s)
    except: return 0.0
