"""Generación de tablas derivadas desde boxscores.

Este módulo maneja la generación y actualización de tablas agregadas:
- PlayerTeamSeason: Resúmenes de jugadores por equipo/temporada/tipo
- TeamGameStats: Estadísticas de equipo por partido
- Sincronización de marcadores faltantes
- Actualización de campeones
"""

import logging
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_

from db.models import (
    PlayerGameStats, PlayerTeamSeason, TeamGameStats, Game
)
from ingestion.models_sync import update_champions
from ingestion.utils import safe_int, safe_float

logger = logging.getLogger(__name__)


class DerivedTablesGenerator:
    """Maneja la generación de tablas estadísticas derivadas."""
    
    def __init__(self):
        pass
    
    def regenerate_for_seasons(self, session: Session, seasons: List[str]):

        """Regenera tablas derivadas solo para temporadas especificadas.
        
        Este método es más eficiente que regenerate_all() porque solo
        recalcula las temporadas que han sido afectadas por nuevos partidos.
        
        Args:
            session: Sesión de SQLAlchemy
            seasons: Lista de temporadas a regenerar (ej: ["2023-24", "2024-25"])
        """
        if not seasons:
            logger.warning("No se especificaron temporadas para regenerar")
            return
        
        logger.info(f"Regenerando tablas derivadas para {len(seasons)} temporadas: {', '.join(seasons)}")
        
        for season in seasons:
            self._regenerate_season(session, season)
        
        logger.info(f"Regeneración completada para {len(seasons)} temporadas")
    
    def regenerate_all(self, session: Session):
        """Regenera todas las tablas derivadas.
        
        Este método elimina y recalcula todas las tablas derivadas.
        Usar solo cuando sea absolutamente necesario (ej: cambios de esquema).
        
        Args:
            session: Sesión de SQLAlchemy
        """
        logger.info("Regenerando TODAS las tablas derivadas...")
        self._regenerate_season(session, season=None)
        logger.info("Regeneración completa finalizada")
    
    def _regenerate_season(self, session: Session, season: Optional[str] = None):
        """Regenera tablas derivadas para una temporada o todas.
        
        Args:
            session: Sesión de SQLAlchemy
            season: Temporada específica o None para todas
        """
        # 1. Borrar registros existentes para evitar datos huérfanos/obsoletos
        if season:
            logger.info(f"Limpiando tablas derivadas de la temporada {season}...")
            # Borrar PlayerTeamSeason
            session.query(PlayerTeamSeason).filter(PlayerTeamSeason.season == season).delete(synchronize_session=False)
            
            # Borrar TeamGameStats (esto ya se hacía dentro de su método, pero centralizamos)
            gids = [gid for (gid,) in session.query(Game.id).filter(Game.season == season).all()]
            if gids:
                session.query(TeamGameStats).filter(TeamGameStats.game_id.in_(gids)).delete(synchronize_session=False)
        else:
            logger.info("Limpiando TODAS las tablas derivadas...")
            session.query(PlayerTeamSeason).delete(synchronize_session=False)
            session.query(TeamGameStats).delete(synchronize_session=False)
        
        session.commit()

        # 2. PlayerTeamSeason
        pts_count = self._regenerate_player_team_seasons(session, season)
        
        # 3. TeamGameStats
        tgs_count = self._regenerate_team_game_stats(session, season, skip_delete=True)
        
        # 4. Sincronizar marcadores faltantes o discrepantes
        scores_synced = self._sync_missing_scores(session, season)
        
        # 6. Actualizar campeones
        if season:
            update_champions(session, season)
        
        season_str = season if season else "TODAS"
        logger.info(
            f"Temporada {season_str}: "
            f"{pts_count} PlayerTeamSeason, "
            f"{tgs_count} TeamGameStats, "
            f"{scores_synced} marcadores"
        )

    def _regenerate_player_team_seasons(self, session: Session, season: Optional[str] = None) -> int:
        """Regenera tabla PlayerTeamSeason.
        
        Args:
            session: Sesión de SQLAlchemy
            season: Temporada específica o None para todas
            
        Returns:
            Número de registros creados/actualizados
        """
        types = ['Regular Season', 'Playoffs', 'Play-In', 'NBA Cup']
        pts_count = 0
        
        for t_name in types:
            # Definir filtro de tipo
            if t_name == 'Regular Season':
                type_filter = Game.rs == True
            elif t_name == 'Playoffs':
                type_filter = Game.po == True
            elif t_name == 'Play-In':
                type_filter = Game.pi == True
            else:  # NBA Cup
                type_filter = Game.ist == True
            
            # Query para agrupar estadísticas
            query = session.query(
                PlayerGameStats.player_id,
                PlayerGameStats.team_id,
                Game.season,
                func.min(Game.date).label('start_date'),
                func.max(Game.date).label('end_date'),
                func.count(PlayerGameStats.id).label('games_played'),
                func.sum(PlayerGameStats.pts).label('pts'),
                func.sum(PlayerGameStats.reb).label('reb'),
                func.sum(PlayerGameStats.ast).label('ast'),
                func.sum(PlayerGameStats.stl).label('stl'),
                func.sum(PlayerGameStats.blk).label('blk'),
                func.sum(PlayerGameStats.tov).label('tov'),
                func.sum(PlayerGameStats.pf).label('pf'),
                func.sum(PlayerGameStats.fgm).label('fgm'),
                func.sum(PlayerGameStats.fga).label('fga'),
                func.sum(PlayerGameStats.fg3m).label('fg3m'),
                func.sum(PlayerGameStats.fg3a).label('fg3a'),
                func.sum(PlayerGameStats.ftm).label('ftm'),
                func.sum(PlayerGameStats.fta).label('fta'),
                func.sum(PlayerGameStats.plus_minus).label('plus_minus'),
                func.sum(PlayerGameStats.min).label('total_min')
            ).join(
                Game, PlayerGameStats.game_id == Game.id
            ).filter(type_filter)
            
            if season:
                query = query.filter(Game.season == season)
            
            query = query.group_by(
                PlayerGameStats.player_id,
                PlayerGameStats.team_id,
                Game.season
            )
            
            for row in query.all():
                pid, tid, sn = int(row.player_id), int(row.team_id), str(row.season)
                
                # Buscar registro existente
                pts = session.query(PlayerTeamSeason).filter(
                    and_(
                        PlayerTeamSeason.player_id == pid,
                        PlayerTeamSeason.team_id == tid,
                        PlayerTeamSeason.season == sn,
                        PlayerTeamSeason.type == t_name
                    )
                ).first()
                
                if not pts:
                    pts = PlayerTeamSeason(
                        player_id=pid,
                        team_id=tid,
                        season=sn,
                        type=t_name
                    )
                    session.add(pts)
                
                # Actualizar datos
                pts.start_date = row.start_date
                pts.end_date = row.end_date
                pts.games_played = safe_int(row.games_played)
                pts.pts = safe_int(row.pts)
                pts.reb = safe_int(row.reb)
                pts.ast = safe_int(row.ast)
                pts.stl = safe_int(row.stl)
                pts.blk = safe_int(row.blk)
                pts.tov = safe_int(row.tov)
                pts.pf = safe_int(row.pf)
                pts.fgm = safe_int(row.fgm)
                pts.fga = safe_int(row.fga)
                pts.fg3m = safe_int(row.fg3m)
                pts.fg3a = safe_int(row.fg3a)
                pts.ftm = safe_int(row.ftm)
                pts.fta = safe_int(row.fta)
                pts.plus_minus = safe_float(row.plus_minus)
                pts.minutes = row.total_min
                pts.is_detailed = True  # Viene de boxscores locales
                
                pts_count += 1
        
        session.commit()
        return pts_count
    
    def _regenerate_team_game_stats(self, session: Session, season: Optional[str] = None, skip_delete: bool = False) -> int:
        """Regenera tabla TeamGameStats.
        
        Args:
            session: Sesión de SQLAlchemy
            season: Temporada específica o None para todas
            skip_delete: Si True, asume que ya se borraron los registros
            
        Returns:
            Número de registros creados
        """
        # Borrar registros existentes de la temporada si no se saltó
        if not skip_delete:
            if season:
                gids = [gid for (gid,) in session.query(Game.id).filter(Game.season == season).all()]
                if gids:
                    session.query(TeamGameStats).filter(
                        TeamGameStats.game_id.in_(gids)
                    ).delete(synchronize_session=False)
            else:
                session.query(TeamGameStats).delete()
        
        # Query para agrupar por equipo/partido
        t_query = session.query(
            PlayerGameStats.game_id,
            PlayerGameStats.team_id,
            func.sum(PlayerGameStats.pts).label('pts'),
            func.sum(PlayerGameStats.reb).label('reb'),
            func.sum(PlayerGameStats.ast).label('ast'),
            func.sum(PlayerGameStats.stl).label('stl'),
            func.sum(PlayerGameStats.blk).label('blk'),
            func.sum(PlayerGameStats.tov).label('tov'),
            func.sum(PlayerGameStats.pf).label('pf'),
            func.avg(PlayerGameStats.plus_minus).label('pm'),
            func.sum(PlayerGameStats.fgm).label('fgm'),
            func.sum(PlayerGameStats.fga).label('fga'),
            func.sum(PlayerGameStats.fg3m).label('fg3m'),
            func.sum(PlayerGameStats.fg3a).label('fg3a'),
            func.sum(PlayerGameStats.ftm).label('ftm'),
            func.sum(PlayerGameStats.fta).label('fta')
        ).join(Game, PlayerGameStats.game_id == Game.id)
        
        if season:
            t_query = t_query.filter(Game.season == season)
        
        t_query = t_query.group_by(PlayerGameStats.game_id, PlayerGameStats.team_id)
        
        tgs_count = 0
        for r in t_query.all():
            tgs = TeamGameStats(
                game_id=r.game_id,
                team_id=r.team_id,
                total_pts=int(r.pts),
                total_reb=int(r.reb),
                total_ast=int(r.ast),
                total_stl=int(r.stl),
                total_blk=int(r.blk),
                total_tov=int(r.tov),
                total_pf=int(r.pf),
                avg_plus_minus=float(r.pm) if r.pm else None,
                total_fgm=int(r.fgm),
                total_fga=int(r.fga),
                fg_pct=r.fgm / r.fga if r.fga > 0 else 0,
                total_fg3m=int(r.fg3m),
                total_fg3a=int(r.fg3a),
                fg3_pct=r.fg3m / r.fg3a if r.fg3a > 0 else 0,
                total_ftm=int(r.ftm),
                total_fta=int(r.fta),
                ft_pct=r.ftm / r.fta if r.fta > 0 else 0
            )
            session.add(tgs)
            tgs_count += 1
        
        session.commit()
        return tgs_count
    
    def _sync_missing_scores(self, session: Session, season: Optional[str] = None) -> int:
        """Sincroniza marcadores faltantes desde TeamGameStats.
        
        Args:
            session: Sesión de SQLAlchemy
            season: Temporada específica o None para todas
            
        Returns:
            Número de partidos actualizados
        """
        # Buscar partidos con marcador 0 o None
        games = session.query(Game).filter(
            or_(
                Game.home_score == 0, 
                Game.home_score.is_(None),
                Game.away_score == 0,
                Game.away_score.is_(None)
            )
        )
        
        if season:
            games = games.filter(Game.season == season)
        
        scores_synced = 0
        for g in games.all():
            # Obtener estadísticas de equipo calculadas desde jugadores
            stats = session.query(TeamGameStats).filter(
                TeamGameStats.game_id == g.id
            ).all()
            
            # Solo sincronizar si tenemos datos de ambos equipos
            if len(stats) >= 2:
                for s in stats:
                    if s.team_id == g.home_team_id:
                        g.home_score = s.total_pts
                    elif s.team_id == g.away_team_id:
                        g.away_score = s.total_pts
                
                # Recalcular ganador tras sincronizar
                if g.home_score is not None and g.away_score is not None:
                    if g.home_score > g.away_score:
                        g.winner_team_id = g.home_team_id
                    elif g.away_score > g.home_score:
                        g.winner_team_id = g.away_team_id
                    else:
                        g.winner_team_id = None
                    
                    scores_synced += 1
        
        session.commit()
        return scores_synced
