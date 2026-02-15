"""Herramientas MCP para consultas de outliers y rachas NBA.

Tools:
- get_league_outliers: Anomalías detectadas por autoencoder (vs. liga)
- get_player_outliers: Anomalías por Z-score (vs. media del jugador)
- get_active_streaks: Rachas activas y recientemente rotas
"""

from typing import Optional

from mcp.server.fastmcp import FastMCP
from sqlalchemy import func, and_, or_, case, cast, Float
from sqlalchemy.orm import Session
from datetime import timedelta

from db.connection import get_session
from db.models import Game, Player, PlayerGameStats
from outliers.models import (
    LeagueOutlier,
    PlayerOutlier,
    PlayerTrendOutlier,
    StreakRecord,
    StreakAllTimeRecord,
)
from mcp_server.serializers import to_json, round_floats


def register_outlier_tools(mcp: FastMCP) -> None:
    """Registra las herramientas de outliers y rachas en el servidor MCP."""

    @mcp.tool()
    def get_league_outliers(
        window: str = "week",
        limit: int = 20,
    ) -> str:
        """Obtiene las actuaciones más anómalas comparadas con toda la liga.

        Usa un autoencoder para detectar líneas estadísticas que se desvían
        significativamente del patrón histórico de la NBA. Solo incluye
        jugadores activos.

        Args:
            window: Ventana temporal:
                    "last_game" = último día con partidos
                    "week" = últimos 7 días (default)
                    "month" = últimos 30 días
            limit: Número máximo de resultados (default: 20, max: 100)

        Returns:
            JSON con outliers de liga ordenados por percentil descendente.
            Cada outlier incluye: jugador, partido, pts/reb/ast, percentil,
            error de reconstrucción y features más anómalas.
        """
        limit = min(limit, 100)
        session = get_session()
        try:
            query = (
                session.query(LeagueOutlier, PlayerGameStats, Player, Game)
                .join(PlayerGameStats, LeagueOutlier.player_game_stat_id == PlayerGameStats.id)
                .join(Player, PlayerGameStats.player_id == Player.id)
                .join(Game, PlayerGameStats.game_id == Game.id)
                .filter(LeagueOutlier.is_outlier == True)
                .filter(Player.is_active == True)
            )

            latest_date = session.query(func.max(Game.date)).scalar()
            if latest_date:
                if window == 'last_game':
                    query = query.filter(Game.date == latest_date)
                elif window == 'week':
                    query = query.filter(Game.date >= latest_date - timedelta(days=7))
                elif window == 'month':
                    query = query.filter(Game.date >= latest_date - timedelta(days=30))

            query = query.order_by(LeagueOutlier.percentile.desc())

            results = []
            for outlier, stats_row, player, game in query.limit(limit).all():
                top_features = []
                if outlier.feature_contributions:
                    sorted_features = sorted(
                        outlier.feature_contributions.items(),
                        key=lambda x: x[1],
                        reverse=True,
                    )[:3]
                    top_features = [f[0] for f in sorted_features]

                results.append({
                    'player_id': player.id,
                    'player_name': player.full_name,
                    'game_id': game.id,
                    'game_date': game.date.isoformat() if game.date else None,
                    'season': game.season,
                    'pts': stats_row.pts,
                    'reb': stats_row.reb,
                    'ast': stats_row.ast,
                    'percentile': round(outlier.percentile, 1),
                    'reconstruction_error': round(outlier.reconstruction_error, 4),
                    'top_features': top_features,
                })

            return to_json({"count": len(results), "window": window, "outliers": results})
        finally:
            session.close()

    @mcp.tool()
    def get_player_outliers(
        window: str = "week",
        outlier_type: Optional[str] = None,
        limit: int = 20,
    ) -> str:
        """Obtiene actuaciones anómalas comparadas con la media histórica del propio jugador.

        Detecta "explosiones" (rendimiento muy por encima de su media) y
        "crisis" (rendimiento muy por debajo). Solo jugadores activos.

        Para window="last_game": usa Z-score por partido individual.
        Para window="week"/"month": usa tendencias agregadas en ventana temporal.

        Args:
            window: Ventana temporal:
                    "last_game" = último día con partidos (Z-score individual)
                    "week" = tendencia semanal (default)
                    "month" = tendencia mensual
            outlier_type: Filtrar por tipo: "explosion" o "crisis". None = ambos.
            limit: Número máximo de resultados (default: 20, max: 100)

        Returns:
            JSON con outliers de jugador ordenados por |Z-score| descendente.
            Incluye features anómalas con valor actual vs. media histórica.
        """
        limit = min(limit, 100)
        session = get_session()
        try:
            results = []

            if window == 'last_game':
                query = (
                    session.query(PlayerOutlier, PlayerGameStats, Player, Game)
                    .join(PlayerGameStats, PlayerOutlier.player_game_stat_id == PlayerGameStats.id)
                    .join(Player, PlayerGameStats.player_id == Player.id)
                    .join(Game, PlayerGameStats.game_id == Game.id)
                    .filter(Player.is_active == True)
                )
                latest_date = session.query(func.max(Game.date)).scalar()
                if latest_date:
                    query = query.filter(Game.date == latest_date)

                if outlier_type:
                    query = query.filter(PlayerOutlier.outlier_type == outlier_type)

                query = query.order_by(func.abs(PlayerOutlier.max_z_score).desc())

                for outlier, stats_row, player, game in query.limit(limit).all():
                    features = sorted(
                        outlier.outlier_features,
                        key=lambda x: abs(x['z_score']),
                        reverse=True,
                    )
                    primary = features[0] if features else None

                    results.append({
                        'player_id': player.id,
                        'player_name': player.full_name,
                        'game_id': game.id,
                        'game_date': game.date.isoformat() if game.date else None,
                        'pts': stats_row.pts,
                        'outlier_type': outlier.outlier_type,
                        'max_z_score': round(outlier.max_z_score, 2),
                        'primary_feature': primary['feature'] if primary else 'pts',
                        'primary_value': primary['val'] if primary else stats_row.pts,
                        'primary_avg': primary['avg'] if primary else 0,
                        'outlier_features': features[:5],
                        'window': 'game',
                    })
            else:
                # Tendencias (week/month)
                effective_window = window if window in ('week', 'month') else 'week'
                query = (
                    session.query(PlayerTrendOutlier, Player)
                    .join(Player, PlayerTrendOutlier.player_id == Player.id)
                    .filter(and_(
                        Player.is_active == True,
                        PlayerTrendOutlier.window_type == effective_window,
                    ))
                )

                latest_date = session.query(func.max(PlayerTrendOutlier.reference_date)).scalar()
                if latest_date:
                    query = query.filter(
                        PlayerTrendOutlier.reference_date >= latest_date - timedelta(days=7)
                    )

                if outlier_type:
                    query = query.filter(PlayerTrendOutlier.outlier_type == outlier_type)

                query = query.order_by(func.abs(PlayerTrendOutlier.max_z_score).desc())

                for trend, player in query.limit(limit).all():
                    sorted_feats = sorted(
                        trend.z_scores.items(),
                        key=lambda x: abs(x[1]),
                        reverse=True,
                    )
                    top_f = sorted_feats[0][0] if sorted_feats else None
                    comp_top = trend.comparison_data.get(top_f, {}) if top_f else {}

                    outlier_features = []
                    for f, z in sorted_feats[:5]:
                        comp = trend.comparison_data.get(f, {})
                        outlier_features.append({
                            'feature': f,
                            'z_score': round(z, 2),
                            'current_avg': comp.get('current_avg', 0),
                            'baseline_avg': comp.get('baseline_avg', 0),
                            'sentiment': comp.get('sentiment', 'positive' if z > 0 else 'negative'),
                        })

                    results.append({
                        'player_id': player.id,
                        'player_name': player.full_name,
                        'reference_date': trend.reference_date.isoformat(),
                        'outlier_type': trend.outlier_type,
                        'max_z_score': round(trend.max_z_score, 2),
                        'primary_feature': top_f,
                        'primary_value': comp_top.get('current_avg', 0),
                        'primary_avg': comp_top.get('baseline_avg', 0),
                        'outlier_features': outlier_features,
                        'window': effective_window,
                    })

            return to_json({"count": len(results), "window": window, "outliers": round_floats(results)})
        finally:
            session.close()

    @mcp.tool()
    def get_active_streaks(
        competition_type: str = "regular",
        streak_type: Optional[str] = None,
        include_broken: bool = True,
        limit: int = 30,
    ) -> str:
        """Obtiene las rachas activas y recientemente terminadas de la NBA.

        Una racha es una secuencia consecutiva de partidos cumpliendo un criterio
        (ej: 20+ puntos, triple-doble, 60%+ FG). Incluye contexto del récord
        histórico (all-time) para cada tipo de racha.

        Args:
            competition_type: Tipo de competición:
                              "regular" = Regular Season (default)
                              "playoffs" = Playoffs
                              "nba_cup" = NBA Cup
            streak_type: Filtrar por tipo específico. Opciones:
                         "pts_20", "pts_30", "pts_40", "triple_double",
                         "reb_10", "ast_10", "fg_pct_60", "fg3_pct_50", "ft_pct_90".
                         None = todos los tipos.
            include_broken: Si True (default), incluye rachas rotas en los
                            últimos 7 días.
            limit: Máximo de resultados (default: 30, max: 100)

        Returns:
            JSON con rachas activas, rachas rotas, y récords all-time.
            Cada racha incluye: jugador, tipo, longitud, fecha inicio,
            progreso vs. récord histórico.
        """
        limit = min(limit, 100)
        session = get_session()
        try:
            shooting_types = ['fg_pct_60', 'fg3_pct_50', 'ft_pct_90']

            # 1. All-time records para contexto
            records_query = (
                session.query(StreakAllTimeRecord, Player.full_name)
                .join(Player, StreakAllTimeRecord.player_id == Player.id)
                .filter(StreakAllTimeRecord.competition_type == competition_type)
            )
            all_time_records = {}
            for rec, player_name in records_query.all():
                all_time_records[rec.streak_type] = {
                    'length': rec.length,
                    'player_name': player_name,
                    'player_id': rec.player_id,
                    'started_at': rec.started_at.isoformat() if rec.started_at else None,
                }

            # 2. Active streaks (con filtro de relevancia SQL)
            active_query = (
                session.query(StreakRecord, Player)
                .join(Player, StreakRecord.player_id == Player.id)
                .outerjoin(StreakAllTimeRecord, and_(
                    StreakRecord.streak_type == StreakAllTimeRecord.streak_type,
                    StreakRecord.competition_type == StreakAllTimeRecord.competition_type,
                ))
                .filter(and_(
                    StreakRecord.is_active == True,
                    Player.is_active == True,
                    StreakRecord.competition_type == competition_type,
                ))
                .filter(case(
                    (StreakRecord.streak_type.in_(shooting_types), StreakRecord.length >= 3),
                    else_=StreakRecord.length >= func.greatest(
                        2, cast(func.coalesce(StreakAllTimeRecord.length, 2), Float) * 0.05
                    ),
                ))
            )
            if streak_type:
                active_query = active_query.filter(StreakRecord.streak_type == streak_type)

            active_query = active_query.order_by(StreakRecord.length.desc())

            active_results = []
            for streak, player in active_query.limit(limit).all():
                record = all_time_records.get(streak.streak_type)
                all_time_length = record['length'] if record else 2
                progress = min(100, int(100 * streak.length / all_time_length)) if all_time_length > 0 else 0

                active_results.append({
                    'player_id': player.id,
                    'player_name': player.full_name,
                    'streak_type': streak.streak_type,
                    'length': streak.length,
                    'all_time_record': all_time_length,
                    'all_time_holder': record['player_name'] if record else None,
                    'progress_pct': progress,
                    'started_at': streak.started_at.isoformat() if streak.started_at else None,
                    'is_historical': streak.length >= all_time_length or streak.is_historical_outlier,
                })

            # 3. Recently broken streaks (últimos 7 días)
            broken_results = []
            if include_broken:
                latest_game_date = session.query(func.max(Game.date)).scalar()
                if latest_game_date:
                    broken_start = latest_game_date - timedelta(days=7)
                    broken_query = (
                        session.query(StreakRecord, Player)
                        .join(Player, StreakRecord.player_id == Player.id)
                        .outerjoin(StreakAllTimeRecord, and_(
                            StreakRecord.streak_type == StreakAllTimeRecord.streak_type,
                            StreakRecord.competition_type == StreakAllTimeRecord.competition_type,
                        ))
                        .filter(and_(
                            StreakRecord.is_active == False,
                            StreakRecord.ended_at >= broken_start,
                            StreakRecord.competition_type == competition_type,
                        ))
                        .filter(case(
                            (StreakRecord.streak_type.in_(shooting_types), StreakRecord.length >= 3),
                            else_=StreakRecord.length >= func.greatest(
                                2, cast(func.coalesce(StreakAllTimeRecord.length, 2), Float) * 0.05
                            ),
                        ))
                    )
                    if streak_type:
                        broken_query = broken_query.filter(StreakRecord.streak_type == streak_type)

                    broken_query = broken_query.order_by(
                        StreakRecord.ended_at.desc(),
                        StreakRecord.length.desc(),
                    )

                    for streak, player in broken_query.limit(limit).all():
                        broken_results.append({
                            'player_id': player.id,
                            'player_name': player.full_name,
                            'streak_type': streak.streak_type,
                            'length': streak.length,
                            'started_at': streak.started_at.isoformat() if streak.started_at else None,
                            'ended_at': streak.ended_at.isoformat() if streak.ended_at else None,
                            'is_historical': streak.is_historical_outlier,
                        })

            return to_json({
                "competition_type": competition_type,
                "active_streaks": {"count": len(active_results), "streaks": active_results},
                "broken_streaks": {"count": len(broken_results), "streaks": broken_results},
                "all_time_records": all_time_records,
            })
        finally:
            session.close()
