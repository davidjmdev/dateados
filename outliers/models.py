"""Modelos SQLAlchemy para el sistema de detección de outliers.

Define tres tablas especializadas:
1. LeagueOutlier: Anomalías detectadas por el autoencoder (comparación con toda la liga)
2. PlayerOutlier: Anomalías por Z-score (comparación con la media del jugador)
3. StreakRecord: Rachas históricas de rendimiento excepcional
"""

from sqlalchemy import (
    Column, Integer, String, Float, Date, ForeignKey, 
    Boolean, DateTime, UniqueConstraint, Index, CheckConstraint
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSON
from datetime import datetime, timezone

from db.models import Base


def utc_now():
    """Retorna la fecha y hora actual en UTC."""
    return datetime.now(timezone.utc)


class LeagueOutlier(Base):
    """Outliers detectados comparando con el rendimiento histórico de toda la liga.
    
    Usa un autoencoder para calcular el error de reconstrucción. Partidos con
    error en el percentil 99+ son marcados como outliers.
    """
    __tablename__ = 'outliers_league'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_game_stat_id = Column(
        Integer, 
        ForeignKey('player_game_stats.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
        comment='Referencia a la línea estadística del partido'
    )
    
    # Métricas del autoencoder
    reconstruction_error = Column(
        Float,
        nullable=False,
        comment='Error de reconstrucción (MSE) del autoencoder'
    )
    percentile = Column(
        Float,
        nullable=False,
        comment='Percentil del error respecto al histórico (0-100)'
    )
    feature_contributions = Column(
        JSON,
        nullable=True,
        comment='Contribución de cada feature al error total'
    )
    is_outlier = Column(
        Boolean,
        default=False,
        nullable=False,
        comment='True si percentile >= 99'
    )
    
    # Metadata
    model_version = Column(
        String(50),
        nullable=True,
        comment='Versión del modelo usado para la detección'
    )
    
    # Auditoría
    created_at = Column(DateTime, default=utc_now, nullable=False)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)
    
    # Relaciones
    player_game_stat = relationship('PlayerGameStats', backref='league_outliers')
    
    __table_args__ = (
        UniqueConstraint('player_game_stat_id', name='uq_league_outlier_stat'),
        CheckConstraint('reconstruction_error >= 0', name='check_reconstruction_error'),
        CheckConstraint('percentile >= 0 AND percentile <= 100', name='check_percentile'),
        Index('idx_league_outlier_is_outlier', 'is_outlier'),
    )
    
    def __repr__(self):
        return f"<LeagueOutlier(stat_id={self.player_game_stat_id}, percentile={self.percentile:.1f}, outlier={self.is_outlier})>"


class PlayerOutlier(Base):
    """Outliers detectados comparando el rendimiento con la media histórica del jugador (Partido Único)."""
    __tablename__ = 'outliers_player'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_game_stat_id = Column(
        Integer,
        ForeignKey('player_game_stats.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
        comment='Referencia a la línea estadística del partido'
    )
    
    # Métricas Z-score
    z_scores = Column(JSON, nullable=False)
    max_z_score = Column(Float, nullable=False)
    outlier_type = Column(String(20), nullable=False) # explosion, crisis
    outlier_features = Column(JSON, nullable=False)
    games_in_sample = Column(Integer, nullable=False)
    
    # Auditoría
    created_at = Column(DateTime, default=utc_now, nullable=False)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)
    
    player_game_stat = relationship('PlayerGameStats', backref='player_outliers')
    
    __table_args__ = (
        UniqueConstraint('player_game_stat_id', name='uq_player_outlier_stat'),
        CheckConstraint("outlier_type IN ('explosion', 'crisis')", name='check_outlier_type'),
    )


class PlayerTrendOutlier(Base):
    """Tendencias anómalas de rendimiento en ventanas temporales (Semana o Mes)."""
    __tablename__ = 'outliers_player_trends'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey('players.id', ondelete='CASCADE'), nullable=False, index=True)
    
    window_type = Column(String(10), nullable=False) # 'week', 'month'
    reference_date = Column(Date, nullable=False, index=True) # Fecha fin de la ventana
    
    # Métricas
    z_scores = Column(JSON, nullable=False)
    max_z_score = Column(Float, nullable=False)
    outlier_type = Column(String(20), nullable=False) # explosion, crisis
    
    # Datos de comparación para storytelling
    comparison_data = Column(JSON, nullable=False)
    
    games_in_window = Column(Integer, nullable=False)
    games_in_baseline = Column(Integer, nullable=False)
    
    created_at = Column(DateTime, default=utc_now, nullable=False)
    
    player = relationship('Player')
    
    __table_args__ = (
        UniqueConstraint('player_id', 'window_type', 'reference_date', name='uq_player_trend_ref'),
        CheckConstraint("window_type IN ('week', 'month')", name='check_window_type'),
        CheckConstraint("outlier_type IN ('explosion', 'crisis')", name='check_trend_outlier_type'),
    )


class PlayerSeasonState(Base):
    """Estado acumulativo de estadísticas de un jugador en una temporada.
    
    Permite calcular media y desviación estándar en O(1) usando sumatorios.
    """
    __tablename__ = 'outliers_player_season_state'
    
    player_id = Column(Integer, ForeignKey('players.id', ondelete='CASCADE'), primary_key=True)
    season = Column(String(10), primary_key=True)
    
    games_played = Column(Integer, default=0, nullable=False)
    first_game_date = Column(Date, nullable=True)
    last_game_date = Column(Date, nullable=True)
    
    # Sumatorios para cálculo de Z-score (JSON: {stat: sum_x, stat_sq: sum_x2})
    # Ej: {"pts": 1200, "pts_sq": 24000, ...}
    accumulated_stats = Column(JSON, nullable=False, default=dict)
    
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)
    
    player = relationship('Player')
    
    def __repr__(self):
        return f"<PlayerOutlier(stat_id={self.player_game_stat_id}, type={self.outlier_type}, max_z={self.max_z_score:.2f})>"


class StreakRecord(Base):
    """Registro de rachas históricas de rendimiento excepcional.
    
    Rastrea secuencias consecutivas de partidos cumpliendo criterios específicos.
    Cuando una racha supera umbrales históricos notables, se marca como outlier.
    """
    __tablename__ = 'outliers_streaks'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(
        Integer,
        ForeignKey('players.id', ondelete='CASCADE'),
        nullable=False,
        index=True
    )
    
    # Definición de la racha
    streak_type = Column(
        String(30),
        nullable=False,
        comment='Tipo de racha: pts_20, pts_30, pts_40, triple_double, reb_10, ast_10, fg_pct_60'
    )
    
    competition_type = Column(
        String(20),
        nullable=False,
        default='regular',
        index=True,
        comment='Tipo de competición: regular, playoffs, nba_cup'
    )
    
    # Estado y métricas
    length = Column(
        Integer,
        nullable=False,
        default=1,
        comment='Longitud actual de la racha en partidos'
    )
    is_active = Column(
        Boolean,
        default=True,
        nullable=False,
        comment='True si la racha sigue activa'
    )
    is_historical_outlier = Column(
        Boolean,
        default=False,
        nullable=False,
        comment='True si la racha supera umbrales históricos notables'
    )
    
    # Fechas
    started_at = Column(
        Date,
        nullable=False,
        comment='Fecha del primer partido de la racha'
    )
    ended_at = Column(
        Date,
        nullable=True,
        comment='Fecha del último partido (null si está activa)'
    )
    
    # Referencias a partidos
    first_game_id = Column(
        String(15),
        ForeignKey('games.id'),
        nullable=False,
        comment='ID del primer partido de la racha'
    )
    last_game_id = Column(
        String(15),
        ForeignKey('games.id'),
        nullable=True,
        comment='ID del último partido (null si está activa)'
    )
    
    # Auditoría
    created_at = Column(DateTime, default=utc_now, nullable=False)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)
    
    # Relaciones
    player = relationship('Player', backref='streak_records')
    first_game = relationship('Game', foreign_keys=[first_game_id])
    last_game = relationship('Game', foreign_keys=[last_game_id])
    
    __table_args__ = (
        # Solo puede haber una racha activa por jugador, tipo y competición
        Index(
            'uq_active_streak_player_type_comp', 
            'player_id', 'streak_type', 'competition_type',
            unique=True,
            postgresql_where=(is_active == True)
        ),
        CheckConstraint('length >= 1', name='check_streak_length'),
        CheckConstraint("competition_type IN ('regular', 'playoffs', 'nba_cup')", name='check_competition_type'),
        Index('idx_streak_active', 'is_active'),
        Index('idx_streak_historical', 'is_historical_outlier'),
        Index('idx_streak_player_type_comp', 'player_id', 'streak_type', 'competition_type'),
    )
    
    def __repr__(self):
        status = "active" if self.is_active else "ended"
        return f"<StreakRecord(player_id={self.player_id}, type={self.streak_type}, length={self.length}, {status})>"


# Porcentaje del récord histórico necesario para recibir el distintivo de "HISTÓRICA" (70%)
STREAK_HISTORICAL_PERCENTAGE = 0.70


class StreakAllTimeRecord(Base):
    """Caché de los récords históricos (All-Time) para cada tipo de racha y competición.
    
    Se utiliza para comparar las rachas actuales contra la mejor marca registrada
    en la base de datos (1983-presente) sin realizar escaneos pesados.
    """
    __tablename__ = 'outliers_streak_all_time_records'
    
    streak_type = Column(
        String(30),
        primary_key=True,
        comment='Tipo de racha (PK)'
    )
    
    competition_type = Column(
        String(20),
        primary_key=True,
        default='regular',
        comment='Tipo de competición (PK): regular, playoffs, nba_cup'
    )
    
    player_id = Column(
        Integer,
        ForeignKey('players.id', ondelete='CASCADE'),
        nullable=False,
        index=True
    )
    
    length = Column(
        Integer,
        nullable=False,
        comment='Longitud máxima histórica'
    )
    
    started_at = Column(
        Date,
        nullable=False,
        comment='Fecha de inicio del récord'
    )
    
    ended_at = Column(
        Date,
        nullable=True,
        comment='Fecha de fin del récord'
    )
    
    game_id_start = Column(
        String(15),
        ForeignKey('games.id'),
        nullable=False
    )
    
    game_id_end = Column(
        String(15),
        ForeignKey('games.id'),
        nullable=True
    )
    
    # Auditoría
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)
    
    # Relaciones
    player = relationship('Player')
    
    def __repr__(self):
        return f"<StreakAllTimeRecord(type={self.streak_type}, length={self.length}, player_id={self.player_id})>"
