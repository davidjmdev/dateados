"""Modelos SQLAlchemy para la base de datos NBA.

Este módulo define todos los modelos de datos (ORM) que representan
las tablas en la base de datos PostgreSQL.
"""

from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, Interval, Boolean, DateTime, UniqueConstraint, Index, CheckConstraint
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.dialects.postgresql import JSON
from datetime import datetime, timezone

Base = declarative_base()


def utc_now():
    """Retorna la fecha y hora actual en UTC (timezone-aware).
    
    Usado como default para campos created_at en los modelos.
    """
    return datetime.now(timezone.utc)


class Team(Base):
    """Modelo para equipos de la NBA."""
    __tablename__ = 'teams'
    
    id = Column(Integer, primary_key=True)
    full_name = Column(String(100), nullable=False)
    abbreviation = Column(String(25), unique=True, nullable=False)
    city = Column(String(50))
    state = Column(String(50))
    nickname = Column(String(50))
    year_founded = Column(Integer)
    conference = Column(String(10), nullable=True, comment='Conferencia: East o West')
    division = Column(String(20), nullable=True, comment='División: Atlantic, Central, Southeast, Northwest, Pacific, Southwest')
    
    # Auditoría
    created_at = Column(DateTime, default=utc_now, nullable=False)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)
    
    # Relaciones
    home_games = relationship(
        'Game',
        foreign_keys='Game.home_team_id',
        back_populates='home_team'
    )
    away_games = relationship(
        'Game',
        foreign_keys='Game.away_team_id',
        back_populates='away_team'
    )
    player_stats = relationship('PlayerGameStats', back_populates='team')
    team_game_stats = relationship('TeamGameStats', back_populates='team')
    player_team_seasons = relationship('PlayerTeamSeason', back_populates='team')
    
    # Índices
    __table_args__ = (
        Index('idx_teams_conference_division', 'conference', 'division'),
    )
    
    def __repr__(self):
        return f"<Team(id={self.id}, name='{self.full_name}')>"


class Player(Base):
    """Modelo para jugadores de la NBA."""
    __tablename__ = 'players'
    
    id = Column(Integer, primary_key=True)
    full_name = Column(String(100), nullable=False)
    
    # Información personal
    birthdate = Column(Date, nullable=True, comment='Fecha de nacimiento')
    height = Column(String(10), nullable=True, comment='Altura en formato pies-pulgadas (ej: 6-9)')
    weight = Column(Integer, nullable=True, comment='Peso en libras')
    position = Column(String(20), nullable=True, comment='Posición de juego (ej: Forward, Guard, Center) - No cambia')
    country = Column(String(50), nullable=True, comment='País de origen')
    jersey = Column(String(10), nullable=True, comment='Dorsal actual o último conocido')
    
    # Información de carrera
    is_active = Column(Boolean, default=False, nullable=False, comment='True si el jugador está en activo')
    season_exp = Column(Integer, nullable=True, comment='Años de experiencia en la NBA')
    from_year = Column(Integer, nullable=True, comment='Año de inicio en la NBA')
    to_year = Column(Integer, nullable=True, comment='Último año activo en la NBA')
    
    # Información del draft
    draft_year = Column(Integer, nullable=True, comment='Año del draft')
    draft_round = Column(Integer, nullable=True, comment='Ronda del draft')
    draft_number = Column(Integer, nullable=True, comment='Número de selección en el draft')
    
    # Información adicional
    school = Column(String(100), nullable=True, comment='Universidad/colegio de procedencia')
    
    # Control de sincronización
    awards_synced = Column(Boolean, default=False, nullable=False, 
                          comment='True si ya se ha sincronizado el palmarés del jugador')
    last_award_sync = Column(DateTime, nullable=True, 
                            comment='Última vez que se sincronizaron los premios del jugador')
    bio_synced = Column(Boolean, default=False, nullable=False,
                        comment='True si ya se ha intentado sincronizar la biografía del jugador')
    
    # Auditoría
    created_at = Column(DateTime, default=utc_now, nullable=False)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)
    
    # Relaciones
    game_stats = relationship('PlayerGameStats', back_populates='player')
    team_seasons = relationship('PlayerTeamSeason', back_populates='player')
    
    # Índices y constraints
    __table_args__ = (
        CheckConstraint('weight > 0', name='check_weight_positive'),
        CheckConstraint('season_exp >= 0', name='check_exp_positive'),
        Index('idx_players_name', 'full_name'),
        Index('idx_players_position', 'position'),
        Index('idx_players_award_sync_active', 'last_award_sync', 'is_active'),
    )
    
    def __repr__(self):
        return f"<Player(id={self.id}, name='{self.full_name}')>"
    
    @property
    def experience(self):
        """Alias para experience_calculated para mantener compatibilidad."""
        return self.experience_calculated
    
    @property
    def experience_calculated(self):
        """Calcula la experiencia basada en el número de temporadas distintas registradas."""
        if not self.team_seasons:
            return self.season_exp or 0
        seasons = {ts.season for ts in self.team_seasons}
        return len(seasons)



class Game(Base):
    """Modelo para partidos de la NBA.
    
    Incluye información básica del partido y resultados (marcadores finales y por cuarto).
    Los campos de resultados son nullable porque no todos los partidos tienen resultados disponibles al momento de la ingesta.
    """
    __tablename__ = 'games'
    
    id = Column(String(15), primary_key=True)
    date = Column(Date, nullable=False, index=True)
    season = Column(String(10), nullable=False, index=True)
    rs = Column(Boolean, default=False, comment='Regular Season')
    ist = Column(Boolean, default=False, comment='In-Season Tournament')
    po = Column(Boolean, default=False, comment='Playoffs')
    pi = Column(Boolean, default=False, comment='PlayIn')
    
    # Campos de resultados (fusionados desde game_scores)
    status = Column(Integer, default=1, comment='Estado del partido (1=Scheduled, 2=In Progress, 3=Final)')
    home_team_id = Column(Integer, ForeignKey('teams.id'), nullable=True, index=True, comment='ID del equipo local')
    away_team_id = Column(Integer, ForeignKey('teams.id'), nullable=True, index=True, comment='ID del equipo visitante')
    home_score = Column(Integer, nullable=True, comment='Puntos del equipo local (marcador final)')
    away_score = Column(Integer, nullable=True, comment='Puntos del equipo visitante (marcador final)')
    # winner_team_id es redundante pero útil para consultas frecuentes
    # Se calcula desde home_score y away_score, pero se mantiene por performance
    winner_team_id = Column(Integer, ForeignKey('teams.id'), nullable=True, comment='Equipo ganador (calculado automáticamente)')
    
    # Marcadores por cuarto (JSON)
    # Estructura: {"home": [Q1, Q2, Q3, Q4, OT1, OT2, ...], "away": [Q1, Q2, Q3, Q4, OT1, OT2, ...]}
    quarter_scores = Column(JSON, nullable=True, comment='Marcadores por cuarto en formato JSON. Puede incluir overtimes si los hay.')
    
    # Auditoría
    created_at = Column(DateTime, default=utc_now, nullable=False)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)
    
    # Relaciones
    home_team = relationship('Team', foreign_keys=[home_team_id], back_populates='home_games')
    away_team = relationship('Team', foreign_keys=[away_team_id], back_populates='away_games')
    player_stats = relationship('PlayerGameStats', back_populates='game')
    team_game_stats = relationship('TeamGameStats', back_populates='game')
    
    # Índices compuestos y constraints
    __table_args__ = (
        CheckConstraint('home_score >= 0', name='check_home_score'),
        CheckConstraint('away_score >= 0', name='check_away_score'),
        Index('idx_games_season_date', 'season', 'date'),
        Index('idx_games_teams', 'home_team_id', 'away_team_id'),
    )
    
    def __repr__(self):
        return f"<Game(id='{self.id}', date={self.date}, season='{self.season}')>"
    
    def get_winner(self):
        """Retorna el ID del equipo ganador o None si es empate/no finalizado."""
        if self.home_score is None or self.away_score is None:
            return None
        if self.home_score > self.away_score:
            return self.home_team_id
        elif self.away_score > self.home_score:
            return self.away_team_id
        return None  # Empate
    
    @property
    def is_finished(self):
        """Retorna True si el partido ha finalizado oficialmente (Status 3)."""
        return self.status == 3
    
    @property
    def total_points(self):
        """Retorna el total de puntos del partido."""
        if self.home_score is None or self.away_score is None:
            return None
        return self.home_score + self.away_score


class PlayerGameStats(Base):
    """Modelo para estadísticas de jugadores en partidos individuales."""
    __tablename__ = 'player_game_stats'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(15), ForeignKey('games.id'), nullable=False, index=True)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False, index=True)
    team_id = Column(Integer, ForeignKey('teams.id'), nullable=False, index=True)
    
    # Core Stats
    min = Column(Interval, nullable=False)
    pts = Column(Integer, nullable=False)
    reb = Column(Integer, nullable=False)
    ast = Column(Integer, nullable=False)
    stl = Column(Integer, nullable=False)
    blk = Column(Integer, nullable=False)
    tov = Column(Integer, nullable=False)
    pf = Column(Integer, nullable=False)
    plus_minus = Column(Float, nullable=True)
    
    # Shooting
    fgm = Column(Integer, nullable=False, comment='Field Goals Made')
    fga = Column(Integer, nullable=False, comment='Field Goals Attempted')
    fg_pct = Column(Float, nullable=True, comment='Field Goal Percentage')
    fg3m = Column(Integer, nullable=False, comment='3-Point Field Goals Made')
    fg3a = Column(Integer, nullable=False, comment='3-Point Field Goals Attempted')
    fg3_pct = Column(Float, nullable=True, comment='3-Point Field Goal Percentage')
    ftm = Column(Integer, nullable=False, comment='Free Throws Made')
    fta = Column(Integer, nullable=False, comment='Free Throws Attempted')
    ft_pct = Column(Float, nullable=True, comment='Free Throw Percentage')
    
    # Relaciones
    game = relationship('Game', back_populates='player_stats')
    player = relationship('Player', back_populates='game_stats')
    team = relationship('Team', back_populates='player_stats')
    
    # Constraints e índices
    __table_args__ = (
        # Constraint de unicidad: un jugador solo puede tener una entrada por partido
        UniqueConstraint('game_id', 'player_id', name='uq_player_game'),
        
        # Check constraints para valores no negativos
        CheckConstraint('pts >= 0', name='check_pts'),
        CheckConstraint('reb >= 0', name='check_reb'),
        CheckConstraint('ast >= 0', name='check_ast'),
        CheckConstraint('stl >= 0', name='check_stl'),
        CheckConstraint('blk >= 0', name='check_blk'),
        CheckConstraint('tov >= 0', name='check_tov'),
        CheckConstraint('pf >= 0', name='check_pf'),
        CheckConstraint('fgm >= 0', name='check_fgm'),
        CheckConstraint('fga >= 0', name='check_fga'),
        CheckConstraint('fg3m >= 0', name='check_fg3m'),
        CheckConstraint('fg3a >= 0', name='check_fg3a'),
        CheckConstraint('ftm >= 0', name='check_ftm'),
        CheckConstraint('fta >= 0', name='check_fta'),
        
        # Check constraints para porcentajes
        CheckConstraint('fg_pct >= 0 AND fg_pct <= 1', name='check_fg_pct'),
        CheckConstraint('fg3_pct >= 0 AND fg3_pct <= 1', name='check_fg3_pct'),
        CheckConstraint('ft_pct >= 0 AND ft_pct <= 1', name='check_ft_pct'),
        
        # Check constraints para lógica de shooting
        CheckConstraint('fgm <= fga', name='check_fgm_lte_fga'),
        CheckConstraint('fg3m <= fg3a', name='check_fg3m_lte_fg3a'),
        CheckConstraint('fg3m <= fgm', name='check_fg3m_lte_fgm'),
        CheckConstraint('fg3a <= fga', name='check_fg3a_lte_fga'),
        CheckConstraint('ftm <= fta', name='check_ftm_lte_fta'),
        
        # Índices compuestos para consultas comunes
        Index('idx_player_game_stats_player_season', 'player_id', 'game_id'),
        Index('idx_player_game_stats_team_game', 'team_id', 'game_id'),
    )
    
    def __repr__(self):
        return f"<PlayerGameStats(player_id={self.player_id}, game_id='{self.game_id}', pts={self.pts})>"
    
    @property
    def is_triple_double(self):
        """Retorna True si el jugador logró un triple-doble."""
        stats = [self.pts, self.reb, self.ast, self.stl, self.blk]
        return sum(1 for s in stats if s >= 10) >= 3
    
    @property
    def is_double_double(self):
        """Retorna True si el jugador logró un doble-doble."""
        stats = [self.pts, self.reb, self.ast, self.stl, self.blk]
        return sum(1 for s in stats if s >= 10) >= 2

    @property
    def minutes_formatted(self):
        """Retorna los minutos jugados en formato MM:SS."""
        if not self.min:
            return "0:00"
        total_seconds = int(self.min.total_seconds())
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        return f"{minutes}:{seconds:02d}"



class PlayerTeamSeason(Base):
    """Relación jugador-equipo-temporada (resumen estadístico).
    
    Representa las estadísticas agregadas de un jugador para un equipo, temporada y tipo de competición.
    """
    __tablename__ = 'player_team_seasons'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False, index=True)
    team_id = Column(Integer, ForeignKey('teams.id'), nullable=False, index=True)
    season = Column(String(10), nullable=False, index=True)
    type = Column(String(20), default='Regular Season', nullable=False, index=True,
                 comment='Tipo de competición: Regular Season, Playoffs, NBA Cup, etc.')
    
    # Fechas
    start_date = Column(Date, nullable=True)
    end_date = Column(Date, nullable=True)
    
    # Estadísticas agregadas
    games_played = Column(Integer, default=0)
    minutes = Column(Interval, nullable=True)
    pts = Column(Integer, default=0)
    reb = Column(Integer, default=0)
    ast = Column(Integer, default=0)
    stl = Column(Integer, default=0)
    blk = Column(Integer, default=0)
    tov = Column(Integer, default=0)
    pf = Column(Integer, default=0)
    
    # Shooting
    fgm = Column(Integer, default=0)
    fga = Column(Integer, default=0)
    fg3m = Column(Integer, default=0)
    fg3a = Column(Integer, default=0)
    ftm = Column(Integer, default=0)
    fta = Column(Integer, default=0)
    
    plus_minus = Column(Float, nullable=True)
    
    # Origen del dato
    is_detailed = Column(Boolean, default=False, nullable=False,
                        comment='True si las estadísticas se derivaron de partidos individuales en BD')
    
    # Auditoría
    created_at = Column(DateTime, default=utc_now, nullable=False)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)
    
    # Relaciones
    player = relationship('Player', back_populates='team_seasons')
    team = relationship('Team', back_populates='player_team_seasons')
    
    __table_args__ = (
        UniqueConstraint('player_id', 'team_id', 'season', 'type', name='uq_player_team_season_type'),
        Index('idx_player_season_type', 'player_id', 'season', 'type'),
        # Un jugador puede estar en múltiples equipos por temporada (trades)
        CheckConstraint('games_played >= 0', name='check_games_played'),
        Index('idx_player_season', 'player_id', 'season'),
        Index('idx_team_season', 'team_id', 'season'),
        # Índice compuesto para optimizar consultas por (player_id, team_id, season)
        Index('idx_player_team_season', 'player_id', 'team_id', 'season'),
    )
    
    def __repr__(self):
        return f"<PlayerTeamSeason(player_id={self.player_id}, team_id={self.team_id}, season='{self.season}')>"


class TeamGameStats(Base):
    """Estadísticas agregadas del equipo por partido.
    
    Esta tabla almacena estadísticas totales del equipo en cada partido,
    evitando tener que agregar desde player_game_stats en cada consulta.
    """
    __tablename__ = 'team_game_stats'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(15), ForeignKey('games.id'), nullable=False, index=True)
    team_id = Column(Integer, ForeignKey('teams.id'), nullable=False, index=True)
    
    # Core Stats (agregadas desde player_game_stats)
    total_pts = Column(Integer, nullable=False)
    total_reb = Column(Integer, nullable=False)
    total_ast = Column(Integer, nullable=False)
    total_stl = Column(Integer, nullable=False)
    total_blk = Column(Integer, nullable=False)
    total_tov = Column(Integer, nullable=False)
    total_pf = Column(Integer, nullable=False)
    avg_plus_minus = Column(Float, nullable=True, comment='Promedio de plus_minus del equipo')
    
    # Shooting (agregadas)
    total_fgm = Column(Integer, nullable=False)
    total_fga = Column(Integer, nullable=False)
    fg_pct = Column(Float, nullable=True)
    total_fg3m = Column(Integer, nullable=False)
    total_fg3a = Column(Integer, nullable=False)
    fg3_pct = Column(Float, nullable=True)
    total_ftm = Column(Integer, nullable=False)
    total_fta = Column(Integer, nullable=False)
    ft_pct = Column(Float, nullable=True)
    
    # Auditoría
    created_at = Column(DateTime, default=utc_now, nullable=False)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)
    
    # Relaciones
    game = relationship('Game', back_populates='team_game_stats')
    team = relationship('Team', back_populates='team_game_stats')
    
    # Constraints e índices
    __table_args__ = (
        # Un equipo solo puede tener una entrada de estadísticas por partido
        UniqueConstraint('game_id', 'team_id', name='uq_team_game'),
        
        # Check constraints para valores no negativos
        CheckConstraint('total_pts >= 0', name='check_total_pts'),
        CheckConstraint('total_reb >= 0', name='check_total_reb'),
        CheckConstraint('total_ast >= 0', name='check_total_ast'),
        CheckConstraint('total_stl >= 0', name='check_total_stl'),
        CheckConstraint('total_blk >= 0', name='check_total_blk'),
        CheckConstraint('total_tov >= 0', name='check_total_tov'),
        CheckConstraint('total_pf >= 0', name='check_total_pf'),
        CheckConstraint('total_fgm >= 0', name='check_total_fgm'),
        CheckConstraint('total_fga >= 0', name='check_total_fga'),
        CheckConstraint('total_fg3m >= 0', name='check_total_fg3m'),
        CheckConstraint('total_fg3a >= 0', name='check_total_fg3a'),
        CheckConstraint('total_ftm >= 0', name='check_total_ftm'),
        CheckConstraint('total_fta >= 0', name='check_total_fta'),
        
        # Check constraints para porcentajes
        CheckConstraint('fg_pct >= 0 AND fg_pct <= 1', name='check_team_fg_pct'),
        CheckConstraint('fg3_pct >= 0 AND fg3_pct <= 1', name='check_team_fg3_pct'),
        CheckConstraint('ft_pct >= 0 AND ft_pct <= 1', name='check_team_ft_pct'),
        
        # Check constraints para lógica de shooting
        CheckConstraint('total_fgm <= total_fga', name='check_team_fgm_lte_fga'),
        CheckConstraint('total_fg3m <= total_fg3a', name='check_team_fg3m_lte_fga'),
        CheckConstraint('total_fg3m <= total_fgm', name='check_team_fg3m_lte_fgm'),
        CheckConstraint('total_fg3a <= total_fga', name='check_team_fg3a_lte_fga'),
        CheckConstraint('total_ftm <= total_fta', name='check_team_ftm_lte_fta'),
        
        # Índices compuestos para consultas comunes
        Index('idx_team_game_stats_team_season', 'team_id', 'game_id'),
    )
    
    def __repr__(self):
        return f"<TeamGameStats(game_id='{self.game_id}', team_id={self.team_id}, pts={self.total_pts})>"


class PlayerAward(Base):
    """Modelo para premios y reconocimientos de jugadores."""
    __tablename__ = 'player_awards'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False, index=True)
    season = Column(String(10), nullable=False, index=True)
    award_type = Column(String(50), nullable=False, index=True, comment='Categoría: MVP, All-NBA, All-Star, Champion, POTW, etc.')
    award_name = Column(String(100), nullable=False)
    description = Column(String(255), nullable=True)
    
    # Auditoría
    created_at = Column(DateTime, default=utc_now, nullable=False)
    
    # Relaciones
    player = relationship('Player', backref='awards')
    
    __table_args__ = (
        # Un jugador puede tener múltiples premios en la misma temporada (ej: MVP y Champion)
        # Pero evitamos duplicados exactos
        UniqueConstraint('player_id', 'season', 'award_type', 'award_name', 'description', name='uq_player_award'),
    )
    
    def __repr__(self):
        return f"<PlayerAward(player_id={self.player_id}, season='{self.season}', award='{self.award_name}')>"


class IngestionCheckpoint(Base):
    """Modelo para checkpoints del proceso de ingesta.
    
    Permite reanudar la ingesta desde el último punto guardado tras un fallo,
    evitando reprocesar datos ya ingresados.
    """
    __tablename__ = 'ingestion_checkpoints'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    checkpoint_type = Column(String(50), nullable=False, index=True,
                            comment='Tipo: season, awards, daily, boxscore')
    checkpoint_key = Column(String(50), nullable=False,
                           comment='Identificador: 2023-24, 2024-01-15, player_id, etc.')
    status = Column(String(20), default='pending', nullable=False,
                   comment='Estado: pending, in_progress, completed, failed')
    
    # Para reanudar dentro de una temporada/día
    last_game_id = Column(String(15), nullable=True,
                         comment='Último game_id procesado para reanudar')
    last_player_id = Column(Integer, nullable=True,
                           comment='Último player_id procesado para premios')
    games_processed = Column(Integer, default=0,
                            comment='Contador de partidos procesados en este checkpoint')
    
    # Control de errores
    error_count = Column(Integer, default=0,
                        comment='Número de errores acumulados')
    last_error = Column(String(500), nullable=True,
                       comment='Último mensaje de error')
    
    # Metadata
    metadata_json = Column(JSON, nullable=True,
                          comment='Datos adicionales del checkpoint en JSON')
    
    # Auditoría
    created_at = Column(DateTime, default=utc_now, nullable=False)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)
    
    __table_args__ = (
        # Solo puede haber un checkpoint por tipo+clave
        UniqueConstraint('checkpoint_type', 'checkpoint_key', name='uq_checkpoint_type_key'),
        Index('idx_checkpoint_status', 'status'),
    )
    
    def __repr__(self):
        return f"<IngestionCheckpoint(type='{self.checkpoint_type}', key='{self.checkpoint_key}', status='{self.status}')>"
    
class SystemStatus(Base):
    """Modelo para persistir el estado de tareas del sistema (ej: ingesta)."""
    __tablename__ = 'system_status'
    
    task_name = Column(String(50), primary_key=True)
    status = Column(String(20), default='idle', nullable=False,
                    comment='Estado: idle, running, completed, failed')
    progress = Column(Integer, default=0, nullable=False,
                     comment='Porcentaje de progreso (0-100)')
    message = Column(String(255), nullable=True,
                    comment='Mensaje descriptivo del paso actual')
    
    # Auditoría
    last_run = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now, nullable=False)
    
    def __repr__(self):
        return f"<SystemStatus(task='{self.task_name}', status='{self.status}', progress={self.progress}%)>"


class LogEntry(Base):
    """Modelo para persistir logs del sistema en la base de datos."""
    __tablename__ = 'log_entries'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=utc_now, nullable=False, index=True)
    level = Column(String(20), nullable=False, index=True)
    module = Column(String(100), nullable=False)
    message = Column(String, nullable=False)
    traceback = Column(String, nullable=True)
    
    def __repr__(self):
        return f"<LogEntry(id={self.id}, level='{self.level}', module='{self.module}')>"
