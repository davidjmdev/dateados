"""Configuración de conexión a la base de datos PostgreSQL.

Este módulo maneja:
- Configuración de la URL de conexión
- Creación de sesiones SQLAlchemy
- Inicialización de la base de datos
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

# URL de conexión a PostgreSQL
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://nba:nba@localhost:5432/nba_stats"
)


# Singleton engine instance and the PID that created it
_engine = None
_engine_pid = None


def get_engine():
    """Crea y retorna un engine de SQLAlchemy (Singleton per process).
    
    Detecta si el proceso ha cambiado (fork) y recrea el engine para evitar
    conflictos con el pool de conexiones del proceso padre.
    
    Returns:
        Engine: Engine de SQLAlchemy configurado
    """
    global _engine, _engine_pid
    current_pid = os.getpid()
    
    if _engine is None or _engine_pid != current_pid:
        if _engine is not None:
            _engine.dispose()
        _engine = create_engine(DATABASE_URL)
        _engine_pid = current_pid
        
    return _engine


def get_session():
    """Crea y retorna una sesión de SQLAlchemy.
    
    Returns:
        Session: Sesión de SQLAlchemy para interactuar con la BD
    """
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()


def init_db():
    """Inicializa la base de datos creando todas las tablas definidas en los modelos.
    
    Esta función debe ser llamada antes de usar los modelos para asegurar
    que todas las tablas existan en la base de datos.
    """
    from db.models import Base
    # Import outliers models to register them with Base.metadata
    try:
        from outliers.models import LeagueOutlier, PlayerOutlier, PlayerTrendOutlier, StreakRecord, StreakAllTimeRecord
    except ImportError:
        pass  # outliers module may not be available
    engine = get_engine()
    Base.metadata.create_all(engine)
