import logging
import traceback
from datetime import datetime, timezone
from sqlalchemy.orm import sessionmaker
from db.connection import get_engine

class SQLAlchemyHandler(logging.Handler):
    """Handler de logging que guarda registros en la tabla log_entries de la base de datos."""
    
    def __init__(self):
        super().__init__()
        self.engine = get_engine()
        self.Session = sessionmaker(bind=self.engine)

    def emit(self, record):
        from db.models import LogEntry
        
        # Evitar recursión si SQLAlchemy genera logs
        if record.name.startswith('sqlalchemy'):
            return

        session = self.Session()
        try:
            # Capturar traceback si existe
            tb = None
            if record.exc_info:
                tb = "".join(traceback.format_exception(*record.exc_info))
            
            log_entry = LogEntry(
                timestamp=datetime.fromtimestamp(record.created, tz=timezone.utc),
                level=record.levelname,
                module=record.name,
                message=record.getMessage(),
                traceback=tb
            )
            session.add(log_entry)
            session.commit()
        except Exception:
            # Si falla el guardado en DB (ej. DB caída), no queremos que la app muera
            session.rollback()
        finally:
            session.close()
