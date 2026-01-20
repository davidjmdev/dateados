import sys
import os
from pathlib import Path

# Configurar path del proyecto
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from db.connection import get_session
from db.models import LogEntry

def view_logs(limit=50, level=None):
    """Muestra los últimos logs guardados en la base de datos."""
    session = get_session()
    try:
        query = session.query(LogEntry).order_by(LogEntry.timestamp.desc())
        if level:
            query = query.filter(LogEntry.level == level.upper())
        
        logs = query.limit(limit).all()
        # Invertir para mostrar cronológicamente
        for log in reversed(logs):
            tb_str = f"\n{log.traceback}" if log.traceback else ""
            print(f"{log.timestamp} [{log.level}] {log.module}: {log.message}{tb_str}")
            
    finally:
        session.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Consultar logs de la base de datos')
    parser.add_argument('--limit', type=int, default=50, help='Número de logs a mostrar')
    parser.add_argument('--level', type=str, help='Filtrar por nivel (INFO, ERROR, etc.)')
    
    args = parser.parse_args()
    view_logs(args.limit, args.level)
