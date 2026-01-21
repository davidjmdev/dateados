import sys
import os
import time
import shutil
from datetime import datetime
from pathlib import Path

# Configurar path del proyecto
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from db.connection import get_session
from db.models import LogEntry, SystemStatus

# Colores ANSI
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    BG_DARK = '\033[48;5;235m'
    LINE = '\033[90m'

def get_status_color(status):
    if status == 'running': return Colors.BLUE
    if status == 'completed': return Colors.GREEN
    if status == 'failed': return Colors.RED
    return Colors.ENDC

def draw_progress_bar(percent, width=20):
    percent = min(100, max(0, percent))
    filled = int(width * percent / 100)
    bar = "█" * filled + "░" * (width - filled)
    return f"|{bar}| {percent:3}%"

def clear_screen():
    print("\033[H\033[2J", end="")

def get_terminal_size():
    size = shutil.get_terminal_size((80, 24))
    return size.columns, size.lines

def monitor_mode(interval=2):
    """Bucle de monitoreo tipo Dashboard en tiempo real."""
    try:
        while True:
            cols, rows = get_terminal_size()
            session = get_session()
            lines_printed = 0
            
            try:
                # 1. Obtener datos
                tasks = session.query(SystemStatus).order_by(SystemStatus.task_name).all()
                main_tasks = [t for t in tasks if not ("Batch" in t.task_name or "Worker" in t.task_name)]
                worker_tasks = [t for t in tasks if "Batch" in t.task_name or "Worker" in t.task_name]
                active_workers = [w for w in worker_tasks if w.status == 'running' or 
                                 (w.updated_at and (datetime.now() - w.updated_at.replace(tzinfo=None)).total_seconds() < 60)]

                # 2. Dibujar Cabecera
                clear_screen()
                title = " DATEADOS INGESTION MONITOR "
                padding = max(0, cols - len(title))
                print(f"{Colors.BG_DARK}{Colors.BOLD}{Colors.HEADER}{title}{Colors.ENDC}{Colors.LINE}{'─' * padding}{Colors.ENDC}")
                lines_printed += 1
                
                status_line = f" Hora: {datetime.now().strftime('%H:%M:%S')} | Terminal: {cols}x{rows}"
                print(f"{Colors.BOLD}{status_line}{Colors.ENDC}")
                lines_printed += 1
                
                # Proceso Principal
                # Ajustar ancho del recuadro al terminal (máximo 100)
                box_width = min(cols - 2, 100)
                if box_width > 40:
                    print(f"\n{Colors.BOLD}┌── PROCESOS PRINCIPALES {'─' * (box_width - 24)}┐{Colors.ENDC}")
                    lines_printed += 2
                    
                    if not main_tasks:
                        print(f"│ {'No hay procesos activos en este momento':<{box_width-2}} │")
                        lines_printed += 1
                    for t in main_tasks:
                        color = get_status_color(t.status)
                        name = (t.task_name or "Unknown")[:20]
                        status = (t.status or "IDLE").upper()[:10]
                        p_bar = draw_progress_bar(t.progress, width=15)
                        
                        # El mensaje ocupa el resto del espacio
                        rem_width = max(5, box_width - 20 - 10 - 20 - 6)
                        msg = (t.message or "")[:rem_width]
                        
                        print(f"│ {Colors.BOLD}{name:<20}{Colors.ENDC} {color}{status:<10}{Colors.ENDC} {p_bar} {msg:<{rem_width}} │")
                        lines_printed += 1
                    print(f"{Colors.BOLD}└{'─' * (box_width - 2)}┘{Colors.ENDC}")
                    lines_printed += 1

                # Workers
                if active_workers:
                    print(f"\n{Colors.BOLD}WORKERS ACTIVOS ({len(active_workers)}):{Colors.ENDC}")
                    lines_printed += 2
                    for t in active_workers:
                        color = get_status_color(t.status)
                        name = t.task_name[:20]
                        status = t.status.upper()[:10]
                        p_bar = draw_progress_bar(t.progress, width=12)
                        rem_width = max(5, cols - 20 - 10 - 17 - 5)
                        msg = (t.message or "")[:rem_width]
                        print(f"  {Colors.CYAN}{name:<20}{Colors.ENDC} {color}{status:<10}{Colors.ENDC} {p_bar} {msg}")
                        lines_printed += 1
                
                # 3. Dibujar Logs
                log_title = " ÚLTIMOS LOGS (Tiempo Real) "
                padding = max(0, cols - len(log_title))
                print(f"\n{Colors.BOLD}{log_title}{Colors.LINE}{'─' * padding}{Colors.ENDC}")
                lines_printed += 2
                
                # Calcular espacio real restante
                log_limit = max(5, rows - lines_printed - 3)
                logs = session.query(LogEntry).order_by(LogEntry.timestamp.desc()).limit(log_limit).all()
                
                for log in reversed(logs):
                    lvl_color = Colors.ENDC
                    if log.level == 'ERROR': lvl_color = Colors.RED
                    elif log.level == 'WARNING': lvl_color = Colors.YELLOW
                    elif log.level == 'INFO': lvl_color = Colors.GREEN
                    
                    ts = log.timestamp.strftime('%H:%M:%S')
                    # Calcular el ancho del prefijo para truncar correctamente el mensaje
                    # prefijo: "HH:MM:SS LEVEL   MODULE: "
                    prefix_len = 8 + 1 + 7 + 1 + len(log.module) + 2
                    msg_width = max(10, cols - prefix_len)
                    msg = log.message.replace('\n', ' ')[:msg_width]
                    
                    print(f"{Colors.LINE}{ts}{Colors.ENDC} {lvl_color}{log.level:<7}{Colors.ENDC} {Colors.BOLD}{log.module}:{Colors.ENDC} {msg}")
                    lines_printed += 1

                # Rellenar con líneas vacías si es necesario para evitar saltos
                if rows > lines_printed + 1:
                    print("\n" * (rows - lines_printed - 2), end="")
                
                print(f"{Colors.LINE}{'─' * cols}{Colors.ENDC}")
                print(f"{Colors.YELLOW}Ctrl+C para salir | Refresco: {interval}s{Colors.ENDC}", end="\r")

            except Exception as e:
                print(f"\n{Colors.RED}Error en monitor: {e}{Colors.ENDC}")
                time.sleep(5)
            finally:
                session.close()
            
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}Monitor finalizado.{Colors.ENDC}")


def view_logs(limit=50, level=None):
    """Muestra los últimos logs guardados en la base de datos (Modo estático)."""
    session = get_session()
    try:
        query = session.query(LogEntry).order_by(LogEntry.timestamp.desc())
        if level:
            query = query.filter(LogEntry.level == level.upper())
        
        logs = query.limit(limit).all()
        for log in reversed(logs):
            lvl_color = ""
            if log.level == 'ERROR': lvl_color = Colors.RED
            elif log.level == 'WARNING': lvl_color = Colors.YELLOW
            elif log.level == 'INFO': lvl_color = Colors.GREEN
            
            tb_str = f"\n{log.traceback}" if log.traceback else ""
            print(f"{Colors.LINE}{log.timestamp}{Colors.ENDC} {lvl_color}[{log.level}]{Colors.ENDC} {Colors.BOLD}{log.module}:{Colors.ENDC} {log.message}{tb_str}")
            
    finally:
        session.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Dashboard de Monitoreo Dateados')
    parser.add_argument('--limit', type=int, default=50, help='Número de logs a mostrar')
    parser.add_argument('--level', type=str, help='Filtrar por nivel (INFO, ERROR, etc.)')
    parser.add_argument('--monitor', '-m', action='store_true', help='Activar Dashboard en tiempo real')
    parser.add_argument('--interval', type=int, default=2, help='Intervalo de refresco en segundos')
    
    args = parser.parse_args()
    
    if args.monitor:
        monitor_mode(args.interval)
    else:
        view_logs(args.limit, args.level)
