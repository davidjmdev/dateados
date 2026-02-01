"""Background Worker Daemon para Render.

Este worker escucha comandos desde la base de datos y ejecuta tareas de ingesta
de forma continua sin los límites de tiempo de los Web Services.

Diseñado específicamente para Render Free Tier Background Workers que:
- No tienen límite de tiempo de ejecución
- Pueden correr 24/7 sin spin-down
- Se comunican vía base de datos en lugar de HTTP

Uso:
    python -m ingestion.worker_daemon
"""

import logging
import time
import signal
import sys
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session

from db import get_session
from db.models import SystemStatus
from db.logging import setup_logging
from ingestion.strategies import SmartIngestion
from ingestion.api_client import NBAApiClient
from ingestion.checkpoints import CheckpointManager
from ingestion.utils import ProgressReporter

logger = logging.getLogger("dateados.worker.daemon")

# Flag global para shutdown graceful
shutdown_requested = False


def signal_handler(signum, frame):
    """Maneja señales de sistema para shutdown graceful."""
    global shutdown_requested
    sig_name = signal.Signals(signum).name
    logger.info(f"Señal {sig_name} recibida. Iniciando shutdown graceful...")
    shutdown_requested = True


def register_signal_handlers():
    """Registra handlers para señales de sistema."""
    signal.signal(signal.SIGTERM, signal_handler)  # Render envía SIGTERM
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C local


def check_for_commands(session: Session) -> Optional[dict]:
    """Verifica si hay comandos pendientes en la base de datos.
    
    Args:
        session: Sesión de SQLAlchemy
        
    Returns:
        Diccionario con comando si existe, None si no hay comandos
    """
    try:
        cmd = session.query(SystemStatus).filter_by(
            task_name="worker_command"
        ).first()
        
        if cmd and cmd.message and cmd.status == "pending":
            return {
                "command": cmd.message,
                "metadata": cmd.progress  # Usamos progress para metadata adicional
            }
        
        return None
    except Exception as e:
        logger.error(f"Error verificando comandos: {e}")
        return None


def execute_smart_ingestion(session: Session):
    """Ejecuta la ingesta inteligente.
    
    Args:
        session: Sesión de SQLAlchemy
    """
    logger.info("=" * 80)
    logger.info("INICIANDO INGESTA INTELIGENTE (Worker Background)")
    logger.info("=" * 80)
    
    try:
        # Crear reporter
        reporter = ProgressReporter("smart_ingestion", session_factory=get_session)
        reporter.update(0, "Iniciando ingesta inteligente...", status="running")
        
        # Ejecutar ingesta
        api = NBAApiClient()
        ingestion = SmartIngestion(api, checkpoint_mgr=CheckpointManager())
        ingestion.run(reporter=reporter)
        
        # Marcar como completado
        reporter.complete("Ingesta inteligente completada con éxito")
        logger.info("✅ Ingesta inteligente completada")
        
    except Exception as e:
        logger.error(f"Error en ingesta inteligente: {e}", exc_info=True)
        reporter = ProgressReporter("smart_ingestion", session_factory=get_session)
        reporter.fail(f"Error: {str(e)[:200]}")
        raise


def execute_awards_sync(session: Session):
    """Ejecuta sincronización de premios.
    
    Args:
        session: Sesión de SQLAlchemy
    """
    logger.info("Iniciando sincronización de premios...")
    
    try:
        from ingestion.models_sync import PlayerAwardsSync, get_players_needing_award_sync
        
        reporter = ProgressReporter("awards_sync", session_factory=get_session)
        reporter.update(0, "Sincronizando premios...", status="running")
        
        # Obtener jugadores que necesitan sync
        player_ids = get_players_needing_award_sync(session, force_all=True)
        
        if not player_ids:
            logger.info("No hay jugadores que necesiten sincronización de premios")
            reporter.complete("No hay premios pendientes")
            return
        
        # Ejecutar sync
        api = NBAApiClient()
        awards_sync = PlayerAwardsSync(api)
        ckpt_mgr = CheckpointManager(checkpoint_key="worker_awards_sync")
        
        awards_sync.sync_batch(
            session, 
            player_ids, 
            ckpt_mgr, 
            show_progress=True,
            reporter=reporter
        )
        
        ckpt_mgr.clear()
        reporter.complete(f"{len(player_ids)} jugadores sincronizados")
        logger.info(f"✅ Premios sincronizados para {len(player_ids)} jugadores")
        
    except Exception as e:
        logger.error(f"Error en sincronización de premios: {e}", exc_info=True)
        reporter = ProgressReporter("awards_sync", session_factory=get_session)
        reporter.fail(f"Error: {str(e)[:200]}")
        raise


def execute_outliers_backfill(session: Session):
    """Ejecuta backfill de outliers.
    
    Args:
        session: Sesión de SQLAlchemy
    """
    logger.info("Iniciando backfill de outliers...")
    
    try:
        from outliers.runner import run_detection_for_games
        from db.models import PlayerGameStats
        
        reporter = ProgressReporter("outliers_backfill", session_factory=get_session)
        reporter.update(0, "Procesando outliers...", status="running")
        
        # Obtener todas las estadísticas
        all_stats = session.query(PlayerGameStats).all()
        
        if not all_stats:
            logger.info("No hay estadísticas para procesar")
            reporter.complete("No hay datos para procesar")
            return
        
        # Ejecutar detección
        result = run_detection_for_games(session, all_stats)
        
        reporter.complete(f"{result.total_outliers} outliers detectados")
        logger.info(f"✅ Outliers procesados: {result.total_outliers} detectados")
        
    except Exception as e:
        logger.error(f"Error en backfill de outliers: {e}", exc_info=True)
        reporter = ProgressReporter("outliers_backfill", session_factory=get_session)
        reporter.fail(f"Error: {str(e)[:200]}")
        raise


def execute_command(command: str, session: Session):
    """Ejecuta un comando recibido.
    
    Args:
        command: Comando a ejecutar
        session: Sesión de SQLAlchemy
    """
    logger.info(f"Ejecutando comando: {command}")
    
    # Marcar comando como en ejecución
    cmd_record = session.query(SystemStatus).filter_by(
        task_name="worker_command"
    ).first()
    
    if cmd_record:
        cmd_record.status = "running"
        cmd_record.updated_at = datetime.now()
        session.commit()
    
    session.close()  # Cerrar sesión antes de ejecutar tarea larga
    
    try:
        if command == "RUN_INGESTION":
            new_session = get_session()
            execute_smart_ingestion(new_session)
            new_session.close()
            
        elif command == "RUN_AWARDS_SYNC":
            new_session = get_session()
            execute_awards_sync(new_session)
            new_session.close()
            
        elif command == "RUN_OUTLIERS_BACKFILL":
            new_session = get_session()
            execute_outliers_backfill(new_session)
            new_session.close()
            
        else:
            logger.warning(f"Comando desconocido: {command}")
        
        # Limpiar comando exitoso
        cleanup_session = get_session()
        cmd_record = cleanup_session.query(SystemStatus).filter_by(
            task_name="worker_command"
        ).first()
        
        if cmd_record:
            cleanup_session.delete(cmd_record)
            cleanup_session.commit()
        
        cleanup_session.close()
        logger.info(f"✅ Comando {command} completado y limpiado")
        
    except Exception as e:
        logger.error(f"❌ Error ejecutando comando {command}: {e}")
        
        # Marcar comando como fallido
        error_session = get_session()
        cmd_record = error_session.query(SystemStatus).filter_by(
            task_name="worker_command"
        ).first()
        
        if cmd_record:
            cmd_record.status = "failed"
            cmd_record.message = f"ERROR: {command}"
            cmd_record.progress = 0
            error_session.commit()
        
        error_session.close()
        raise


def main():
    """Loop principal del worker daemon."""
    global shutdown_requested
    
    # Configurar logging
    setup_logging(context="worker")
    
    # Registrar signal handlers
    register_signal_handlers()
    
    logger.info("=" * 80)
    logger.info("DATEADOS BACKGROUND WORKER DAEMON INICIADO")
    logger.info("=" * 80)
    logger.info("Esperando comandos desde la base de datos...")
    logger.info("Presiona Ctrl+C para detener")
    
    poll_interval = 10  # Segundos entre checks
    idle_count = 0
    
    while not shutdown_requested:
        try:
            session = get_session()
            
            # Verificar comandos pendientes
            command_data = check_for_commands(session)
            
            if command_data:
                idle_count = 0
                command = command_data["command"]
                logger.info(f"📨 Comando recibido: {command}")
                
                # Ejecutar comando (cierra la sesión internamente)
                execute_command(command, session)
                
            else:
                session.close()
                idle_count += 1
                
                # Log cada 6 checks (1 minuto) para no saturar logs
                if idle_count % 6 == 0:
                    logger.debug(f"Esperando comandos... ({idle_count * poll_interval}s en idle)")
            
            # Esperar antes del siguiente check
            time.sleep(poll_interval)
            
        except KeyboardInterrupt:
            logger.info("Interrupción de teclado detectada")
            shutdown_requested = True
            
        except Exception as e:
            logger.error(f"Error en loop principal: {e}", exc_info=True)
            time.sleep(30)  # Esperar más tiempo si hay error
    
    # Shutdown graceful
    logger.info("=" * 80)
    logger.info("WORKER DAEMON DETENIDO")
    logger.info("=" * 80)
    sys.exit(0)


if __name__ == "__main__":
    main()
