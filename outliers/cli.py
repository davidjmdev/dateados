"""CLI para el sistema de detección de outliers.

Comandos disponibles:
- backfill: Procesar datos históricos
- stats: Mostrar estadísticas del sistema
- clear: Limpiar datos de outliers
- validate-model: Validar el modelo de autoencoder
- train: Entrenar el modelo de autoencoder

Uso:
    python -m outliers.cli backfill [--season SEASON]
    python -m outliers.cli stats
    python -m outliers.cli clear [--confirm]
    python -m outliers.cli validate-model
    python -m outliers.cli train [--epochs N]
"""

import argparse
import logging
import sys
from typing import Optional

from sqlalchemy import func

from db import get_session
from db.models import PlayerGameStats, Game
from outliers.models import LeagueOutlier, PlayerOutlier, StreakRecord, StreakAllTimeRecord, STREAK_HISTORICAL_PERCENTAGE
from outliers.runner import run_backfill, OutlierRunner

try:
    from outliers.ml.autoencoder import LeagueAnomalyDetector
    HAS_AUTOENCODER = True
except ImportError:
    LeagueAnomalyDetector = None
    HAS_AUTOENCODER = False

from outliers.stats.streaks import get_streak_summary

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def cmd_backfill(args: argparse.Namespace) -> int:
    """Ejecuta el backfill de outliers."""
    season = args.season
    skip_league = args.skip_league
    skip_player = args.skip_player
    skip_streaks = args.skip_streaks
    
    logger.info(f"Iniciando backfill de outliers...")
    if season:
        logger.info(f"Temporada: {season}")
    else:
        logger.info("Procesando todas las temporadas")
    
    with get_session() as session:
        results = run_backfill(
            session,
            season=season,
            skip_league=skip_league,
            skip_player=skip_player,
            skip_streaks=skip_streaks
        )
    
    print("\n" + "=" * 50)
    print("RESULTADOS DEL BACKFILL")
    print("=" * 50)
    print(f"Duración: {results.duration_seconds:.2f} segundos")
    print(f"Outliers de liga: {results.league_outliers}")
    print(f"Outliers de jugador: {results.player_outliers}")
    print(f"Rachas notables: {results.streak_outliers}")
    print(f"TOTAL: {results.total_outliers}")
    
    if results.errors:
        print("\nErrores encontrados:")
        for error in results.errors:
            print(f"  - {error}")
        return 1
    
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    """Muestra estadísticas del sistema de outliers."""
    with get_session() as session:
        # Estadísticas generales
        total_games = session.query(func.count(PlayerGameStats.id)).scalar() or 0
        
        # Outliers de liga
        league_total = session.query(func.count(LeagueOutlier.id)).scalar() or 0
        league_outliers = session.query(func.count(LeagueOutlier.id)).filter(
            LeagueOutlier.is_outlier == True
        ).scalar() or 0
        
        # Outliers de jugador
        player_outliers = session.query(func.count(PlayerOutlier.id)).scalar() or 0
        explosions = session.query(func.count(PlayerOutlier.id)).filter(
            PlayerOutlier.outlier_type == 'explosion'
        ).scalar() or 0
        crises = session.query(func.count(PlayerOutlier.id)).filter(
            PlayerOutlier.outlier_type == 'crisis'
        ).scalar() or 0
        
        # Rachas
        total_streaks = session.query(func.count(StreakRecord.id)).scalar() or 0
        active_streaks = session.query(func.count(StreakRecord.id)).filter(
            StreakRecord.is_active == True
        ).scalar() or 0
        notable_streaks = session.query(func.count(StreakRecord.id)).filter(
            StreakRecord.is_historical_outlier == True
        ).scalar() or 0
        
        # Top rachas activas
        top_active = session.query(StreakRecord).filter(
            StreakRecord.is_active == True
        ).order_by(StreakRecord.length.desc()).limit(5).all()
    
    print("\n" + "=" * 60)
    print("ESTADÍSTICAS DEL SISTEMA DE OUTLIERS")
    print("=" * 60)
    
    print(f"\nTotal de líneas estadísticas: {total_games:,}")
    
    print("\n--- OUTLIERS DE LIGA (Autoencoder) ---")
    print(f"Registros procesados: {league_total:,}")
    print(f"Outliers detectados (p99+): {league_outliers:,}")
    if league_total > 0:
        print(f"Porcentaje: {100 * league_outliers / league_total:.2f}%")
    
    print("\n--- OUTLIERS DE JUGADOR (Z-score) ---")
    print(f"Total de outliers: {player_outliers:,}")
    print(f"  - Explosiones (Z > 2.5): {explosions:,}")
    print(f"  - Crisis (Z < -2.5): {crises:,}")
    
    print("\n--- RACHAS ---")
    print(f"Total de rachas: {total_streaks:,}")
    print(f"Rachas activas: {active_streaks:,}")
    print(f"Rachas históricas notables: {notable_streaks:,}")
    
    if top_active:
        print("\nTop 5 rachas activas:")
        with get_session() as session:
            # Obtener récords para mostrar umbrales dinámicos
            from outliers.models import StreakAllTimeRecord
            records = {r.streak_type: r.length for r in session.query(StreakAllTimeRecord).filter(
                StreakAllTimeRecord.competition_type == 'regular' # Por defecto RS para stats
            ).all()}
            
            for streak in top_active:
                all_time = records.get(streak.streak_type, 2)
                threshold = max(2, int(all_time * STREAK_HISTORICAL_PERCENTAGE))
                print(f"  - Player {streak.player_id}: {streak.streak_type} x{streak.length} (umbral histórico: {threshold})")
    
    # Estado del modelo
    print("\n--- MODELO DE AUTOENCODER ---")
    if not HAS_AUTOENCODER:
        print("Estado: No disponible (torch no instalado)")
    elif LeagueAnomalyDetector.exists():
        try:
            detector = LeagueAnomalyDetector.load()
            print(f"Estado: Entrenado")
            print(f"Versión: {detector.version}")
            print(f"Umbral (p99): {detector.threshold_value:.6f}")
        except Exception as e:
            print(f"Estado: Error al cargar - {e}")
    else:
        print("Estado: No entrenado")
    
    return 0


def cmd_clear(args: argparse.Namespace) -> int:
    """Limpia los datos de outliers."""
    if not args.confirm:
        print("ADVERTENCIA: Esta operación eliminará todos los datos de outliers.")
        print("Use --confirm para confirmar la operación.")
        return 1
    
    what = args.what
    
    with get_session() as session:
        if what in ('all', 'league'):
            count = session.query(LeagueOutlier).delete()
            session.commit()
            print(f"Eliminados {count} registros de outliers de liga")
        
        if what in ('all', 'player'):
            count = session.query(PlayerOutlier).delete()
            session.commit()
            print(f"Eliminados {count} registros de outliers de jugador")
        
        if what in ('all', 'streaks'):
            count = session.query(StreakRecord).delete()
            session.commit()
            print(f"Eliminados {count} registros de rachas")
    
    print("Limpieza completada.")
    return 0


def cmd_validate_model(args: argparse.Namespace) -> int:
    """Valida el modelo de autoencoder."""
    print("Validando modelo de autoencoder...")
    
    if not HAS_AUTOENCODER:
        print("ERROR: Sistema de ML no disponible (torch no instalado).")
        return 1
    
    if not LeagueAnomalyDetector.exists():
        print("ERROR: No hay modelo entrenado.")
        print("Ejecute: python -m outliers.cli train")
        return 1
    
    try:
        detector = LeagueAnomalyDetector.load()
        print(f"Modelo cargado correctamente")
        print(f"Versión: {detector.version}")
        print(f"Dimensión de entrada: {detector.input_dim}")
        print(f"Dimensiones ocultas: {detector.hidden_dims}")
        print(f"Umbral (p99): {detector.threshold_value:.6f}")
        
        # Probar con datos sintéticos
        import numpy as np
        test_data = np.random.randn(10, detector.input_dim).astype(np.float32)
        errors, percentiles, contributions = detector.predict(test_data)
        
        print(f"\nTest con datos sintéticos:")
        print(f"  - Errores: min={errors.min():.4f}, max={errors.max():.4f}, mean={errors.mean():.4f}")
        print(f"  - Percentiles: min={percentiles.min():.1f}, max={percentiles.max():.1f}")
        
        print("\nModelo validado correctamente.")
        return 0
        
    except Exception as e:
        print(f"ERROR: {e}")
        return 1


def cmd_train(args: argparse.Namespace) -> int:
    """Entrena el modelo de autoencoder."""
    if not HAS_AUTOENCODER:
        print("ERROR: No se puede entrenar sin torch instalado.")
        return 1
        
    epochs = args.epochs
    hidden_dims = [int(x) for x in args.hidden_dims.split(',')]
    
    print(f"Entrenando modelo de autoencoder...")
    print(f"Épocas: {epochs}")
    print(f"Dimensiones ocultas: {hidden_dims}")
    
    # Importar aquí para evitar imports circulares
    from outliers.ml.train import train_model
    
    try:
        metrics = train_model(
            epochs=epochs,
            hidden_dims=hidden_dims,
            experiment=args.experiment
        )
        
        print("\n" + "=" * 50)
        print("ENTRENAMIENTO COMPLETADO")
        print("=" * 50)
        print(f"Épocas entrenadas: {metrics['epochs_trained']}")
        print(f"Loss final (train): {metrics['final_train_loss']:.6f}")
        print(f"Loss final (val): {metrics['final_val_loss']:.6f}")
        print(f"Umbral p99: {metrics['threshold']:.6f}")
        
        return 0
        
    except Exception as e:
        print(f"ERROR: {e}")
        logger.exception("Error en entrenamiento")
        return 1


def cmd_top_outliers(args: argparse.Namespace) -> int:
    """Muestra los outliers más extremos."""
    limit = args.limit
    season = args.season
    window = args.window
    
    from outliers.ml.inference import get_top_outliers
    
    with get_session() as session:
        outliers = get_top_outliers(session, limit=limit, season=season, window=window)
    
    if not outliers:
        print(f"No se encontraron outliers de liga para la ventana: {window}")
        return 0
    
    print("\n" + "=" * 70)
    print(f"TOP {len(outliers)} OUTLIERS DE LIGA (Ventana: {window})")
    if window == 'season' and season:
        print(f"Temporada: {season}")
    print("=" * 70)
    
    for i, o in enumerate(outliers, 1):
        print(f"\n{i}. {o['player_name']}")
        print(f"   Fecha: {o['game_date']} | Temporada: {o['season']}")
        print(f"   Estadísticas: {o['pts']} pts, {o['reb']} reb, {o['ast']} ast")
        print(f"   Percentil: {o['percentile']:.2f} | Error: {o['reconstruction_error']:.4f}")
        print(f"   Features destacadas: {', '.join(o['top_features'])}")
    
    return 0


def main():
    """Punto de entrada principal."""
    parser = argparse.ArgumentParser(
        description='Sistema de Detección de Outliers NBA',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Comando a ejecutar')
    
    # Comando: backfill
    backfill_parser = subparsers.add_parser('backfill', help='Procesar datos históricos')
    backfill_parser.add_argument('--season', type=str, help='Temporada a procesar (ej: 2023-24)')
    backfill_parser.add_argument('--skip-league', action='store_true', help='Omitir detector de liga')
    backfill_parser.add_argument('--skip-player', action='store_true', help='Omitir detector de jugador')
    backfill_parser.add_argument('--skip-streaks', action='store_true', help='Omitir detector de rachas')
    
    # Comando: stats
    subparsers.add_parser('stats', help='Mostrar estadísticas del sistema')
    
    # Comando: clear
    clear_parser = subparsers.add_parser('clear', help='Limpiar datos de outliers')
    clear_parser.add_argument('--confirm', action='store_true', help='Confirmar la operación')
    clear_parser.add_argument(
        '--what', 
        choices=['all', 'league', 'player', 'streaks'], 
        default='all',
        help='Qué datos limpiar'
    )
    
    # Comando: validate-model
    subparsers.add_parser('validate-model', help='Validar el modelo de autoencoder')
    
    # Comando: train
    train_parser = subparsers.add_parser('train', help='Entrenar el modelo de autoencoder')
    train_parser.add_argument('--epochs', type=int, default=100, help='Número de épocas')
    train_parser.add_argument(
        '--hidden-dims', 
        type=str, 
        default='64,32,16',
        help='Dimensiones ocultas (separadas por coma)'
    )
    train_parser.add_argument(
        '--experiment', 
        action='store_true',
        help='Modo experimento (no guarda modelo)'
    )
    
    # Comando: top
    top_parser = subparsers.add_parser('top', help='Mostrar outliers más extremos')
    top_parser.add_argument('--limit', type=int, default=10, help='Número de resultados')
    top_parser.add_argument('--season', type=str, help='Filtrar por temporada')
    top_parser.add_argument(
        '--window', 
        choices=['last_game', 'week', 'month', 'season'], 
        default='week',
        help='Ventana temporal para los outliers'
    )
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        return 0
    
    # Ejecutar comando
    commands = {
        'backfill': cmd_backfill,
        'stats': cmd_stats,
        'clear': cmd_clear,
        'validate-model': cmd_validate_model,
        'train': cmd_train,
        'top': cmd_top_outliers,
    }
    
    return commands[args.command](args)


if __name__ == '__main__':
    sys.exit(main())
