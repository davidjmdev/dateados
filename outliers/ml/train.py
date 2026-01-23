"""CLI para entrenar el autoencoder de detección de outliers.

Uso:
    python -m outliers.ml.train [--epochs N] [--hidden-dims LIST] [--batch-size N]
    
Ejemplo:
    python -m outliers.ml.train --epochs 100 --hidden-dims 64,32,16
"""

import argparse
import logging
import sys
from pathlib import Path

# Asegurar que el proyecto esté en el path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from db.connection import get_session
from outliers.ml.data_pipeline import (
    DataPipeline, 
    get_current_season, 
    get_previous_season,
    calculate_temporal_weights
)
from outliers.ml.autoencoder import (
    LeagueAnomalyDetector,
    DEFAULT_EPOCHS,
    DEFAULT_HIDDEN_DIMS,
    DEFAULT_BATCH_SIZE,
    DEFAULT_LEARNING_RATE,
)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def parse_hidden_dims(dims_str: str) -> list:
    """Parsea string de dimensiones a lista de enteros."""
    return [int(d.strip()) for d in dims_str.split(',')]


def train_model(
    epochs: int = DEFAULT_EPOCHS,
    hidden_dims: list | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    learning_rate: float = DEFAULT_LEARNING_RATE,
    experiment: bool = False,
    limit: int | None = None,
    include_current_season: bool = False,
    decay_rate: float = 0.1,
) -> dict:
    """Entrena el modelo de autoencoder.
    
    Args:
        epochs: Número de épocas de entrenamiento
        hidden_dims: Lista de dimensiones de capas ocultas
        batch_size: Tamaño de batch
        learning_rate: Tasa de aprendizaje
        experiment: Si True, experimenta con múltiples arquitecturas
        limit: Limitar número de muestras (para testing)
        include_current_season: Si True, incluye la temporada actual (riesgo de data leak)
        decay_rate: Tasa de decaimiento para pesos temporales (0 = sin pesos)
        
    Returns:
        Diccionario con métricas de entrenamiento
    """
    if hidden_dims is None:
        hidden_dims = list(DEFAULT_HIDDEN_DIMS)
    
    logger.info("=" * 60)
    logger.info("ENTRENAMIENTO DE AUTOENCODER - DETECCIÓN DE OUTLIERS NBA")
    logger.info("=" * 60)
    
    # Conectar a BD y extraer datos
    logger.info("\n1. Extrayendo datos de la base de datos...")
    session = get_session()
    
    try:
        pipeline = DataPipeline(session)
        
        # Calcular temporada de corte (excluir actual por defecto)
        current_season = get_current_season()
        if include_current_season:
            end_season = None
            logger.info(f"   ADVERTENCIA: Incluyendo temporada actual ({current_season}) - riesgo de data leak")
        else:
            end_season = get_previous_season(current_season)
            logger.info(f"   Excluyendo temporada actual ({current_season}) para evitar data leak")
            logger.info(f"   Entrenando hasta temporada: {end_season}")
        
        # Extraer datos con temporadas
        data, stat_ids, seasons = pipeline.get_all_historical_data(
            end_season=end_season,
            return_seasons=True
        )
        
        if len(data) == 0:
            raise ValueError("No hay datos disponibles para entrenar")
        
        if limit:
            data = data[:limit]
            stat_ids = stat_ids[:limit]
            seasons = seasons[:limit] if seasons else None
            logger.info(f"Limitando a {limit} muestras (modo testing)")
        
        logger.info(f"   Total de muestras: {len(data)}")
        logger.info(f"   Features: {data.shape[1]}")
        
        # Split temporal
        logger.info("\n2. Dividiendo datos (80% train, 20% val)...")
        train_data, val_data, train_ids, val_ids = pipeline.create_train_val_split(
            data, stat_ids, train_ratio=0.8
        )
        
        # Calcular pesos temporales para train
        sample_weights = None
        if decay_rate > 0 and seasons:
            split_idx = int(len(seasons) * 0.8)
            train_seasons = seasons[:split_idx]
            sample_weights = calculate_temporal_weights(
                train_seasons, 
                decay_rate=decay_rate,
                reference_season=end_season or current_season
            )
            logger.info(f"   Pesos temporales calculados (decay_rate={decay_rate})")
            logger.info(f"   Peso mínimo (temporadas antiguas): {sample_weights.min():.4f}")
            logger.info(f"   Peso máximo (temporadas recientes): {sample_weights.max():.4f}")
        
        # Normalizar
        logger.info("\n3. Normalizando datos...")
        scaler = pipeline.fit_scaler(train_data, season=None)
        train_normalized = scaler.transform(train_data)
        val_normalized = scaler.transform(val_data)
        
        if experiment:
            # Experimentar con múltiples arquitecturas
            logger.info("\n4. Ejecutando experimentos con múltiples arquitecturas...")
            architectures = [
                [64, 32, 16],
                [128, 64, 32],
                [32, 16, 8],
                [64, 32],
                [128, 64, 32, 16],
            ]
            
            results = []
            for arch in architectures:
                logger.info(f"\n--- Arquitectura: {arch} ---")
                detector = LeagueAnomalyDetector(
                    input_dim=train_normalized.shape[1],
                    hidden_dims=arch
                )
                
                metrics = detector.train(
                    train_normalized,
                    val_normalized,
                    epochs=epochs,
                    batch_size=batch_size,
                    learning_rate=learning_rate,
                )
                
                results.append({
                    'architecture': arch,
                    'val_loss': metrics['best_val_loss'],
                    'epochs': metrics['epochs_trained'],
                    'threshold': metrics['threshold']
                })
            
            # Encontrar mejor arquitectura
            results.sort(key=lambda x: x['val_loss'])
            hidden_dims = results[0]['architecture']
            logger.info(f"\nMejor arquitectura: {hidden_dims}")
        
        # Entrenar modelo final
        logger.info(f"\n4. Entrenando modelo con arquitectura: {hidden_dims}")
        detector = LeagueAnomalyDetector(
            input_dim=train_normalized.shape[1],
            hidden_dims=hidden_dims
        )
        
        metrics = detector.train(
            train_normalized,
            val_normalized,
            epochs=epochs,
            batch_size=batch_size,
            learning_rate=learning_rate,
            sample_weights=sample_weights,
        )
        
        # Guardar modelo
        logger.info("\n5. Guardando modelo...")
        detector.save()
        
        return metrics
        
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(
        description='Entrena el autoencoder para detección de outliers de liga'
    )
    parser.add_argument(
        '--epochs',
        type=int,
        default=DEFAULT_EPOCHS,
        help=f'Número de épocas (default: {DEFAULT_EPOCHS})'
    )
    parser.add_argument(
        '--hidden-dims',
        type=str,
        default=','.join(map(str, DEFAULT_HIDDEN_DIMS)),
        help=f'Dimensiones de capas ocultas separadas por coma (default: {DEFAULT_HIDDEN_DIMS})'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f'Tamaño de batch (default: {DEFAULT_BATCH_SIZE})'
    )
    parser.add_argument(
        '--learning-rate',
        type=float,
        default=DEFAULT_LEARNING_RATE,
        help=f'Tasa de aprendizaje (default: {DEFAULT_LEARNING_RATE})'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limitar número de muestras (para testing)'
    )
    parser.add_argument(
        '--experiment',
        action='store_true',
        help='Ejecutar experimento con múltiples arquitecturas'
    )
    parser.add_argument(
        '--include-current',
        action='store_true',
        help='Incluir temporada actual (riesgo de data leak, no recomendado)'
    )
    parser.add_argument(
        '--decay-rate',
        type=float,
        default=0.1,
        help='Tasa de decaimiento temporal (0 = sin pesos, default: 0.1)'
    )
    
    args = parser.parse_args()
    hidden_dims = parse_hidden_dims(args.hidden_dims)
    
    logger.info("=" * 60)
    logger.info("ENTRENAMIENTO DE AUTOENCODER - DETECCIÓN DE OUTLIERS NBA")
    logger.info("=" * 60)
    
    # Conectar a BD y extraer datos
    logger.info("\n1. Extrayendo datos de la base de datos...")
    session = get_session()
    
    try:
        pipeline = DataPipeline(session)
        
        # Calcular temporada de corte
        current_season = get_current_season()
        if args.include_current:
            end_season = None
            logger.info(f"   ADVERTENCIA: Incluyendo temporada actual ({current_season}) - riesgo de data leak")
        else:
            end_season = get_previous_season(current_season)
            logger.info(f"   Excluyendo temporada actual ({current_season}) para evitar data leak")
            logger.info(f"   Entrenando hasta temporada: {end_season}")
        
        # Extraer datos con temporadas para pesos
        data, stat_ids, seasons = pipeline.get_all_historical_data(
            end_season=end_season,
            return_seasons=True
        )
        
        if len(data) == 0:
            logger.error("No hay datos disponibles para entrenar")
            return 1
        
        if args.limit:
            data = data[:args.limit]
            stat_ids = stat_ids[:args.limit]
            seasons = seasons[:args.limit] if seasons else None
            logger.info(f"Limitando a {args.limit} muestras (modo testing)")
        
        logger.info(f"   Total de muestras: {len(data)}")
        logger.info(f"   Features: {data.shape[1]}")
        
        # Split temporal
        logger.info("\n2. Dividiendo datos (80% train, 20% val)...")
        train_data, val_data, train_ids, val_ids = pipeline.create_train_val_split(
            data, stat_ids, train_ratio=0.8
        )
        
        # Calcular pesos temporales
        sample_weights = None
        if args.decay_rate > 0 and seasons:
            split_idx = int(len(seasons) * 0.8)
            train_seasons = seasons[:split_idx]
            sample_weights = calculate_temporal_weights(
                train_seasons,
                decay_rate=args.decay_rate,
                reference_season=end_season or current_season
            )
            logger.info(f"   Pesos temporales calculados (decay_rate={args.decay_rate})")
            logger.info(f"   Peso mínimo (temporadas antiguas): {sample_weights.min():.4f}")
            logger.info(f"   Peso máximo (temporadas recientes): {sample_weights.max():.4f}")
        
        # Normalizar
        logger.info("\n3. Normalizando datos...")
        scaler = pipeline.fit_scaler(train_data, season=None)
        train_normalized = scaler.transform(train_data)
        val_normalized = scaler.transform(val_data)
        
        if args.experiment:
            # Experimentar con múltiples arquitecturas
            logger.info("\n4. Ejecutando experimentos con múltiples arquitecturas...")
            architectures = [
                [64, 32, 16],
                [128, 64, 32],
                [32, 16, 8],
                [64, 32],
                [128, 64, 32, 16],
            ]
            
            results = []
            for arch in architectures:
                logger.info(f"\n--- Arquitectura: {arch} ---")
                detector = LeagueAnomalyDetector(
                    input_dim=train_normalized.shape[1],
                    hidden_dims=arch
                )
                
                metrics = detector.train(
                    train_normalized,
                    val_normalized,
                    epochs=args.epochs,
                    batch_size=args.batch_size,
                    learning_rate=args.learning_rate,
                    sample_weights=sample_weights,
                )
                
                results.append({
                    'architecture': arch,
                    'val_loss': metrics['best_val_loss'],
                    'epochs': metrics['epochs_trained'],
                    'threshold': metrics['threshold']
                })
            
            # Mostrar resultados
            logger.info("\n" + "=" * 60)
            logger.info("RESULTADOS DE EXPERIMENTOS")
            logger.info("=" * 60)
            
            results.sort(key=lambda x: x['val_loss'])
            for i, r in enumerate(results):
                marker = " (MEJOR)" if i == 0 else ""
                logger.info(
                    f"{r['architecture']}: val_loss={r['val_loss']:.6f}, "
                    f"epochs={r['epochs']}, threshold={r['threshold']:.6f}{marker}"
                )
            
            # Entrenar modelo final con mejor arquitectura
            best_arch = results[0]['architecture']
            logger.info(f"\n5. Entrenando modelo final con mejor arquitectura: {best_arch}")
            hidden_dims = best_arch
        
        # Entrenar modelo final
        logger.info(f"\n4. Entrenando modelo con arquitectura: {hidden_dims}")
        detector = LeagueAnomalyDetector(
            input_dim=train_normalized.shape[1],
            hidden_dims=hidden_dims
        )
        
        metrics = detector.train(
            train_normalized,
            val_normalized,
            epochs=args.epochs,
            batch_size=args.batch_size,
            learning_rate=args.learning_rate,
            sample_weights=sample_weights,
        )
        
        # Guardar modelo
        logger.info("\n5. Guardando modelo...")
        model_path = detector.save()
        
        # Resumen final
        logger.info("\n" + "=" * 60)
        logger.info("ENTRENAMIENTO COMPLETADO")
        logger.info("=" * 60)
        logger.info(f"   Épocas entrenadas: {metrics['epochs_trained']}")
        logger.info(f"   Loss final (train): {metrics['final_train_loss']:.6f}")
        logger.info(f"   Loss final (val): {metrics['final_val_loss']:.6f}")
        logger.info(f"   Mejor loss (val): {metrics['best_val_loss']:.6f}")
        logger.info(f"   Umbral (p99): {metrics['threshold']:.6f}")
        logger.info(f"   Modelo guardado en: {model_path}")
        
        # Mostrar ejemplos de outliers en datos de validación
        logger.info("\n6. Verificando detección en datos de validación...")
        errors, percentiles, _ = detector.predict(val_normalized)
        
        n_outliers = (percentiles >= 99).sum()
        logger.info(f"   Outliers detectados (p >= 99): {n_outliers} ({100*n_outliers/len(val_normalized):.2f}%)")
        
        # Top 5 outliers
        top_indices = errors.argsort()[-5:][::-1]
        logger.info("\n   Top 5 outliers en validación:")
        for idx in top_indices:
            logger.info(f"   - stat_id={val_ids[idx]}: error={errors[idx]:.4f}, percentil={percentiles[idx]:.1f}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error durante el entrenamiento: {e}", exc_info=True)
        return 1
    finally:
        session.close()


if __name__ == '__main__':
    sys.exit(main())
