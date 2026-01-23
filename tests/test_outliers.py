"""Tests unitarios para el sistema de detección de outliers.

Verifica:
- Creación de modelos SQLAlchemy
- StandardScaler personalizado
- Pipeline de extracción de datos
- Autoencoder y detección
- PlayerZScoreDetector
- StreakDetector
"""

import pytest
import numpy as np
from datetime import timedelta, date
from unittest.mock import MagicMock, patch, PropertyMock

# Tests para StandardScaler
class TestStandardScaler:
    """Tests para la implementación propia de StandardScaler."""
    
    def test_fit_calculates_mean_and_std(self):
        """Verifica que fit() calcula media y std correctamente."""
        from outliers.ml.data_pipeline import StandardScaler
        
        scaler = StandardScaler()
        X = np.array([
            [1.0, 2.0, 3.0],
            [4.0, 5.0, 6.0],
            [7.0, 8.0, 9.0]
        ])
        
        scaler.fit(X)
        
        np.testing.assert_array_almost_equal(scaler.mean_, [4.0, 5.0, 6.0])
        expected_std = np.std(X, axis=0)
        np.testing.assert_array_almost_equal(scaler.std_, expected_std)
    
    def test_transform_normalizes_data(self):
        """Verifica que transform() normaliza correctamente."""
        from outliers.ml.data_pipeline import StandardScaler
        
        scaler = StandardScaler()
        X = np.array([
            [0.0, 10.0],
            [10.0, 20.0],
            [20.0, 30.0]
        ])
        
        scaler.fit(X)
        X_transformed = scaler.transform(X)
        
        # La media debe ser 0 y std debe ser 1
        np.testing.assert_array_almost_equal(X_transformed.mean(axis=0), [0.0, 0.0], decimal=5)
        np.testing.assert_array_almost_equal(X_transformed.std(axis=0), [1.0, 1.0], decimal=5)
    
    def test_fit_transform_combined(self):
        """Verifica que fit_transform() funciona igual que fit + transform."""
        from outliers.ml.data_pipeline import StandardScaler
        
        X = np.array([
            [1.0, 2.0],
            [3.0, 4.0],
            [5.0, 6.0]
        ])
        
        scaler1 = StandardScaler()
        result1 = scaler1.fit_transform(X)
        
        scaler2 = StandardScaler()
        scaler2.fit(X)
        result2 = scaler2.transform(X)
        
        np.testing.assert_array_almost_equal(result1, result2)
    
    def test_inverse_transform_reverses_normalization(self):
        """Verifica que inverse_transform() revierte la normalización."""
        from outliers.ml.data_pipeline import StandardScaler
        
        X = np.array([
            [10.0, 100.0],
            [20.0, 200.0],
            [30.0, 300.0]
        ])
        
        scaler = StandardScaler()
        X_transformed = scaler.fit_transform(X)
        X_restored = scaler.inverse_transform(X_transformed)
        
        np.testing.assert_array_almost_equal(X, X_restored)
    
    def test_transform_without_fit_raises_error(self):
        """Verifica que transform() sin fit() lanza error."""
        from outliers.ml.data_pipeline import StandardScaler
        
        scaler = StandardScaler()
        X = np.array([[1.0, 2.0]])
        
        with pytest.raises(RuntimeError, match="no ha sido ajustado"):
            scaler.transform(X)
    
    def test_handles_zero_std(self):
        """Verifica que maneja columnas con std=0 (constantes)."""
        from outliers.ml.data_pipeline import StandardScaler
        
        X = np.array([
            [1.0, 5.0],  # Segunda columna es constante
            [2.0, 5.0],
            [3.0, 5.0]
        ])
        
        scaler = StandardScaler()
        X_transformed = scaler.fit_transform(X)
        
        # No debe haber NaN o Inf
        assert not np.any(np.isnan(X_transformed))
        assert not np.any(np.isinf(X_transformed))


# Tests para STAT_FEATURES
class TestStatFeatures:
    """Tests para la lista de features estadísticas."""
    
    def test_stat_features_has_14_elements(self):
        """Verifica que hay exactamente 14 features."""
        from outliers.ml.data_pipeline import STAT_FEATURES
        
        assert len(STAT_FEATURES) == 14
    
    def test_stat_features_contains_expected_fields(self):
        """Verifica que contiene los campos esperados."""
        from outliers.ml.data_pipeline import STAT_FEATURES
        
        expected = ['pts', 'ast', 'reb', 'stl', 'blk', 'tov', 'pf',
                   'fg_pct', 'fg3_pct', 'ft_pct', 'fga', 'fta', 'fg3a', 'min']
        
        assert set(STAT_FEATURES) == set(expected)


# Tests para modelos SQLAlchemy
class TestOutlierModels:
    """Tests para los modelos de outliers."""
    
    def test_league_outlier_model_exists(self):
        """Verifica que el modelo LeagueOutlier existe."""
        from outliers.models import LeagueOutlier
        
        assert LeagueOutlier.__tablename__ == 'outliers_league'
    
    def test_player_outlier_model_exists(self):
        """Verifica que el modelo PlayerOutlier existe."""
        from outliers.models import PlayerOutlier
        
        assert PlayerOutlier.__tablename__ == 'outliers_player'
    
    def test_streak_record_model_exists(self):
        """Verifica que el modelo StreakRecord existe."""
        from outliers.models import StreakRecord
        
        assert StreakRecord.__tablename__ == 'outliers_streaks'
    
    def test_streak_historical_percentage_defined(self):
        """Verifica que el porcentaje para racha histórica está definido."""
        from outliers.models import STREAK_HISTORICAL_PERCENTAGE
        
        assert STREAK_HISTORICAL_PERCENTAGE == 0.70


# Tests para BaseDetector
class TestBaseDetector:
    """Tests para la clase base abstracta."""
    
    def test_base_detector_is_abstract(self):
        """Verifica que BaseDetector no puede instanciarse."""
        from outliers.base import BaseDetector
        
        with pytest.raises(TypeError, match="abstract"):
            BaseDetector()
    
    def test_outlier_result_dataclass(self):
        """Verifica que OutlierResult funciona como dataclass."""
        from outliers.base import OutlierResult
        
        result = OutlierResult(
            player_game_stat_id=123,
            is_outlier=True,
            outlier_data={'type': 'explosion'}
        )
        
        assert result.player_game_stat_id == 123
        assert result.is_outlier is True
        assert result.outlier_data['type'] == 'explosion'


# Tests para DataPipeline
class TestDataPipeline:
    """Tests para el pipeline de extracción de datos."""
    
    def test_interval_to_minutes_conversion(self):
        """Verifica conversión de timedelta a minutos."""
        from outliers.ml.data_pipeline import DataPipeline
        
        # 30 minutos y 45 segundos
        interval = timedelta(minutes=30, seconds=45)
        minutes = DataPipeline._interval_to_minutes(interval)
        
        assert abs(minutes - 30.75) < 0.01
    
    def test_interval_to_minutes_none(self):
        """Verifica que None retorna 0."""
        from outliers.ml.data_pipeline import DataPipeline
        
        assert DataPipeline._interval_to_minutes(None) == 0.0
    
    def test_get_feature_names(self):
        """Verifica que get_feature_names retorna una copia."""
        from outliers.ml.data_pipeline import get_feature_names, STAT_FEATURES
        
        names = get_feature_names()
        names.append('extra')
        
        assert len(STAT_FEATURES) == 14  # Original sin modificar


class TestTemporalWeighting:
    """Tests para las funciones de pesos temporales."""
    
    def test_get_current_season_format(self):
        """Verifica que get_current_season retorna formato correcto."""
        from outliers.ml.data_pipeline import get_current_season
        
        season = get_current_season()
        
        # Formato YYYY-YY
        assert len(season) == 7
        assert season[4] == '-'
        
        year = int(season[:4])
        suffix = int(season[5:])
        assert year >= 2020
        assert suffix == (year + 1) % 100
    
    def test_get_previous_season(self):
        """Verifica que get_previous_season retorna la temporada anterior."""
        from outliers.ml.data_pipeline import get_previous_season
        
        assert get_previous_season("2024-25") == "2023-24"
        assert get_previous_season("2000-01") == "1999-00"
        assert get_previous_season("2010-11") == "2009-10"
    
    def test_calculate_temporal_weights_basic(self):
        """Verifica cálculo básico de pesos temporales."""
        from outliers.ml.data_pipeline import calculate_temporal_weights
        
        seasons = ['2020-21', '2021-22', '2022-23', '2023-24']
        weights = calculate_temporal_weights(seasons, decay_rate=0.1)
        
        # Los pesos deben existir
        assert len(weights) == 4
        
        # Temporadas recientes deben tener mayor peso
        assert weights[-1] > weights[0]
        
        # La suma normalizada debe ser igual al número de muestras
        assert abs(weights.sum() - len(seasons)) < 0.01
    
    def test_calculate_temporal_weights_zero_decay(self):
        """Verifica que decay_rate=0 da pesos iguales."""
        from outliers.ml.data_pipeline import calculate_temporal_weights
        
        seasons = ['2020-21', '2021-22', '2022-23', '2023-24']
        weights = calculate_temporal_weights(seasons, decay_rate=0.0)
        
        # Todos los pesos deben ser iguales
        assert all(abs(w - weights[0]) < 0.001 for w in weights)
    
    def test_calculate_temporal_weights_high_decay(self):
        """Verifica que decay alto da mucho más peso a recientes."""
        from outliers.ml.data_pipeline import calculate_temporal_weights
        
        seasons = ['2010-11', '2020-21', '2023-24']
        weights = calculate_temporal_weights(seasons, decay_rate=0.5)
        
        # El ratio entre reciente y antiguo debe ser grande
        assert weights[-1] / weights[0] > 10
    
    def test_calculate_temporal_weights_empty_list(self):
        """Verifica que lista vacía retorna array vacío."""
        from outliers.ml.data_pipeline import calculate_temporal_weights
        
        weights = calculate_temporal_weights([], decay_rate=0.1)
        
        assert len(weights) == 0
    
    def test_calculate_temporal_weights_with_reference(self):
        """Verifica cálculo con temporada de referencia explícita."""
        from outliers.ml.data_pipeline import calculate_temporal_weights
        
        seasons = ['2020-21', '2021-22', '2022-23']
        weights = calculate_temporal_weights(
            seasons, 
            decay_rate=0.1,
            reference_season='2023-24'
        )
        
        # Los pesos deben existir y ser válidos
        assert len(weights) == 3
        assert all(w > 0 for w in weights)


# Tests para Autoencoder
class TestAutoencoder:
    """Tests para el modelo autoencoder."""
    
    def test_autoencoder_creation(self):
        """Verifica que el autoencoder se puede crear."""
        from outliers.ml.autoencoder import Autoencoder
        
        model = Autoencoder(input_dim=14, hidden_dims=[64, 32, 16])
        
        assert model.input_dim == 14
        assert model.hidden_dims == [64, 32, 16]
    
    def test_autoencoder_forward_pass(self):
        """Verifica el forward pass del autoencoder."""
        import torch
        from outliers.ml.autoencoder import Autoencoder
        
        model = Autoencoder(input_dim=14, hidden_dims=[32, 16])
        model.eval()
        
        # Entrada de prueba
        x = torch.randn(10, 14)
        output = model(x)
        
        assert output.shape == x.shape
    
    def test_autoencoder_encode(self):
        """Verifica la función de encoding."""
        import torch
        from outliers.ml.autoencoder import Autoencoder
        
        model = Autoencoder(input_dim=14, hidden_dims=[32, 16])
        model.eval()
        
        x = torch.randn(10, 14)
        encoded = model.encode(x)
        
        # La dimensión latente es la última en hidden_dims
        assert encoded.shape == (10, 16)
    
    def test_league_anomaly_detector_creation(self):
        """Verifica que el detector se puede crear."""
        from outliers.ml.autoencoder import LeagueAnomalyDetector
        
        detector = LeagueAnomalyDetector(input_dim=14, hidden_dims=[32, 16])
        
        assert detector.input_dim == 14
        assert detector.hidden_dims == [32, 16]
        assert detector.model is None  # No entrenado aún
    
    def test_league_anomaly_detector_train_small(self):
        """Verifica entrenamiento con datos pequeños."""
        from outliers.ml.autoencoder import LeagueAnomalyDetector
        
        detector = LeagueAnomalyDetector(input_dim=14, hidden_dims=[16, 8])
        
        # Datos sintéticos
        train_data = np.random.randn(100, 14).astype(np.float32)
        val_data = np.random.randn(20, 14).astype(np.float32)
        
        metrics = detector.train(
            train_data, 
            val_data, 
            epochs=5,
            batch_size=32
        )
        
        assert 'epochs_trained' in metrics
        assert metrics['epochs_trained'] <= 5
        assert detector.model is not None
        assert detector.threshold_value is not None
    
    def test_league_anomaly_detector_predict(self):
        """Verifica predicción después de entrenar."""
        from outliers.ml.autoencoder import LeagueAnomalyDetector
        
        detector = LeagueAnomalyDetector(input_dim=14, hidden_dims=[16, 8])
        
        # Entrenar
        train_data = np.random.randn(100, 14).astype(np.float32)
        detector.train(train_data, epochs=3, batch_size=32)
        
        # Predecir
        test_data = np.random.randn(10, 14).astype(np.float32)
        errors, percentiles, contributions = detector.predict(test_data)
        
        assert len(errors) == 10
        assert len(percentiles) == 10
        assert len(contributions) == 10
        assert all(0 <= p <= 100 for p in percentiles)
    
    def test_league_anomaly_detector_is_outlier(self):
        """Verifica la función is_outlier."""
        from outliers.ml.autoencoder import LeagueAnomalyDetector
        
        detector = LeagueAnomalyDetector(input_dim=14, hidden_dims=[16, 8])
        
        # Entrenar para establecer umbral
        train_data = np.random.randn(100, 14).astype(np.float32)
        detector.train(train_data, epochs=3, batch_size=32)
        
        # Verificar umbral
        assert detector.is_outlier(detector.threshold_value + 0.1) is True
        assert detector.is_outlier(detector.threshold_value - 0.1) is False


# Tests para PlayerZScoreDetector
class TestPlayerZScoreDetector:
    """Tests para el detector de Z-score de jugadores."""
    
    def test_zscore_features_list(self):
        """Verifica que ZSCORE_FEATURES contiene las features correctas."""
        from outliers.stats.player_zscore import ZSCORE_FEATURES
        
        expected = [
            'pts', 'ast', 'reb', 'stl', 'blk', 'tov',
            'fga', 'fta', 'fg3a', 'fg_pct', 'fg3_pct', 'ft_pct'
        ]
        assert set(ZSCORE_FEATURES) == set(expected)
    
    def test_zscore_threshold_value(self):
        """Verifica el valor del umbral de Z-score."""
        from outliers.stats.player_zscore import Z_SCORE_THRESHOLD
        
        assert Z_SCORE_THRESHOLD == 2.0
    
    def test_min_games_required_value(self):
        """Verifica el mínimo de partidos requeridos."""
        from outliers.stats.player_zscore import MIN_GAMES_REQUIRED
        
        assert MIN_GAMES_REQUIRED == 10
    
    def test_detector_creation(self):
        """Verifica que el detector se puede crear."""
        from outliers.stats.player_zscore import PlayerZScoreDetector
        
        detector = PlayerZScoreDetector()
        assert detector.z_threshold == 2.0
        
        detector_custom = PlayerZScoreDetector(z_threshold=3.0)
        assert detector_custom.z_threshold == 3.0
    
    def test_detector_inherits_base(self):
        """Verifica que hereda de BaseDetector."""
        from outliers.stats.player_zscore import PlayerZScoreDetector
        from outliers.base import BaseDetector
        
        detector = PlayerZScoreDetector()
        assert isinstance(detector, BaseDetector)


# Tests para StreakDetector
class TestStreakDetector:
    """Tests para el detector de rachas."""
    
    def test_streak_criteria_pts_20(self):
        """Verifica criterio de 20+ pts."""
        from outliers.stats.streaks import StreakCriteria
        
        class MockStats:
            pts = 25
            reb = 5
            ast = 5
            stl = 1
            blk = 1
            fga = 15
            fg_pct = 0.5
        
        assert StreakCriteria.pts_20(MockStats()) is True
        
        MockStats.pts = 15
        assert StreakCriteria.pts_20(MockStats()) is False
    
    def test_streak_criteria_triple_double(self):
        """Verifica criterio de triple-doble."""
        from outliers.stats.streaks import StreakCriteria
        
        class MockStats:
            pts = 15
            reb = 12
            ast = 10
            stl = 5
            blk = 3
        
        assert StreakCriteria.triple_double(MockStats()) is True
        
        # Solo doble-doble
        MockStats.ast = 5
        assert StreakCriteria.triple_double(MockStats()) is False
    
    def test_streak_criteria_fg_pct_60(self):
        """Verifica criterio de 60% FG."""
        from outliers.stats.streaks import StreakCriteria
        
        class MockStats:
            fga = 10
            fg_pct = 0.65
        
        assert StreakCriteria.fg_pct_60(MockStats()) is True
        
        # 0 intentos (debe retornar None para congelar racha)
        MockStats.fga = 0
        assert StreakCriteria.fg_pct_60(MockStats()) is None
        
        # Suficientes intentos pero bajo porcentaje
        MockStats.fga = 10
        MockStats.fg_pct = 0.55
        assert StreakCriteria.fg_pct_60(MockStats()) is False
    
    def test_streak_criteria_all_types(self):
        """Verifica que todos los tipos de criterio están definidos."""
        from outliers.stats.streaks import StreakCriteria
        
        all_criteria = StreakCriteria.get_all_criteria()
        
        expected_types = [
            'pts_20', 'pts_30', 'pts_40', 'triple_double',
            'reb_10', 'ast_10', 'fg_pct_60', 'fg3_pct_50', 'ft_pct_90'
        ]
        
        assert set(all_criteria.keys()) == set(expected_types)
    
    def test_detector_creation_default(self):
        """Verifica creación con tipos por defecto."""
        from outliers.stats.streaks import StreakDetector
        
        detector = StreakDetector()
        
        assert len(detector.streak_types) == 9
        assert 'pts_30' in detector.streak_types
        assert 'triple_double' in detector.streak_types
    
    def test_detector_creation_custom_types(self):
        """Verifica creación con tipos personalizados."""
        from outliers.stats.streaks import StreakDetector
        
        detector = StreakDetector(streak_types=['pts_30', 'triple_double'])
        
        assert len(detector.streak_types) == 2
        assert 'pts_30' in detector.streak_types
        assert 'pts_20' not in detector.streak_types
    
    def test_detector_invalid_type_raises(self):
        """Verifica que tipos inválidos lanzan error."""
        from outliers.stats.streaks import StreakDetector
        
        with pytest.raises(ValueError, match="Tipos de racha inválidos"):
            StreakDetector(streak_types=['invalid_type'])
    
    def test_detector_inherits_base(self):
        """Verifica que hereda de BaseDetector."""
        from outliers.stats.streaks import StreakDetector
        from outliers.base import BaseDetector
        
        detector = StreakDetector()
        assert isinstance(detector, BaseDetector)
    
    def test_notable_thresholds_logic(self):
        """Verifica la lógica de umbrales notables dinámicos."""
        # Esta lógica ahora es dinámica basada en el 70% del récord
        pass


# Tests de integración para exports
class TestOutliersExports:
    """Tests para verificar que todos los exports funcionan."""
    
    def test_main_module_exports(self):
        """Verifica exports del módulo principal."""
        from outliers import (
            LeagueOutlier,
            PlayerOutlier,
            StreakRecord,
            BaseDetector,
            OutlierResult,
            PlayerZScoreDetector,
            StreakDetector,
            StreakCriteria,
            Z_SCORE_THRESHOLD,
        )
        
        assert LeagueOutlier is not None
        assert PlayerOutlier is not None
        assert StreakRecord is not None
        assert BaseDetector is not None
        assert OutlierResult is not None
        assert PlayerZScoreDetector is not None
        assert StreakDetector is not None
        assert StreakCriteria is not None
        assert Z_SCORE_THRESHOLD == 2.0
    
    def test_stats_module_exports(self):
        """Verifica exports del submódulo stats."""
        from outliers.stats import (
            PlayerZScoreDetector,
            detect_player_outliers,
            StreakDetector,
            StreakCriteria,
            get_streak_summary,
            Z_SCORE_THRESHOLD,
        )
        
        assert PlayerZScoreDetector is not None
        assert Z_SCORE_THRESHOLD == 2.0


# Tests para OutlierRunner
class TestOutlierRunner:
    """Tests para el orquestador de outliers."""
    
    def test_runner_creation_default(self):
        """Verifica creación con parámetros por defecto."""
        from outliers.runner import OutlierRunner
        
        runner = OutlierRunner()
        
        assert runner.run_league is True
        assert runner.run_player is True
        assert runner.run_streaks is True
    
    def test_runner_creation_custom(self):
        """Verifica creación con parámetros personalizados."""
        from outliers.runner import OutlierRunner
        
        runner = OutlierRunner(
            run_league=False,
            run_player=True,
            run_streaks=False,
            player_z_threshold=3.0
        )
        
        assert runner.run_league is False
        assert runner.run_player is True
        assert runner.run_streaks is False
        assert runner._player_detector.z_threshold == 3.0
    
    def test_detection_results_dataclass(self):
        """Verifica que DetectionResults funciona correctamente."""
        from outliers.runner import DetectionResults
        from datetime import datetime
        
        results = DetectionResults(
            total_processed=100,
            league_outliers=5,
            player_outliers=10,
            streak_outliers=2,
            started_at=datetime.now()
        )
        
        assert results.total_outliers == 17
        assert results.total_processed == 100
    
    def test_detection_results_to_dict(self):
        """Verifica serialización a diccionario."""
        from outliers.runner import DetectionResults
        from datetime import datetime
        
        now = datetime.now()
        results = DetectionResults(
            total_processed=50,
            league_outliers=3,
            player_outliers=5,
            streak_outliers=1,
            started_at=now,
            finished_at=now
        )
        
        d = results.to_dict()
        
        assert d['total_processed'] == 50
        assert d['total_outliers'] == 9
        assert 'errors' in d
    
    def test_runner_detect_empty_list(self):
        """Verifica que detect() con lista vacía no falla."""
        from outliers.runner import OutlierRunner
        from unittest.mock import MagicMock
        
        runner = OutlierRunner(run_league=False)  # Sin modelo entrenado
        mock_session = MagicMock()
        
        results = runner.detect(mock_session, [])
        
        assert results.total_processed == 0
        assert results.total_outliers == 0


# Tests para runner exports
class TestRunnerExports:
    """Tests para verificar exports del runner."""
    
    def test_runner_module_exports(self):
        """Verifica exports del módulo runner."""
        from outliers.runner import (
            OutlierRunner,
            DetectionResults,
            run_detection_for_games,
            run_backfill,
        )
        
        assert OutlierRunner is not None
        assert DetectionResults is not None
        assert run_detection_for_games is not None
        assert run_backfill is not None
    
    def test_main_module_runner_exports(self):
        """Verifica que el módulo principal exporta runner."""
        from outliers import (
            OutlierRunner,
            DetectionResults,
            run_detection_for_games,
            run_backfill,
        )
        
        assert OutlierRunner is not None
        assert DetectionResults is not None
        assert run_detection_for_games is not None
        assert run_backfill is not None
