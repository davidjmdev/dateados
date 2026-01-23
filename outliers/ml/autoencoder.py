"""Autoencoder para detección de outliers a nivel de liga.

Implementa un autoencoder variacional simple que aprende a reconstruir
las estadísticas de jugadores. Los partidos con alto error de reconstrucción
son considerados outliers (rendimientos atípicos respecto a la liga).
"""

import logging
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

from outliers.ml.data_pipeline import MODELS_DIR, get_feature_names

logger = logging.getLogger(__name__)

# Configuración por defecto
DEFAULT_HIDDEN_DIMS = [64, 32, 16]
DEFAULT_EPOCHS = 100
DEFAULT_BATCH_SIZE = 256
DEFAULT_LEARNING_RATE = 1e-3
DEFAULT_EARLY_STOPPING_PATIENCE = 10


class Autoencoder(nn.Module):
    """Red neuronal autoencoder para detección de anomalías."""
    
    def __init__(self, input_dim: int, hidden_dims: List[int]):
        """Inicializa el autoencoder.
        
        Args:
            input_dim: Dimensión de entrada (número de features)
            hidden_dims: Lista de dimensiones de capas ocultas (encoder)
                        El decoder es simétrico
        """
        super().__init__()
        
        self.input_dim = input_dim
        self.hidden_dims = hidden_dims
        
        # Construir encoder
        encoder_layers = []
        prev_dim = input_dim
        for dim in hidden_dims:
            encoder_layers.extend([
                nn.Linear(prev_dim, dim),
                nn.ReLU(),
                nn.BatchNorm1d(dim),
            ])
            prev_dim = dim
        self.encoder = nn.Sequential(*encoder_layers)
        
        # Construir decoder (simétrico)
        decoder_layers = []
        decoder_dims = hidden_dims[::-1][1:] + [input_dim]
        for dim in decoder_dims:
            decoder_layers.extend([
                nn.Linear(prev_dim, dim),
                nn.ReLU() if dim != input_dim else nn.Identity(),
            ])
            prev_dim = dim
        self.decoder = nn.Sequential(*decoder_layers)
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Forward pass del autoencoder."""
        encoded = self.encoder(x)
        decoded = self.decoder(encoded)
        return decoded
    
    def encode(self, x: torch.Tensor) -> torch.Tensor:
        """Obtiene la representación latente."""
        return self.encoder(x)


class LeagueAnomalyDetector:
    """Detector de outliers de liga basado en autoencoder.
    
    Entrena un autoencoder sobre estadísticas históricas de la liga
    y detecta outliers basándose en el error de reconstrucción.
    """
    
    def __init__(
        self,
        input_dim: int = 14,
        hidden_dims: Optional[List[int]] = None,
        device: Optional[str] = None
    ):
        """Inicializa el detector.
        
        Args:
            input_dim: Número de features de entrada
            hidden_dims: Dimensiones de capas ocultas
            device: Dispositivo ('cpu' o 'cuda')
        """
        self.input_dim = input_dim
        self.hidden_dims = hidden_dims or DEFAULT_HIDDEN_DIMS
        self.device = device or ('cuda' if torch.cuda.is_available() else 'cpu')
        
        self.model: Optional[Autoencoder] = None
        self.train_errors: Optional[np.ndarray] = None
        self.threshold_percentile: float = 99.0
        self.threshold_value: Optional[float] = None
        self.version: Optional[str] = None
        self.feature_names = get_feature_names()
    
    def _create_model(self) -> Autoencoder:
        """Crea una nueva instancia del modelo."""
        model = Autoencoder(self.input_dim, self.hidden_dims)
        return model.to(self.device)
    
    def train(
        self,
        train_data: np.ndarray,
        val_data: Optional[np.ndarray] = None,
        epochs: int = DEFAULT_EPOCHS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        learning_rate: float = DEFAULT_LEARNING_RATE,
        early_stopping_patience: int = DEFAULT_EARLY_STOPPING_PATIENCE,
        sample_weights: Optional[np.ndarray] = None,
    ) -> Dict[str, Any]:
        """Entrena el autoencoder.
        
        Args:
            train_data: Datos de entrenamiento normalizados (n_samples, n_features)
            val_data: Datos de validación (opcional)
            epochs: Número de épocas
            batch_size: Tamaño de batch
            learning_rate: Tasa de aprendizaje
            early_stopping_patience: Paciencia para early stopping
            sample_weights: Pesos por muestra para weighted training (opcional)
            
        Returns:
            Diccionario con métricas de entrenamiento
        """
        logger.info(f"Iniciando entrenamiento: {len(train_data)} muestras, {epochs} épocas")
        logger.info(f"Arquitectura: {self.input_dim} -> {self.hidden_dims} -> {self.input_dim}")
        if sample_weights is not None:
            logger.info(f"Weighted training activado (rango de pesos: {sample_weights.min():.3f} - {sample_weights.max():.3f})")
        
        # Crear modelo
        self.model = self._create_model()
        
        # Preparar datos
        train_tensor = torch.FloatTensor(train_data).to(self.device)
        
        if sample_weights is not None:
            weights_tensor = torch.FloatTensor(sample_weights).to(self.device)
            train_dataset = TensorDataset(train_tensor, train_tensor, weights_tensor)
        else:
            train_dataset = TensorDataset(train_tensor, train_tensor)
        
        train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
        
        val_loader = None
        if val_data is not None:
            val_tensor = torch.FloatTensor(val_data).to(self.device)
            val_dataset = TensorDataset(val_tensor, val_tensor)
            val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False)
        
        # Configurar entrenamiento
        optimizer = torch.optim.Adam(self.model.parameters(), lr=learning_rate)
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer, mode='min', factor=0.5, patience=5
        )
        
        # Tracking
        best_val_loss = float('inf')
        best_model_state = None
        patience_counter = 0
        history = {'train_loss': [], 'val_loss': []}
        
        # Entrenamiento
        use_weights = sample_weights is not None
        
        for epoch in range(epochs):
            # Train
            self.model.train()
            train_loss = 0.0
            total_weight = 0.0
            
            for batch in train_loader:
                if use_weights:
                    batch_x, _, batch_w = batch
                else:
                    batch_x, _ = batch
                    batch_w = None
                
                optimizer.zero_grad()
                output = self.model(batch_x)
                
                # Calcular loss (weighted o standard)
                if batch_w is not None:
                    # Weighted MSE: sum(w_i * (x_i - y_i)^2) / sum(w_i)
                    mse_per_sample = ((output - batch_x) ** 2).mean(dim=1)
                    loss = (mse_per_sample * batch_w).sum() / batch_w.sum()
                    train_loss += (mse_per_sample * batch_w).sum().item()
                    total_weight += batch_w.sum().item()
                else:
                    loss = nn.functional.mse_loss(output, batch_x)
                    train_loss += loss.item() * len(batch_x)
                    total_weight += len(batch_x)
                
                loss.backward()
                optimizer.step()
            
            train_loss /= total_weight
            history['train_loss'].append(train_loss)
            
            # Validation (siempre sin pesos, para métricas comparables)
            val_loss = None
            if val_loader is not None:
                self.model.eval()
                val_loss = 0.0
                with torch.no_grad():
                    for batch_x, _ in val_loader:
                        output = self.model(batch_x)
                        loss = nn.functional.mse_loss(output, batch_x)
                        val_loss += loss.item() * len(batch_x)
                val_loss /= len(val_data)
                history['val_loss'].append(val_loss)
                
                # Scheduler
                scheduler.step(val_loss)
                
                # Early stopping
                if val_loss < best_val_loss:
                    best_val_loss = val_loss
                    best_model_state = self.model.state_dict().copy()
                    patience_counter = 0
                else:
                    patience_counter += 1
                    if patience_counter >= early_stopping_patience:
                        logger.info(f"Early stopping en época {epoch + 1}")
                        break
            
            # Log cada 10 épocas
            if (epoch + 1) % 10 == 0:
                val_str = f", val_loss={val_loss:.6f}" if val_loss else ""
                logger.info(f"Época {epoch + 1}/{epochs}: train_loss={train_loss:.6f}{val_str}")
        
        # Restaurar mejor modelo
        if best_model_state is not None:
            self.model.load_state_dict(best_model_state)
        
        # Calcular errores de entrenamiento para umbrales
        self.model.eval()
        with torch.no_grad():
            train_output = self.model(train_tensor)
            self.train_errors = ((train_tensor - train_output) ** 2).mean(dim=1).cpu().numpy()
        
        # Calcular umbral
        self.threshold_value = float(np.percentile(self.train_errors, self.threshold_percentile))
        self.version = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        logger.info(f"Entrenamiento completado. Umbral (p{self.threshold_percentile}): {self.threshold_value:.6f}")
        
        return {
            'epochs_trained': len(history['train_loss']),
            'final_train_loss': history['train_loss'][-1],
            'final_val_loss': history['val_loss'][-1] if history['val_loss'] else None,
            'best_val_loss': best_val_loss if val_data is not None else None,
            'threshold': self.threshold_value,
            'history': history
        }
    
    def predict(
        self,
        data: np.ndarray
    ) -> Tuple[np.ndarray, np.ndarray, List[Dict[str, float]]]:
        """Calcula errores de reconstrucción para nuevos datos.
        
        Args:
            data: Datos normalizados (n_samples, n_features)
            
        Returns:
            Tupla de (errores, percentiles, contribuciones_por_feature)
        """
        if self.model is None:
            raise RuntimeError("Modelo no entrenado. Llama a train() o load() primero.")
        
        self.model.eval()
        with torch.no_grad():
            tensor = torch.FloatTensor(data).to(self.device)
            output = self.model(tensor)
            
            # Error por muestra (MSE)
            errors = ((tensor - output) ** 2).mean(dim=1).cpu().numpy()
            
            # Contribución por feature
            feature_errors = ((tensor - output) ** 2).cpu().numpy()
            
            # Calcular percentiles respecto a distribución de entrenamiento
            if self.train_errors is not None:
                percentiles = np.array([
                    (self.train_errors < err).mean() * 100 
                    for err in errors
                ])
            else:
                percentiles = np.zeros_like(errors)
            
            # Contribuciones normalizadas
            contributions = []
            for i in range(len(data)):
                total_error = feature_errors[i].sum()
                if total_error > 0:
                    contrib = {
                        name: float(feature_errors[i, j] / total_error)
                        for j, name in enumerate(self.feature_names)
                    }
                else:
                    contrib = {name: 0.0 for name in self.feature_names}
                contributions.append(contrib)
        
        return errors, percentiles, contributions
    
    def is_outlier(self, error: float) -> bool:
        """Determina si un error indica outlier."""
        if self.threshold_value is None:
            raise RuntimeError("Umbral no calculado. Entrena el modelo primero.")
        return error >= self.threshold_value
    
    def save(self, path: Optional[Path] = None) -> Path:
        """Guarda el modelo y metadatos.
        
        Args:
            path: Ruta de destino (opcional)
            
        Returns:
            Path donde se guardó el modelo
        """
        if self.model is None:
            raise RuntimeError("No hay modelo para guardar.")
        
        if path is None:
            path = MODELS_DIR / f"autoencoder_{self.version}.pt"
        
        state = {
            'model_state_dict': self.model.state_dict(),
            'input_dim': self.input_dim,
            'hidden_dims': self.hidden_dims,
            'train_errors': self.train_errors,
            'threshold_percentile': self.threshold_percentile,
            'threshold_value': self.threshold_value,
            'version': self.version,
            'feature_names': self.feature_names,
        }
        
        torch.save(state, path)
        logger.info(f"Modelo guardado en {path}")
        
        # Guardar también como "best" si es el caso
        best_path = MODELS_DIR / "autoencoder_best.pt"
        torch.save(state, best_path)
        logger.info(f"Modelo guardado como best en {best_path}")
        
        return path
    
    @classmethod
    def load(cls, path: Optional[Path] = None) -> 'LeagueAnomalyDetector':
        """Carga un modelo guardado.
        
        Args:
            path: Ruta del modelo (usa best por defecto)
            
        Returns:
            Instancia cargada del detector
        """
        if path is None:
            path = MODELS_DIR / "autoencoder_best.pt"
        
        if not path.exists():
            raise FileNotFoundError(f"No se encontró modelo en {path}")
        
        state = torch.load(path, map_location='cpu', weights_only=False)
        
        detector = cls(
            input_dim=state['input_dim'],
            hidden_dims=state['hidden_dims']
        )
        
        detector.model = detector._create_model()
        detector.model.load_state_dict(state['model_state_dict'])
        detector.model.eval()
        
        detector.train_errors = state['train_errors']
        detector.threshold_percentile = state['threshold_percentile']
        detector.threshold_value = state['threshold_value']
        detector.version = state['version']
        detector.feature_names = state.get('feature_names', get_feature_names())
        
        logger.info(f"Modelo cargado desde {path} (versión {detector.version})")
        
        return detector
    
    @classmethod
    def exists(cls) -> bool:
        """Verifica si existe un modelo guardado."""
        return (MODELS_DIR / "autoencoder_best.pt").exists()
