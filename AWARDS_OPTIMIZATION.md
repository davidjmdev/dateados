# Optimización de Sincronización de Premios

**Fecha de implementación:** 31 de enero de 2026  
**Reducción de llamadas estimada:** 85-90%  
**Tiempo ahorrado estimado:** 80%

---

## 📋 Resumen

Se implementó un sistema de **filtrado inteligente** para la sincronización de premios de jugadores NBA, reduciendo drásticamente el número de llamadas a la API sin sacrificar la actualidad de los datos.

### Problema Original

- **Sincronización indiscriminada**: Todos los jugadores activos (~500) se sincronizaban en cada ejecución
- **Llamadas API excesivas**: 500 jugadores × 0.6s delay = ~5 minutos por ejecución
- **Rate limiting frecuente**: Alto riesgo de bloqueos de IP
- **Procesamiento innecesario**: Jugadores sin actividad reciente se sincronizaban igual

### Solución Implementada

Sistema de **filtrado inteligente temporal y por actividad** con 3 modos de operación:

1. **Temporada de Premios (Abril-Junio)**: Sincroniza todos los jugadores activos
2. **Temporada Regular**: Filtra por actividad reciente (últimos 15 días) + timestamp
3. **Modo Manual Full**: Flag `--full-awards` para sincronizar todos (legacy)

---

## 🔧 Cambios Técnicos Implementados

### 1. Modelo de Datos (db/models.py)

```python
# Nuevo campo en la tabla players
last_award_sync = Column(DateTime, nullable=True, 
                        comment='Última vez que se sincronizaron los premios del jugador')

# Nuevo índice compuesto
Index('idx_players_award_sync_active', 'last_award_sync', 'is_active')
```

### 2. Migración SQL (db/migrations/001_add_last_award_sync.sql)

```sql
ALTER TABLE players ADD COLUMN last_award_sync TIMESTAMP;
CREATE INDEX idx_players_award_sync_active ON players(last_award_sync, is_active);
COMMENT ON COLUMN players.last_award_sync IS 
  'Última vez que se sincronizaron los premios del jugador';
```

### 3. Función de Filtrado Inteligente (ingestion/models_sync.py)

```python
def get_players_needing_award_sync(
    session: Session, 
    force_all: bool = False,
    days_threshold: int = 15
) -> List[int]:
    """
    Retorna IDs de jugadores que necesitan sincronización de premios.
    
    Estrategia:
    - Temporada de premios (Abril-Junio): todos los activos
    - Resto del año: solo activos con actividad reciente Y desactualizados
    
    Returns:
        Lista de IDs de jugadores que necesitan sincronización
    """
```

**Lógica de filtrado:**
- ✅ Jugadores nunca sincronizados (`last_award_sync IS NULL`)
- ✅ Jugadores con `last_award_sync` > 15 días **Y** con partidos en últimos 15 días
- ❌ Jugadores sincronizados recientemente (< 15 días)
- ❌ Jugadores inactivos sin partidos recientes

### 4. Actualización de Timestamps (ingestion/models_sync.py)

```python
# En PlayerAwardsSync.sync_batch() líneas ~460 y ~468
from datetime import datetime
player.awards_synced = True
player.last_award_sync = datetime.now()
```

### 5. Integración en sync_post_process (ingestion/strategies.py)

```python
def sync_post_process(
    self, 
    session: Session, 
    force_full_awards: bool = False,  # NUEVO parámetro
    ...
):
    if force_full_awards:
        # Modo legacy: todos los activos
        player_ids = [...]
    else:
        # Modo inteligente con filtrado
        player_ids = get_players_needing_award_sync(session, days_threshold=15)
```

### 6. CLI Flag (ingestion/cli.py)

```bash
# Modo optimizado (default)
python -m ingestion.cli --mode awards

# Modo legacy (sincroniza todos los activos)
python -m ingestion.cli --mode awards --full-awards
```

---

## 📊 Impacto Esperado

### Escenario 1: Temporada Regular (Octubre-Marzo)
**Antes:**
- 500 jugadores activos × 0.6s = **5 minutos**

**Después (filtrado inteligente):**
- ~50-80 jugadores filtrados × 0.6s = **30-48 segundos**
- **Mejora: 85-90% más rápido**

### Escenario 2: Temporada de Premios (Abril-Junio)
**Antes:**
- 500 jugadores × 0.6s = **5 minutos**

**Después:**
- 500 jugadores × 0.6s = **5 minutos** (mismo comportamiento)
- **Mejora: 0%** (correcto - necesitamos sincronizar todos)

### Escenario 3: Off-season (Julio-Septiembre)
**Antes:**
- 500 jugadores × 0.6s = **5 minutos**

**Después:**
- ~10-20 jugadores activos con torneos/partidos × 0.6s = **6-12 segundos**
- **Mejora: ~95% más rápido**

---

## 🧪 Comandos de Testing

```bash
# Test 1: Modo normal (filtrado inteligente)
python -m ingestion.cli --mode awards

# Test 2: Modo full (legacy, todos los activos)
python -m ingestion.cli --mode awards --full-awards

# Test 3: Smart ingestion (usa filtrado automáticamente)
python -m ingestion.cli --mode smart

# Verificar migración SQL
docker exec nba_postgres psql -U nba -d nba_stats -c "\d players" | grep last_award_sync
docker exec nba_postgres psql -U nba -d nba_stats -c "\di" | grep award_sync

# Test Python directo
python -c "
from db import get_session
from ingestion.models_sync import get_players_needing_award_sync

session = get_session()
player_ids = get_players_needing_award_sync(session, days_threshold=15)
print(f'Jugadores que necesitan sync: {len(player_ids)}')
session.close()
"
```

---

## 📈 Monitoreo y Verificación

### Queries Útiles

```sql
-- Ver jugadores nunca sincronizados
SELECT COUNT(*) FROM players 
WHERE is_active = true AND last_award_sync IS NULL;

-- Ver jugadores sincronizados recientemente
SELECT COUNT(*) FROM players 
WHERE is_active = true 
  AND last_award_sync > NOW() - INTERVAL '15 days';

-- Ver distribución de sincronizaciones
SELECT 
  CASE 
    WHEN last_award_sync IS NULL THEN 'Nunca'
    WHEN last_award_sync > NOW() - INTERVAL '15 days' THEN 'Reciente (<15 días)'
    WHEN last_award_sync > NOW() - INTERVAL '30 days' THEN 'Media (15-30 días)'
    ELSE 'Antigua (>30 días)'
  END AS categoria,
  COUNT(*) as total
FROM players 
WHERE is_active = true
GROUP BY categoria
ORDER BY total DESC;

-- Jugadores con actividad reciente pero sin sincronizar
SELECT p.id, p.full_name, MAX(g.date) as ultimo_partido
FROM players p
JOIN player_game_stats pgs ON p.id = pgs.player_id
JOIN games g ON pgs.game_id = g.id
WHERE p.is_active = true
  AND (p.last_award_sync IS NULL OR p.last_award_sync < NOW() - INTERVAL '15 days')
  AND g.date >= CURRENT_DATE - INTERVAL '15 days'
GROUP BY p.id, p.full_name
ORDER BY ultimo_partido DESC;
```

### Logs a Revisar

Buscar estos mensajes en los logs:

```
✅ "Filtrado inteligente: X jugadores necesitan sync de premios"
✅ "Temporada de premios detectada: sincronizando todos los activos"
✅ "Modo full forzado: sincronizando X jugadores activos"
```

---

## ⚙️ Configuración Avanzada

### Ajustar Ventana Temporal

Para cambiar el umbral de 15 días, modificar en `ingestion/strategies.py`:

```python
player_ids = get_players_needing_award_sync(
    session, 
    days_threshold=30  # Cambiar aquí (de 15 a 30 días)
)
```

### Ajustar Meses de Temporada de Premios

En `ingestion/models_sync.py` función `get_players_needing_award_sync()`:

```python
# Temporada de premios (Abril-Junio): todos los activos
if current_month in [4, 5, 6]:  # Modificar lista de meses aquí
    ...
```

---

## 🐛 Troubleshooting

### Problema: Todos los jugadores se sincronizan siempre

**Causa posible:** Timestamps no se están guardando

**Solución:**
```python
# Verificar en logs que se ejecuta:
player.last_award_sync = datetime.now()

# Query de verificación:
SELECT COUNT(*) FROM players WHERE last_award_sync IS NOT NULL;
```

### Problema: Ningún jugador se sincroniza

**Causa posible:** Filtro demasiado restrictivo

**Solución temporal:**
```bash
# Usar modo full para forzar sincronización
python -m ingestion.cli --mode awards --full-awards
```

### Problema: Jugador con premio nuevo no se detecta

**Causa posible:** No tuvo partidos recientes

**Solución:**
- Los premios importantes (MVP, All-NBA, etc.) se otorgan en Abril-Junio
- En esos meses, el sistema sincroniza **TODOS** los activos automáticamente
- Premios semanales/mensuales solo se otorgan a jugadores con actividad reciente

---

## 🔄 Mantenimiento Futuro

### Limpiar Timestamps Antiguos (Opcional)

```sql
-- Resetear timestamps de jugadores inactivos (liberar recursos)
UPDATE players 
SET last_award_sync = NULL 
WHERE is_active = false 
  AND last_award_sync < NOW() - INTERVAL '1 year';
```

### Re-sincronizar Todos (Mantenimiento)

```bash
# Opción 1: Flag CLI
python -m ingestion.cli --mode awards --full-awards

# Opción 2: SQL manual
UPDATE players SET last_award_sync = NULL WHERE is_active = true;
python -m ingestion.cli --mode awards
```

---

## 📝 Notas Importantes

1. **Primera ejecución**: Todos los jugadores tienen `last_award_sync = NULL`, por lo que la primera sincronización procesa todos. Las siguientes serán mucho más rápidas.

2. **Jugadores retirados**: Los jugadores con `is_active = false` **nunca** se sincronizan, independientemente de `last_award_sync`.

3. **Premios históricos**: Los premios de jugadores ya sincronizados no cambian, por lo que no hay necesidad de re-consultarlos frecuentemente.

4. **Rate limiting**: Aunque se redujeron las llamadas, el delay de 0.6s entre requests se mantiene para respetar los límites de la API.

5. **Compatibilidad**: El flag `--full-awards` permite volver al comportamiento original si es necesario para debugging o mantenimiento.

---

## 🎯 Conclusión

La optimización reduce significativamente el tiempo y las llamadas API necesarias para mantener actualizados los premios de jugadores, mientras mantiene la precisión y actualidad de los datos. El sistema es consciente de la estacionalidad de los premios NBA y ajusta su comportamiento automáticamente.

**Próximos pasos recomendados:**
- Monitorear logs durante las primeras sincronizaciones
- Ajustar `days_threshold` si es necesario según el volumen de premios semanales/mensuales
- Considerar paralelización (Fase 3) si el tiempo de sincronización sigue siendo alto
