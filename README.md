# Dateados - Sistema de Datos NBA

Sistema integral de alto rendimiento para la ingesta, procesamiento, análisis y visualización de datos de la NBA. Diseñado para construir una base de datos histórica exhaustiva desde la temporada 1983-84 hasta la actualidad, con capacidades avanzadas de detección de anomalías, análisis de rachas de rendimiento y gamificación interactiva.

---

## Tabla de Contenidos

1. [Características Principales](#características-principales)
2. [Stack Tecnológico](#stack-tecnológico)
3. [Arquitectura de Base de Datos](#arquitectura-de-base-de-datos)
4. [Estructura del Proyecto](#estructura-del-proyecto)
5. [Instalación y Configuración](#instalación-y-configuración)
6. [Manual de Operaciones](#manual-de-operaciones)
   - [Ingesta de Datos](#ingesta-de-datos)
   - [Sistema de Outliers](#sistema-de-outliers-detección-de-anomalías)
   - [Sistema de Rachas](#sistema-de-rachas)
   - [Utilidades de Base de Datos](#utilidades-de-base-de-datos)
   - [Monitoreo de Logs](#monitoreo-de-logs)
7. [Interfaz Web](#interfaz-web)
8. [Gamificación](#gamificación)
9. [Tests](#tests)
10. [Despliegue](#despliegue)
11. [Estadísticas del Proyecto](#estadísticas-del-proyecto)
12. [Licencia](#licencia)

---

## Características Principales

### Motor de Ingesta Paralelizado

Sistema basado en `multiprocessing` que optimiza la descarga de datos históricos y diarios. Gestiona workers independientes con **staggering automático** para evitar bloqueos de la API de NBA.

**Características técnicas**:
- **Procesamiento paralelo por temporadas**: Múltiples temporadas procesadas simultáneamente
- **Staggering inteligente**: Retrasos escalonados entre workers para distribuir la carga
- **Supervisión de procesos**: Detección y relanzamiento automático de workers fallidos
- **Capacidad dinámica**: Se adapta automáticamente a los cores del CPU disponibles

### Resiliencia y Checkpoints

Arquitectura con **checkpoints automáticos** y sistema de **autoreinicio** (`restart_process`) ante errores fatales o límites de la API.

**Funcionalidades**:
- **Checkpoints cada 10-20 partidos**: Persistencia frecuente del progreso
- **Reanudación inteligente**: Continúa desde el último punto guardado tras fallos
- **Limpieza automática**: Al iniciar nueva ejecución, limpia logs y estados del monitor
- **Retry con backoff exponencial**: Reintentos automáticos con espera progresiva

### Detección de Anomalías (ML + Estadístico)

Sistema de detección de outliers con **tres metodologías complementarias**:

#### 1. Autoencoder (PyTorch)
Detecta partidos estadísticamente anómalos a nivel de **liga** comparando líneas de jugadores contra el universo global.

**Características**:
- **Entrenamiento con pesado temporal**: Mayor peso a temporadas recientes
- **14 features**: pts, ast, reb, stl, blk, tov, pf, fg_pct, fg3_pct, ft_pct, fga, fta, fg3a, min
- **Percentiles de reconstrucción**: Calcula qué tan anómalo es cada partido
- **Contribuciones por feature**: Identifica qué estadísticas causan la anomalía
- **Optimizado para activos**: Solo procesa jugadores activos durante detección

#### 2. Z-Score por Jugador
Identifica **explosiones** (rendimiento excepcional) y **crisis** (rendimiento bajo) comparando contra el historial personal del jugador.

**Características**:
- **12 features**: pts, ast, reb, stl, blk, tov, fga, fta, fg3a, fg_pct, fg3_pct, ft_pct
- **Umbral Z-Score**: 2.0 (2 desviaciones estándar)
- **Mínimo de partidos**: Requiere 10+ partidos históricos para calcular media/desviación
- **Detección de tendencias**: Identifica cambios sostenidos en ventanas de 7 y 30 días
- **Solo jugadores activos**: Enfocado en performance actual

#### 3. Detector de Rachas
Rastrea **rachas de rendimiento** notables en jugadores activos.

**9 tipos de rachas soportadas**:
- **Puntos**: 20+, 30+, 40+ puntos
- **Triple-dobles**: 10+ en 3 categorías
- **Rebotes**: 10+ rebotes
- **Asistencias**: 10+ asistencias
- **Precisión de tiro**: 60%+ FG, 50%+ 3P, 90%+ FT (con mínimo de intentos)

**Características avanzadas**:
- **Soporte multi-competición**: Regular Season, Playoffs, NBA Cup (seguimiento separado)
- **Umbrales dinámicos**: Badges históricos otorgados al 70% del récord absoluto
- **Filtrado por relevancia**: Solo muestra rachas ≥5% del récord para reducir ruido
- **Caché de récords**: Tabla `StreakAllTimeRecord` para comparaciones rápidas
- **Congelamiento inteligente**: Rachas se congelan (no rompen) si el jugador no juega o no tiene intentos suficientes

### Ventanas Temporales
Clasificación automática por **impacto reciente** para facilitar la generación de noticias deportivas:
- **Último partido**: Outliers del partido más reciente
- **Última semana**: Últimos 7 días
- **Último mes**: Últimos 30 días
- **Temporada completa**: Toda la temporada actual

### Interfaz Web

Dashboard moderno desarrollado con **FastAPI**, **Jinja2** y **Tailwind CSS**. Incluye:

- **Navegación completa**: Equipos, jugadores, temporadas, partidos
- **Líderes estadísticos**: Top 10 por categoría (PTS, REB, AST, STL, BLK)
- **Standings dinámicos**: Clasificación con brackets de Playoffs y NBA Cup
- **Panel de administración**: Ejecuta y monitorea ingestas en tiempo real
- **Barras de progreso precisas**: Para todos los procesos (partidos, premios, biografías)
- **Visualizador de outliers**: Dashboard con filtros por tipo y ventana temporal
- **Dashboard de rachas**: Rachas activas, récords históricos y rachas recientemente rotas
- **Box scores completos**: Estadísticas detalladas por partido
- **Historial de compañeros**: Analiza con quién ha jugado cada jugador

### Gamificación - "Alto el Lápiz"

Juego de trivia integrado (tipo **Tutti Frutti/Scattergories**) que utiliza los datos reales de la base de datos para validar conocimientos sobre jugadores de la NBA.

**8 categorías de desafío**:
1. **Campeón**: Jugadores que ganaron un campeonato NBA
2. **All-Star**: Seleccionados al All-Star Game
3. **Lottery Pick**: Drafteados en el top 14
4. **Ambas Conferencias**: Jugaron en Este y Oeste
5. **Premio No-MVP**: Finals MVP, DPOY, ROY, 6MOY, MIP, All-NBA
6. **Compañero Español**: Jugaron con un español
7. **Europeo**: De Europa (excluyendo Turquía/Israel)
8. **Compañero de LeBron**: Jugaron con LeBron James

**Sistema de hints inteligente**:
- Pistas basadas en cantidad de partidos jugados
- Optimización con cláusulas EXISTS para rendimiento
- Validación en tiempo real contra la base de datos

### Monitoreo Avanzado

Sistema de **logging persistente** en base de datos con visualizador por CLI y modo dashboard en tiempo real.

**Características**:
- **Persistencia en BD**: Todos los logs en tabla `log_entries`
- **Modo monitor**: Dashboard en tiempo real con auto-refresh configurable
- **Filtrado por nivel**: ERROR, WARNING, INFO, DEBUG
- **Limpieza programada**: Utilidad `log_cleanup.py` para gestión de retención
- **SQLAlchemy Handler**: Integración transparente con el sistema de logging de Python

---

## Stack Tecnológico

| Categoría | Tecnologías |
|-----------|-------------|
| Lenguaje | Python 3.11+ |
| Web Framework | FastAPI, Jinja2, Tailwind CSS |
| ORM | SQLAlchemy 2.0 |
| Base de Datos | PostgreSQL 18 |
| ML | PyTorch (ROCm 7.1 para AMD GPU) |
| API Externa | nba_api |
| Contenedores | Docker, Docker Compose |
| Testing | pytest |

---

## Arquitectura de Base de Datos

El sistema utiliza **15 tablas** organizadas en tres capas lógicas:

### Capa 1: Datos Core (7 tablas)

#### `teams`
Equipos de la NBA con información organizativa.

**Campos principales**: id, full_name, abbreviation, city, state, conference, division, year_founded

**Índices**: conference + division

#### `players`
Jugadores con biografía completa y control de sincronización.

**Campos principales**: id, full_name, birthdate, height, weight, position, country, is_active, season_exp, draft_year, draft_round, draft_number, school, awards_synced, bio_synced

**Índices**: full_name, position

**Constraints**: weight > 0, season_exp >= 0

#### `games`
Partidos con resultados y marcadores por cuarto.

**Campos principales**: id, date, season, status, home_team_id, away_team_id, home_score, away_score, winner_team_id, quarter_scores (JSON), rs, po, pi, ist

**Índices**: date, season, season + date, home_team_id + away_team_id

**Constraints**: scores >= 0

**JSON Structure** (quarter_scores):
```json
{
  "home": [30, 28, 32, 30],
  "away": [28, 30, 27, 30]
}
```

#### `player_game_stats`
Estadísticas individuales por partido (tabla principal de análisis).

**Campos principales**: id, game_id, player_id, team_id, min, pts, reb, ast, stl, blk, tov, pf, plus_minus, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct

**Índices**: game_id, player_id, team_id, player_id + game_id, team_id + game_id

**Constraints**: 
- Unicidad: (game_id, player_id)
- Validaciones: pts/reb/ast/etc >= 0
- Lógica de tiro: fgm <= fga, fg3m <= fgm, ftm <= fta
- Porcentajes: 0 <= pct <= 1

**Propiedades calculadas**: is_triple_double, is_double_double, minutes_formatted

#### `player_team_seasons`
Estadísticas agregadas por jugador/equipo/temporada/tipo de competición.

**Campos principales**: id, player_id, team_id, season, type, games_played, minutes, pts, reb, ast, stl, blk, tov, pf, fgm, fga, fg3m, fg3a, ftm, fta, plus_minus, is_detailed, start_date, end_date

**Índices**: player_id, team_id, season, type, player_id + season + type, player_id + team_id + season

**Constraints**: Unicidad: (player_id, team_id, season, type)

**Tipos de competición**: Regular Season, Playoffs, NBA Cup, Play-In

#### `team_game_stats`
Estadísticas agregadas del equipo por partido.

**Campos principales**: id, game_id, team_id, total_pts, total_reb, total_ast, total_stl, total_blk, total_tov, total_pf, avg_plus_minus, total_fgm, total_fga, fg_pct, total_fg3m, total_fg3a, fg3_pct, total_ftm, total_fta, ft_pct

**Índices**: game_id, team_id, team_id + game_id

**Constraints**: Unicidad: (game_id, team_id)

#### `player_awards`
Premios y reconocimientos de jugadores.

**Campos principales**: id, player_id, season, award_type, award_name, description

**Índices**: player_id, season, award_type

**Constraints**: Unicidad: (player_id, season, award_type, award_name, description)

**Tipos de premios**: MVP, Champion, Finals MVP, All-Star, All-NBA, All-Defensive, DPOY, ROY, 6MOY, MIP, POTW, POTM

### Capa 2: Sistema de Outliers (5 tablas)

#### `outliers_league` (LeagueOutlier)
Anomalías detectadas por el Autoencoder a nivel de liga.

**Campos principales**: id, player_game_stat_id, reconstruction_error, percentile, feature_contributions (JSON), detection_date, time_window

**Índices**: player_game_stat_id, percentile, time_window, detection_date

**JSON Structure** (feature_contributions):
```json
{
  "pts": 0.35,
  "ast": 0.22,
  "reb": 0.18,
  ...
}
```

#### `outliers_player` (PlayerOutlier)
Explosiones/crisis detectadas por Z-Score.

**Campos principales**: id, player_game_stat_id, outlier_type, z_scores (JSON), detection_date, time_window

**Índices**: player_game_stat_id, outlier_type, time_window

**Tipos**: explosion (rendimiento excepcional), crisis (rendimiento bajo)

#### `outliers_player_trends` (PlayerTrendOutlier)
Cambios sostenidos de rendimiento en ventanas temporales.

**Campos principales**: id, player_id, season, outlier_type, window_days, baseline_avg (JSON), current_avg (JSON), z_scores (JSON), detection_date

**Índices**: player_id, season, window_days

**Ventanas**: 7 días (semana), 30 días (mes)

#### `outliers_player_season_state` (PlayerSeasonState)
Estado acumulado para cálculo O(1) de media/desviación estándar.

**Campos principales**: id, player_id, season, games_count, sum_stats (JSON), sum_squares (JSON), last_updated

**Propósito**: Evitar recalcular estadísticas históricas en cada detección

#### `outliers_streaks` (StreakRecord)
Registro de rachas de rendimiento.

**Campos principales**: id, player_id, streak_type, competition_type, current_count, is_active, start_game_id, last_game_id, broken_game_id, start_date, last_date, broken_date, is_notable, is_historical, created_at

**Índices**: player_id, streak_type, competition_type, is_active, is_notable, is_historical

**Tipos de racha**: pts_20, pts_30, pts_40, triple_double, reb_10, ast_10, fg_pct_60, fg3_pct_50, ft_pct_90

**Estados**: 
- **active**: Racha en curso
- **notable**: Racha ≥5% del récord
- **historical**: Racha ≥70% del récord (badge histórico)

#### `outliers_streak_all_time_records` (StreakAllTimeRecord)
Caché de récords absolutos por tipo de racha y competición.

**Campos principales**: id, streak_type, competition_type, record_count, player_id, start_date, end_date, last_updated

**Propósito**: Comparaciones rápidas sin escanear toda la tabla de rachas

### Capa 3: Sistema y Auditoría (3 tablas)

#### `ingestion_checkpoints`
Checkpoints para ingesta resumible.

**Campos principales**: id, checkpoint_type, checkpoint_key, status, last_game_id, last_player_id, games_processed, error_count, last_error, metadata_json (JSON), created_at, updated_at

**Índices**: checkpoint_type + checkpoint_key, status

**Tipos de checkpoint**: season, awards, daily, boxscore

**Estados**: pending, in_progress, completed, failed

#### `system_status`
Estado de tareas del sistema.

**Campos principales**: task_name (PK), status, progress, message, last_run, updated_at

**Estados**: idle, running, completed, failed

**Tareas**: ingestion, outlier_detection, awards_sync, bio_sync

#### `log_entries`
Logs persistentes del sistema.

**Campos principales**: id, timestamp, level, module, message, traceback

**Índices**: timestamp, level

**Niveles**: DEBUG, INFO, WARNING, ERROR, CRITICAL

### Diagrama de Relaciones

```
teams (1) ──────< (N) player_team_seasons (N) ────── (1) players
  │                           │                            │
  │                           │                            │
  ├──< home_games             │                            ├──< player_awards
  ├──< away_games             │                            │
  │                           │                            ├──< player_game_stats
  │                           │                            │         │
  └──< team_game_stats        │                            │         │
                              │                            │         │
games (1) ───────────────────┴────────────────────────────┘         │
  │                                                                  │
  └──< player_game_stats ──────────────────────────────────────────┘
                │
                ├──< outliers_league
                └──< outliers_player

players (1) ──< outliers_player_trends
players (1) ──< outliers_player_season_state
players (1) ──< outliers_streaks (1) ──> outliers_streak_all_time_records (reference)

ingestion_checkpoints (standalone)
system_status (standalone)
log_entries (standalone)
```

---

## Estructura del Proyecto

```
Dateados/
├── db/                          # Capa de Base de Datos
│   ├── __init__.py              # Exports: models, queries, connections
│   ├── connection.py            # Pool de conexiones y sesiones
│   ├── models.py                # 15 modelos SQLAlchemy (ORM)
│   ├── query.py                 # Consultas optimizadas de alto nivel
│   ├── summary.py               # Generador de resúmenes de estado
│   └── utils/                   # Herramientas de mantenimiento (8 archivos)
│       ├── query_cli.py         # CLI interactivo de consultas
│       ├── view_logs.py         # Visualizador de logs con modo monitor
│       ├── check_db_status.py   # Estado de tareas del sistema
│       ├── clean_database.py    # Limpieza selectiva de datos
│       ├── clean_players.py     # Limpieza de jugadores
│       ├── logging_handler.py   # Handler de logging a BD
│       ├── repair_bios.py       # Reparación de biografías
│       └── log_cleanup.py       # Limpieza de logs antiguos
│
├── ingestion/                   # Pipeline de Datos (ETL)
│   ├── __init__.py
│   ├── cli.py                   # Punto de entrada CLI
│   ├── core.py                  # Lógica de ingesta (Full, Incremental)
│   ├── parallel.py              # Orquestador de multiprocessing
│   ├── api_client.py            # Wrapper de nba_api con backoff
│   ├── models_sync.py           # Sincronización de Biografías y Premios
│   ├── derived_tables.py        # Generación de tablas agregadas
│   ├── checkpoints.py           # Persistencia del progreso
│   ├── config.py                # Configuración de ingesta
│   ├── log_config.py            # Configuración de logging
│   ├── restart.py               # Sistema de autoreinicio
│   ├── nba_static_data.py       # Datos estáticos de NBA
│   └── utils.py                 # Utilidades comunes
│
├── web/                         # Aplicación Web (FastAPI)
│   ├── __init__.py
│   ├── app.py                   # Configuración del servidor
│   ├── pencil_logic.py          # Lógica del juego Alto el Lápiz
│   ├── routes/                  # Controladores por módulo (10 archivos)
│   │   ├── home.py              # Página principal
│   │   ├── players.py           # Jugadores
│   │   ├── teams.py             # Equipos
│   │   ├── seasons.py           # Temporadas y standings
│   │   ├── leaders.py           # Líderes estadísticos
│   │   ├── games.py             # Partidos
│   │   ├── pencil.py            # Juego Alto el Lápiz
│   │   ├── admin.py             # Panel de administración
│   │   ├── outliers.py          # Dashboard de outliers
│   │   └── streaks.py           # Dashboard de rachas
│   ├── templates/               # Vistas Jinja2 (19 archivos)
│   │   ├── base.html            # Template base
│   │   ├── home.html            # Homepage
│   │   ├── components/          # Componentes reutilizables
│   │   ├── admin/               # Templates de admin
│   │   ├── outliers/            # Templates de outliers
│   │   └── streaks/             # Templates de rachas
│   └── static/                  # Assets estáticos
│       └── icon.png             # Icono de la aplicación
│
├── outliers/                    # Sistema de Detección de Anomalías
│   ├── __init__.py              # Exports principales
│   ├── base.py                  # BaseDetector, OutlierResult
│   ├── models.py                # LeagueOutlier, PlayerOutlier, StreakRecord (5 modelos)
│   ├── runner.py                # OutlierRunner, orquestador
│   ├── cli.py                   # CLI de outliers (6 comandos)
│   ├── stats/                   # Métodos estadísticos
│   │   ├── __init__.py
│   │   ├── player_zscore.py     # Detector Z-Score por jugador
│   │   └── streaks.py           # Detector de rachas (9 tipos)
│   └── ml/                      # Machine Learning
│       ├── __init__.py
│       ├── data_pipeline.py     # StandardScaler, preparación de datos
│       ├── autoencoder.py       # Modelo Autoencoder (PyTorch)
│       ├── train.py             # Entrenamiento del modelo
│       ├── inference.py         # Inferencia de outliers
│       └── models/              # Modelos entrenados (.pt, .pkl)
│
├── tests/                       # Suite de pruebas
│   ├── conftest.py              # Fixtures de pytest (10,640 bytes)
│   ├── test_ingest.py           # Tests de ingesta (4,104 bytes)
│   ├── test_models.py           # Tests de modelos (12,206 bytes)
│   ├── test_outliers.py         # Tests de outliers (25,135 bytes, 56+ tests)
│   └── test_utils.py            # Tests de utilidades (13,103 bytes)
│
├── scripts/                     # Scripts de utilidad
│   └── reset_outliers.py        # Resetear tablas de outliers
│
├── docker-compose.yml           # Infraestructura PostgreSQL
├── requirements.txt             # Dependencias Python (base)
├── requirements-ml.txt          # Dependencias ML (PyTorch + ROCm)
├── render.yaml                  # Configuración Render.com
├── AGENTS.md                    # Guía para agentes de IA
└── README.md                    # Este archivo
```

---

## Instalación y Configuración

### 1. Clonar y Preparar Entorno

```bash
git clone <repo-url>
cd Dateados
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Instalar Dependencias ML (Opcional)

Solo si quieres entrenar el modelo Autoencoder localmente:

```bash
pip install -r requirements-ml.txt
```

**Nota**: `requirements-ml.txt` incluye PyTorch optimizado para AMD GPU (ROCm 7.1). Si usas NVIDIA GPU o CPU, modifica el índice en el archivo.

### 3. Levantar Base de Datos

```bash
docker-compose up -d
```

Esto levanta PostgreSQL 18 en el puerto 5432 con datos persistentes en `./postgres_data`.

### 4. Configurar Variables de Entorno

Crear archivo `.env` en la raíz del proyecto:

```env
DATABASE_URL=postgresql://nba:nba@localhost:5432/nba_stats
```

### 5. Inicializar Esquema

```bash
python -m ingestion.cli --init-db
```

Esto crea las **15 tablas** y todos los índices necesarios.

---

## Manual de Operaciones

### Ingesta de Datos

**CLI Principal:** `python -m ingestion.cli`

#### Modos de Ingesta

| Modo | Descripción | Uso Recomendado |
|------|-------------|-----------------|
| `incremental` | Procesa partidos nuevos retrocediendo en el tiempo hasta encontrar uno ya existente y finalizado | **Actualización diaria** |
| `full` | Ingesta histórica completa desde 1983-84 (o rango definido). Sincroniza biografías para todos los jugadores con datos faltantes al finalizar | **Primera carga** o reconstrucción completa |

#### Flujo de Ingesta Incremental

1. Obtiene lista de partidos de las temporadas más recientes (limitado por `--limit-seasons`)
2. Procesa partidos cronológicamente inverso (más recientes primero)
3. **Se detiene** al encontrar un partido ya existente y finalizado
4. Sincroniza premios para jugadores activos
5. Sincroniza biografías faltantes
6. Regenera tablas derivadas (`player_team_seasons`, `team_game_stats`)
7. Ejecuta detección de outliers automáticamente

#### Flujo de Ingesta Full

1. Sincroniza equipos y jugadores base desde la API
2. Divide las temporadas en lotes para procesamiento paralelo
3. Lanza workers independientes por lote (con staggering)
4. Cada worker:
   - Procesa todos los partidos de sus temporadas asignadas
   - Guarda checkpoints cada 20 partidos
   - Regenera tablas derivadas al finalizar cada temporada
5. Al finalizar todos los workers:
   - Sincroniza premios para todos los jugadores sin datos de premios
   - Sincroniza biografías faltantes
   - Verifica sistema de outliers (auto-backfill si está vacío)

#### Ejemplos

```bash
# Ingesta incremental (procesa hasta encontrar partido existente)
python -m ingestion.cli --mode incremental

# Ingesta incremental limitada a las últimas 2 temporadas
python -m ingestion.cli --mode incremental --limit-seasons 2

# Ingesta completa desde 1983-84 hasta la actualidad
python -m ingestion.cli --mode full --start-season 1983-84

# Ingesta de temporadas específicas
python -m ingestion.cli --mode full --start-season 2020-21 --end-season 2023-24

# Reanudar ingesta desde checkpoint guardado
python -m ingestion.cli --mode full --resume

# Solo inicializar BD (sin ingestar datos)
python -m ingestion.cli --init-db
```

#### Parámetros Completos

| Parámetro | Tipo | Default | Descripción |
|-----------|------|---------|-------------|
| `--mode` | choice | (requerido) | `full` o `incremental` |
| `--start-season` | str | `1983-84` | Temporada inicial (modo full) |
| `--end-season` | str | actual | Temporada final (modo full) |
| `--resume` | flag | - | Reanudar desde checkpoint guardado |
| `--limit-seasons` | int | None | Límite de temporadas a procesar (modo incremental) |
| `--init-db` | flag | - | Inicializar esquema antes de ingestar |

---

### Sistema de Outliers (Detección de Anomalías)

**CLI Principal:** `python -m outliers.cli <comando>`

#### Comandos Disponibles

| Comando | Descripción |
|---------|-------------|
| `train` | Entrena el modelo autoencoder con datos históricos |
| `backfill` | Procesa datos históricos para detectar outliers (las 3 metodologías) |
| `top` | Muestra los outliers más extremos (por defecto última semana, solo activos) |
| `stats` | Muestra estadísticas del sistema (totales por tipo, distribución temporal) |
| `validate-model` | Valida que el modelo autoencoder entrenado funcione correctamente |
| `clear` | Limpia datos de outliers (league/player/streaks) |

#### Flujo de Uso Recomendado

##### Primera Configuración

**1. Entrenar modelo Autoencoder** (requiere datos suficientes, recomendado: ≥3 temporadas):

```bash
python -m outliers.cli train --epochs 100 --hidden-dims 64,32,16
```

Esto crea:
- `outliers/ml/models/autoencoder.pt` (pesos del modelo PyTorch)
- `outliers/ml/models/scaler.pkl` (StandardScaler para normalización)

**Parámetros de entrenamiento**:
- `--epochs N`: Número de épocas (default: 50)
- `--hidden-dims DIMS`: Dimensiones de capas ocultas separadas por coma (default: 64,32,16)
- `--experiment`: Activa logging detallado de métricas de entrenamiento

**2. Validar modelo entrenado**:

```bash
python -m outliers.cli validate-model
```

Verifica que el modelo cargue correctamente y genere predicciones válidas.

**3. Procesar histórico** (backfill):

```bash
# Procesar todas las temporadas
python -m outliers.cli backfill

# Procesar solo una temporada específica
python -m outliers.cli backfill --season 2023-24

# Procesar sin algún detector específico
python -m outliers.cli backfill --skip-league  # Sin autoencoder
python -m outliers.cli backfill --skip-player  # Sin Z-Score
python -m outliers.cli backfill --skip-streaks # Sin rachas
```

**Tiempo estimado**: ~10-30 minutos para una temporada completa, dependiendo de:
- Número de partidos
- Si el modelo autoencoder ya está entrenado
- CPU/GPU disponible

##### Uso Diario

**Detección automática**: Los nuevos partidos se analizan automáticamente durante `python -m ingestion.cli --mode incremental`.

**Consulta manual de outliers**:

```bash
# Ver top outliers de la semana (default)
python -m outliers.cli top --limit 20

# Ver top outliers del último mes
python -m outliers.cli top --limit 20 --window month

# Ver top outliers de una temporada completa
python -m outliers.cli top --limit 10 --season 2023-24 --window season

# Ver solo del último partido
python -m outliers.cli top --limit 20 --window last_game
```

**Ventanas disponibles**:
- `last_game`: Solo el partido más reciente
- `week`: Últimos 7 días
- `month`: Últimos 30 días
- `season`: Toda la temporada especificada

#### Ejemplos Avanzados

```bash
# Limpiar todos los outliers para recalcular desde cero
python -m outliers.cli clear --confirm --what all

# Limpiar solo outliers de liga (autoencoder)
python -m outliers.cli clear --confirm --what league

# Limpiar solo outliers de jugador (Z-Score)
python -m outliers.cli clear --confirm --what player

# Limpiar solo rachas
python -m outliers.cli clear --confirm --what streaks

# Ver estadísticas del sistema
python -m outliers.cli stats
```

**Output de `stats`**:
- Total de outliers por tipo (league, player, streaks)
- Distribución por ventana temporal
- Top jugadores con más outliers
- Rachas activas por tipo
- Récords históricos por competición

---

### Sistema de Rachas

Las rachas se detectan automáticamente durante la ingesta incremental, pero también se pueden gestionar manualmente.

#### Ver Rachas Activas

```bash
python -m outliers.cli top --limit 50
```

Esto incluye las rachas activas en la salida.

#### Regenerar Rachas desde Cero

```bash
# 1. Limpiar rachas existentes
python -m outliers.cli clear --confirm --what streaks

# 2. Regenerar solo rachas (sin league/player outliers)
python -m outliers.cli backfill --skip-league --skip-player
```

#### Dashboard Web de Rachas

Acceder a: `http://localhost:8000/streaks`

**Características del dashboard**:
- **Rachas activas**: Ordenadas por duración, con indicadores de notabilidad e historialidad
- **Rachas recientemente rotas**: Últimas 30 rachas terminadas
- **Récords absolutos**: Por tipo de racha y competición
- **Filtros**: Por jugador, tipo de racha, competición
- **Badges visuales**:
  - 🔥 **Racha activa**: En curso
  - ⭐ **Notable**: ≥5% del récord
  - 🏆 **Histórica**: ≥70% del récord

#### Tipos de Rachas y Criterios

| Tipo | Descripción | Criterio | Mínimo de Intentos |
|------|-------------|----------|-------------------|
| `pts_20` | Partidos con 20+ puntos | pts >= 20 | - |
| `pts_30` | Partidos con 30+ puntos | pts >= 30 | - |
| `pts_40` | Partidos con 40+ puntos | pts >= 40 | - |
| `triple_double` | Triple-dobles | 3+ stats con 10+ | - |
| `reb_10` | Partidos con 10+ rebotes | reb >= 10 | - |
| `ast_10` | Partidos con 10+ asistencias | ast >= 10 | - |
| `fg_pct_60` | 60%+ en tiros de campo | fg_pct >= 0.60 | fga >= 5 |
| `fg3_pct_50` | 50%+ en triples | fg3_pct >= 0.50 | fg3a >= 3 |
| `ft_pct_90` | 90%+ en tiros libres | ft_pct >= 0.90 | fta >= 3 |

**Nota sobre congelamiento**: Si un jugador no tiene suficientes intentos (ej: 0 triples intentados), la racha se **congela** en lugar de romperse. Esto previene que rachas de precisión se rompan artificialmente cuando un jugador no intenta tiros de ese tipo.

---

### Utilidades de Base de Datos

#### CLI de Consultas

**Entrada:** `python -m db.utils.query_cli`

##### Consultas Disponibles

```bash
# Resumen de registros en BD
python -m db.utils.query_cli --summary

# Listar equipos
python -m db.utils.query_cli --teams
python -m db.utils.query_cli --teams --conference West
python -m db.utils.query_cli --teams --division Pacific

# Buscar jugadores
python -m db.utils.query_cli --players
python -m db.utils.query_cli --players --name "LeBron"
python -m db.utils.query_cli --players --position G
python -m db.utils.query_cli --players --position F --active-only

# Stats de un jugador específico
python -m db.utils.query_cli --player "LeBron James"
python -m db.utils.query_cli --player "LeBron James" --season 2023-24

# Listar partidos
python -m db.utils.query_cli --games --season 2023-24 --limit 10
python -m db.utils.query_cli --games --team LAL --limit 20
python -m db.utils.query_cli --game 0022300123

# Top jugadores por estadística
python -m db.utils.query_cli --top pts --season 2023-24
python -m db.utils.query_cli --top ast --limit 20
python -m db.utils.query_cli --top reb --season 2023-24 --limit 15
```

**Estadísticas disponibles para `--top`**: pts, reb, ast, stl, blk, fg_pct, fg3_pct, ft_pct

#### Limpieza de Datos

```bash
# Limpiar partidos y stats (preserva equipos/jugadores)
python -m db.utils.clean_database

# Limpiar jugadores sin estadísticas
python -m db.utils.clean_players

# Reparar biografías incompletas
python -m db.utils.repair_bios

# Limpiar logs antiguos (mantiene últimos 30 días por defecto)
python -m db.utils.log_cleanup
```

#### Estado del Sistema

```bash
# Ver estado de tareas (monitor de ingesta/outliers)
python -m db.utils.check_db_status

# Limpiar estados de tareas manualmente (resetear a idle)
python -m db.utils.check_db_status --clear
```

#### Scripts de Utilidad

```bash
# Resetear completamente todas las tablas de outliers
python scripts/reset_outliers.py
```

**Advertencia**: `reset_outliers.py` borra **todos los datos** de las 5 tablas de outliers. Útil para re-backfill completo.

---

### Monitoreo de Logs

**CLI:** `python -m db.utils.view_logs`

#### Modos de Visualización

```bash
# Ver últimos 50 logs (default)
python -m db.utils.view_logs

# Ver más logs
python -m db.utils.view_logs --limit 100
python -m db.utils.view_logs --limit 500

# Filtrar por nivel
python -m db.utils.view_logs --level ERROR
python -m db.utils.view_logs --level WARNING --limit 50
python -m db.utils.view_logs --level INFO --limit 200

# Modo monitor (dashboard en tiempo real)
python -m db.utils.view_logs --monitor
python -m db.utils.view_logs --monitor --interval 1  # Refresh cada 1 segundo
python -m db.utils.view_logs --monitor --interval 5  # Refresh cada 5 segundos
```

#### Modo Monitor

El **modo monitor** (`--monitor`) muestra un dashboard en tiempo real con:
- **Auto-refresh**: Se actualiza automáticamente cada N segundos
- **Color coding**: Errores en rojo, warnings en amarillo, info en verde
- **Estadísticas**: Resumen de logs por nivel
- **Scroll automático**: Siempre muestra los logs más recientes

**Atajos de teclado** (en modo monitor):
- `Ctrl+C`: Salir del monitor

---

## Interfaz Web

### Iniciar Servidor

```bash
# Desarrollo (con reload automático al cambiar código)
uvicorn web.app:app --reload --port 8000

# Producción
uvicorn web.app:app --host 0.0.0.0 --port 8000
```

**Acceso:** http://localhost:8000

### Páginas Disponibles

| Ruta | Descripción |
|------|-------------|
| `/` | Página principal con estadísticas de BD y partidos recientes |
| `/players` | Lista de jugadores con búsqueda, filtros por posición/conferencia y paginación |
| `/players/{id}` | Detalle completo de jugador (estadísticas, carrera, premios, biografía) |
| `/players/{id}/teammates` | Compañeros históricos del jugador (por temporada y equipo) |
| `/teams` | Lista de equipos organizados por conferencia y división |
| `/teams/{id}` | Detalle de equipo (roster actual, récord, partidos recientes) |
| `/seasons` | Redirección a la temporada más reciente |
| `/seasons/{season}` | Standings + Playoffs bracket + NBA Cup bracket |
| `/leaders` | Top 10 líderes estadísticos (PTS, REB, AST, STL, BLK) |
| `/games` | Lista de partidos con filtros por temporada y equipo |
| `/games/{game_id}` | Box score completo del partido con estadísticas detalladas |
| `/pencil` | Juego "Alto el Lápiz" (trivia interactiva) |
| `/admin/ingest` | Panel de administración para ejecutar y monitorear ingestas |
| `/outliers` | Dashboard de detección de anomalías (league + player + trends) |
| `/streaks` | Dashboard de rachas (activas, rotas, récords históricos) |

**Total rutas de páginas**: 15

### API Endpoints

| Ruta | Método | Descripción |
|------|--------|-------------|
| `/api/pencil/validate` | GET | Validar respuesta del jugador en el juego (params: category, answer) |
| `/api/pencil/hint` | GET | Obtener pista inteligente para una categoría (param: category) |
| `/admin/ingest/run` | POST | Iniciar proceso de ingesta incremental en background |
| `/admin/ingest/status` | GET | Obtener estado actual de la ingesta (progress, message, status) |
| `/admin/ingest/logs` | GET | Obtener últimos logs de la ingesta (param: limit, default 50) |
| `/outliers/api/league` | GET | Top outliers de liga en JSON (params: limit, window, season) |
| `/outliers/api/player` | GET | Top outliers de jugador en JSON (params: limit, window, season) |
| `/outliers/api/stats` | GET | Estadísticas del sistema de outliers en JSON |

**Total endpoints de API**: 8

**Total rutas**: **23** (15 páginas + 8 APIs)

### Características Destacadas de la Interfaz

#### Standings y Brackets
- **Clasificación en tiempo real**: Ordenada por wins, porcentaje, división
- **Playoff bracket**: Visualización de rondas (First Round, Semifinals, Conference Finals, Finals)
- **NBA Cup bracket**: Visualización separada del torneo In-Season
- **Parseo automático de Game IDs**: Extrae ronda y posición desde el ID oficial

#### Box Score Completo
- **Estadísticas por jugador**: Minutos, pts, reb, ast, stl, blk, tov, pf, +/-
- **Shooting stats detallados**: FGM-FGA (pct), 3PM-3PA (pct), FTM-FTA (pct)
- **Totales por equipo**: Agregación automática
- **Indicadores visuales**: Triple-dobles, doble-dobles
- **Marcadores por cuarto**: Desglose completo incluyendo overtimes

#### Panel de Admin
- **Inicio de ingesta**: Botón para ejecutar ingesta incremental
- **Progreso en tiempo real**: Barra de progreso actualizada vía polling
- **Logs en vivo**: Stream de logs del proceso
- **Estado de tareas**: Monitoring de todos los procesos del sistema

---

## Gamificación

### "Alto el Lápiz" - Juego de Trivia NBA

Juego de trivia estilo **Tutti Frutti/Scattergories** donde el usuario debe nombrar jugadores que cumplan con criterios específicos basados en la letra inicial sorteada.

#### Mecánica del Juego

1. **Se sortea una letra aleatoria** (excluyendo letras raras como Q, X, Z)
2. **Se presentan 8 categorías** simultáneamente
3. El usuario tiene **tiempo limitado** para ingresar nombres de jugadores
4. **Validación en tiempo real** contra la base de datos
5. **Sistema de puntuación** basado en dificultad de la categoría
6. **Hints disponibles** cuando el usuario no encuentra respuestas

#### 8 Categorías de Desafío

| Categoría | Descripción | Dificultad | Validación |
|-----------|-------------|------------|------------|
| **Campeón** | Jugadores que ganaron un campeonato NBA | ⭐⭐ | Existe premio con `award_type = 'Champion'` |
| **All-Star** | Seleccionados al All-Star Game | ⭐ | Existe premio con `award_type = 'All-Star'` |
| **Lottery Pick** | Drafteados en el top 14 | ⭐⭐ | `draft_number <= 14` |
| **Ambas Conferencias** | Jugaron en Este y Oeste | ⭐⭐⭐ | Existe en `player_team_seasons` con equipos de ambas conferencias |
| **Premio No-MVP** | Ganaron Finals MVP, DPOY, ROY, 6MOY, MIP, o All-NBA | ⭐⭐⭐ | Existe premio con `award_type IN ('Finals MVP', 'DPOY', 'ROY', '6MOY', 'MIP', 'All-NBA', ...)` |
| **Compañero Español** | Jugaron con un jugador español | ⭐⭐⭐⭐ | Comparten `team_id + season` con jugador de `country = 'Spain'` |
| **Europeo** | De Europa (excluyendo Turquía/Israel) | ⭐⭐ | `country IN (lista de países europeos)` |
| **Compañero de LeBron** | Jugaron con LeBron James | ⭐⭐ | Comparten `team_id + season` con `player_id = 2544` |

#### Sistema de Hints

Cuando el usuario solicita una pista, el sistema genera sugerencias inteligentes basadas en:

1. **Cantidad de partidos jugados**: Sugiere jugadores con más presencia
2. **Optimización de consultas**: Usa cláusulas `EXISTS` para rendimiento
3. **Filtrado por letra**: Solo jugadores cuyo apellido empieza con la letra sorteada
4. **Aleatorización**: Orden aleatorio para no revelar siempre los mismos nombres

**Ejemplo de hint para "Compañero Español"**:
```sql
SELECT DISTINCT p.id, p.full_name, COUNT(pg.game_id) as games_played
FROM players p
WHERE p.full_name LIKE 'M%'  -- Letra sorteada: M
  AND EXISTS (
    SELECT 1 FROM player_team_seasons pts1
    JOIN player_team_seasons pts2 
      ON pts1.team_id = pts2.team_id 
      AND pts1.season = pts2.season
    JOIN players spanish ON pts2.player_id = spanish.id
    WHERE pts1.player_id = p.id
      AND spanish.country = 'Spain'
  )
ORDER BY RANDOM()
LIMIT 1
```

#### API de Validación

**Endpoint**: `GET /api/pencil/validate`

**Parámetros**:
- `category`: Nombre de la categoría (ej: "champion")
- `answer`: Nombre del jugador ingresado

**Respuesta**:
```json
{
  "valid": true,
  "player_id": 2544,
  "full_name": "LeBron James",
  "message": "¡Correcto! LeBron James es válido para esta categoría."
}
```

o si es inválido:

```json
{
  "valid": false,
  "message": "El jugador no cumple el criterio o no existe."
}
```

#### Acceso al Juego

**URL**: `http://localhost:8000/pencil`

**Interfaz**:
- Diseño responsivo con Tailwind CSS
- Inputs independientes por categoría
- Validación en tiempo real al presionar Enter
- Botón de hint por categoría
- Timer visual
- Contador de puntos

---

## Tests

### Ejecutar Suite Completa

```bash
# Todos los tests
python -m pytest tests/ -v

# Tests específicos por archivo
python -m pytest tests/test_outliers.py -v
python -m pytest tests/test_ingest.py -v
python -m pytest tests/test_models.py -v
python -m pytest tests/test_utils.py -v

# Ejecutar un test específico
python -m pytest tests/test_outliers.py::TestStandardScaler::test_fit_calculates_mean_and_std -v

# Tests que coincidan con un patrón
python -m pytest tests/ -k "zscore" -v
python -m pytest tests/ -k "streak" -v

# Con cobertura de código
python -m pytest tests/ --cov=outliers --cov=ingestion --cov=db -v
```

### Cobertura por Módulo

| Archivo | Tamaño | Tests | Cobertura |
|---------|--------|-------|-----------|
| `test_outliers.py` | 25,135 bytes | **56+** | Outliers completo |
| `test_models.py` | 12,206 bytes | 15+ | Modelos SQLAlchemy |
| `test_utils.py` | 13,103 bytes | 20+ | Utilidades |
| `test_ingest.py` | 4,104 bytes | 10+ | Parseo e ingesta |
| `conftest.py` | 10,640 bytes | - | Fixtures compartidos |

**Total tests**: **100+ tests automatizados**

### Áreas de Cobertura Detallada

#### `test_outliers.py` (56+ tests)

**StandardScaler (7 tests)**:
- `test_fit_calculates_mean_and_std`: Verifica cálculo correcto de media y desviación
- `test_transform_normalizes_data`: Valida normalización (media=0, std=1)
- `test_fit_transform_combined`: Comprueba equivalencia fit+transform
- `test_inverse_transform_reverses_normalization`: Verifica reversión exacta
- `test_transform_without_fit_raises_error`: Valida error si no se ajustó primero
- `test_handles_zero_std`: Maneja columnas constantes sin NaN/Inf
- `test_stat_features_has_14_elements`: Verifica número correcto de features

**Player Z-Score Detector (8 tests)**:
- `test_zscore_features_list`: Valida lista de 12 features
- `test_zscore_threshold_value`: Verifica umbral = 2.0
- `test_min_games_required_value`: Valida mínimo = 10 partidos
- `test_detector_creation`: Prueba inicialización con threshold custom
- `test_detector_inherits_base`: Verifica herencia de BaseDetector
- Tests de detección de explosiones y crisis
- Tests de ventanas temporales (week, month)

**Streak Detector (12 tests)**:
- `test_streak_criteria_pts_20`: Valida criterio 20+ puntos
- `test_streak_criteria_triple_double`: Prueba detección de triple-dobles
- `test_streak_criteria_fg_pct_60`: Verifica 60%+ FG con mínimo de intentos
- `test_streak_criteria_all_types`: Valida los 9 tipos de racha
- `test_detector_creation_default`: Prueba creación con tipos default
- `test_detector_creation_custom_types`: Valida tipos personalizados
- `test_detector_invalid_type_raises`: Verifica error en tipos inválidos
- `test_detector_inherits_base`: Comprueba herencia
- Tests de umbrales notables dinámicos
- Tests de congelamiento de rachas
- Tests de multi-competición

**Autoencoder (8 tests, condicional a PyTorch)**:
- `test_autoencoder_creation`: Valida inicialización del modelo
- `test_autoencoder_forward_pass`: Prueba forward pass
- `test_autoencoder_encode`: Verifica encoding a dimensión latente
- `test_league_anomaly_detector_creation`: Inicialización del detector
- `test_league_anomaly_detector_train_small`: Entrenamiento con datos sintéticos
- `test_league_anomaly_detector_predict`: Predicción post-entrenamiento
- `test_league_anomaly_detector_is_outlier`: Función de clasificación
- Tests de temporal weighting (get_current_season, get_previous_season, calculate_temporal_weights)

**OutlierRunner (6 tests)**:
- `test_runner_creation_default`: Prueba parámetros default
- `test_runner_creation_custom`: Valida parámetros personalizados
- `test_detection_results_dataclass`: Verifica estructura de resultados
- `test_detection_results_to_dict`: Serialización a diccionario
- `test_runner_detect_empty_list`: Manejo de listas vacías
- Tests de orquestación multi-detector

**Temporal Weighting (7 tests)**:
- `test_get_current_season_format`: Valida formato YYYY-YY
- `test_get_previous_season`: Prueba cálculo de temporada anterior
- `test_calculate_temporal_weights_basic`: Pesos básicos
- `test_calculate_temporal_weights_zero_decay`: decay=0 da pesos iguales
- `test_calculate_temporal_weights_high_decay`: decay alto prioriza recientes
- `test_calculate_temporal_weights_empty_list`: Manejo de lista vacía
- `test_calculate_temporal_weights_with_reference`: Temporada de referencia

**Data Pipeline (3 tests)**:
- `test_interval_to_minutes_conversion`: Conversión timedelta a float
- `test_interval_to_minutes_none`: Manejo de None
- `test_get_feature_names`: Copia inmutable de features

**Exports (5 tests)**:
- Verifica exports del módulo principal
- Valida exports del submódulo stats
- Comprueba exports del runner

#### `test_models.py` (15+ tests)

- Validación de los 15 modelos SQLAlchemy
- Comprobación de relaciones entre tablas
- Tests de constraints (unicidad, checks)
- Validación de propiedades calculadas (is_triple_double, is_finished, etc.)
- Tests de índices compuestos

#### `test_utils.py` (20+ tests)

- `safe_int()` y `safe_float()`: Conversiones seguras con defaults
- `parse_date()`: Múltiples formatos de fecha
- `convert_minutes_to_interval()`: Parseo de minutos (MM:SS, decimal)
- `normalize_season()`: Normalización de formato de temporada
- `get_or_create_player()`: Patrón get-or-create
- Tests de validación de shooting stats

#### `test_ingest.py` (10+ tests)

- Parseo de game IDs (formato corto y largo)
- Deducción de temporada desde fecha
- Validación de datos de API
- Tests de utilidades de ingesta

### Fixtures Disponibles (conftest.py)

**Datos de ejemplo**:
- `sample_team_data`: Lakers, Warriors, etc.
- `sample_player_data`: LeBron James con datos completos
- `sample_game_data`: Partido con scores y quarter breakdown
- `sample_player_stats_data`: Línea estadística completa
- `triple_double_stats`, `double_double_stats`, `quadruple_double_stats`

**Mocks de API**:
- `mock_nba_api_response`: Respuesta genérica
- `mock_boxscore_response`: BoxScoreTraditionalV3
- `mock_game_summary_response`: BoxScoreSummaryV3
- `mock_league_game_finder_response`: LeagueGameFinder

**Game IDs**:
- `regular_season_game_id`: 0022300123
- `playoff_game_id`: 0042300123
- `playin_game_id`: 0052300001
- `ist_game_id`: 0062300001
- `preseason_game_id`: 0012300001
- `allstar_game_id`: 0032300001

**Formatos**:
- `season_formats`: Variaciones de formato de temporada
- `minutes_formats`: Variaciones de formato de minutos
- `date_formats`: Variaciones de formato de fecha
- `edge_case_values`: Valores edge case para conversiones

**Shooting stats**:
- `valid_shooting_stats`: Estadísticas de tiro válidas
- `invalid_shooting_stats`: Casos que requieren corrección

---

## Despliegue

### Render.com (Producción)

El proyecto incluye configuración completa para Render.com en `render.yaml`.

**Servicios definidos**:

1. **Base de datos**: PostgreSQL (Free tier)
   - Nombre: `nba_stats`
   - Plan: Free
   - Versión: PostgreSQL 14+

2. **Aplicación web**: FastAPI (Free tier)
   - Nombre: `dateados-web`
   - Runtime: Python 3.10.12
   - Build: `pip install -r requirements.txt` (sin ML)
   - Pre-deploy: `python -m ingestion.cli --init-db`
   - Start: `uvicorn web.app:app --host 0.0.0.0 --port $PORT`
   - Health check: `/`

**Variables de entorno automáticas**:
- `DATABASE_URL`: URL de conexión PostgreSQL (inyectada por Render)
- `PORT`: Puerto asignado (inyectado por Render)

**Notas**:
- ❌ **No incluye PyTorch** en producción (solo `requirements.txt`, no `requirements-ml.txt`)
- ✅ **Autoencoder pre-entrenado**: Subir modelos `.pt` y `.pkl` al repo
- ✅ **Detección de outliers**: Funcionará con modelo pre-entrenado (solo inferencia)
- ⚠️ **No se puede entrenar** el autoencoder en Render Free (requiere GPU/mucha CPU)

### Docker (Local)

```bash
# Levantar PostgreSQL
docker-compose up -d

# Verificar estado
docker-compose ps

# Ver logs
docker-compose logs postgres
docker-compose logs -f postgres  # Modo follow

# Detener
docker-compose down

# Detener y eliminar volumen (borra datos)
docker-compose down -v
```

**Configuración** (`docker-compose.yml`):
- **Imagen**: PostgreSQL 18 Alpine
- **Puerto**: 5432
- **Usuario/Password**: nba/nba (configurable en `.env`)
- **Base de datos**: nba_stats
- **Persistencia**: Volumen `./postgres_data`
- **Health checks**: `pg_isready` cada 10s

### Variables de Entorno Requeridas

| Variable | Descripción | Ejemplo |
|----------|-------------|---------|
| `DATABASE_URL` | URL completa de conexión PostgreSQL | `postgresql://nba:nba@localhost:5432/nba_stats` |

**Formato de `DATABASE_URL`**:
```
postgresql://[usuario]:[password]@[host]:[puerto]/[database]
```

---

## Estadísticas del Proyecto

### Datos

- **Temporadas cubiertas**: 1983-84 a presente (**42+ temporadas**)
- **Registros estimados**: 1.2M+ estadísticas de jugador
- **Jugadores históricos**: 4,500+
- **Equipos**: 30 actuales + históricos
- **Partidos**: 50,000+ (Regular Season + Playoffs + NBA Cup + Play-In)

### Código

- **Archivos Python**: ~50
- **Líneas de código**: ~20,000+ (estimado)
- **Tests automatizados**: **100+**
- **Cobertura de tests**: ~80% en módulos core

### Base de Datos

- **Tablas totales**: **15**
  - **7** tablas core (teams, players, games, player_game_stats, player_team_seasons, team_game_stats, player_awards)
  - **5** tablas de outliers (outliers_league, outliers_player, outliers_player_trends, outliers_player_season_state, outliers_streaks)
  - **3** tablas de sistema (ingestion_checkpoints, system_status, log_entries)
- **Índices**: 40+ (simples + compuestos)
- **Constraints**: 50+ (checks, unique, foreign keys)

### Interfaz Web

- **Rutas totales**: **23** (15 páginas + 8 APIs)
- **Templates Jinja2**: 19
- **Controladores (routes)**: 10

### Detección de Anomalías

- **Detectores**: 3 (Autoencoder, Z-Score, Streaks)
- **Tipos de outliers**: 3 (league, player, streaks)
- **Tipos de rachas**: **9** (pts_20, pts_30, pts_40, triple_double, reb_10, ast_10, fg_pct_60, fg3_pct_50, ft_pct_90)
- **Ventanas temporales**: 4 (last_game, week, month, season)
- **Features ML**: 14 (para Autoencoder)
- **Features Z-Score**: 12 (para detector de jugador)

### CLI

- **Comandos de ingesta**: 6 (init-db, full, incremental, resume, limit-seasons)
- **Comandos de outliers**: 6 (train, backfill, top, stats, validate-model, clear)
- **Utilidades de DB**: 8 (query_cli, view_logs, check_db_status, clean_database, clean_players, repair_bios, log_cleanup)
- **Scripts**: 1 (reset_outliers.py)
- **Total comandos**: **30+**

### Gamificación

- **Juegos**: 1 ("Alto el Lápiz")
- **Categorías de trivia**: **8**
- **Sistema de validación**: Tiempo real contra BD
- **Sistema de hints**: Optimizado con EXISTS clauses

### Competiciones Soportadas

- **Regular Season** (rs)
- **Playoffs** (po)
- **Play-In Tournament** (pi)
- **NBA Cup / In-Season Tournament** (ist)

**Total**: **4 competiciones**

### Performance

- **Ingesta incremental**: ~5-10 minutos (1-2 temporadas recientes)
- **Ingesta full**: ~8-12 horas (42 temporadas completas, con paralelización)
- **Detección de outliers**: ~10-30 minutos por temporada (backfill)
- **Consultas web**: <100ms para la mayoría de páginas
- **Entrenamiento de Autoencoder**: ~15-30 minutos (100 epochs, CPU)

---

## Licencia

Proyecto privado - Todos los derechos reservados.
