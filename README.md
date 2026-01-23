# Dateados - Sistema de Datos NBA

Sistema integral de alto rendimiento para la ingesta, procesamiento, análisis y visualización de datos de la NBA. Diseñado para construir una base de datos histórica exhaustiva desde la temporada 1983-84 hasta la actualidad, con capacidades avanzadas de detección de anomalías y gamificación.

## Tabla de Contenidos

1. [Características Principales](#características-principales)
2. [Stack Tecnológico](#stack-tecnológico)
3. [Estructura del Proyecto](#estructura-del-proyecto)
4. [Instalación y Configuración](#instalación-y-configuración)
5. [Manual de Operaciones](#manual-de-operaciones)
   - [Ingesta de Datos](#ingesta-de-datos)
   - [Sistema de Outliers](#sistema-de-outliers-detección-de-anomalías)
   - [Utilidades de Base de Datos](#utilidades-de-base-de-datos)
   - [Monitoreo de Logs](#monitoreo-de-logs)
6. [Interfaz Web](#interfaz-web)
7. [Tests](#tests)
8. [Despliegue](#despliegue)

---

## Características Principales

### Motor de Ingesta Paralelizado
Sistema basado en `multiprocessing` que optimiza la descarga de datos históricos y diarios. Gestiona workers independientes con staggering para evitar bloqueos de la API de NBA.

### Resiliencia y Checkpoints
Arquitectura con checkpoints automáticos y sistema de autoreinicio (`restart_process`) ante errores fatales o límites de la API. La ingesta puede reanudarse desde el último punto guardado. Al iniciar una nueva ejecución (no reanudada), el sistema limpia automáticamente los logs y los estados del monitor para garantizar una visualización clara.

### Detección de Anomalías (ML + Estadístico)
Sistema de detección de outliers con tres metodologías:
- **Autoencoder (PyTorch)**: Detecta partidos estadísticamente anómalos a nivel de liga (comparando líneas de jugadores contra el universo global) usando entrenamiento con pesado temporal. Optimizado para procesar solo jugadores en activo durante la detección, aunque entrena con todo el histórico.
- **Z-Score por Jugador**: Identifica explosiones y crisis individuales vs. historial personal (solo para jugadores activos).
- **Detector de Rachas**: Rastrea rachas notables (20+ pts, triple-dobles, etc.) en jugadores activos.
- **Ventanas Temporales**: Clasificación automática por impacto reciente (Último partido, última semana, último mes) para facilitar la generación de noticias deportivas.

### Interfaz Web
Dashboard moderno desarrollado con FastAPI, Jinja2 y Tailwind CSS. Incluye:
- Navegación por equipos, jugadores, temporadas y partidos.
- Líderes estadísticos por categoría.
- Standings con brackets de Playoffs y NBA Cup.
- Panel de administración para ejecutar y monitorear ingestas en tiempo real con barras de progreso precisas para todos los procesos (incluyendo sincronización de premios y biografías).
- Visualizador de outliers y rachas.

### Gamificación - "Alto el Lápiz"
Juego de trivia integrado (tipo Tutti Frutti) que utiliza los datos reales de la BD para validar conocimientos sobre jugadores de la NBA.

### Monitoreo Avanzado
Sistema de logging persistente en base de datos con visualizador por CLI y modo dashboard en tiempo real.

---

## Stack Tecnológico

| Categoría | Tecnologías |
|-----------|-------------|
| Lenguaje | Python 3.11+ |
| Web Framework | FastAPI, Jinja2, Tailwind CSS |
| ORM | SQLAlchemy 2.0 |
| Base de Datos | PostgreSQL |
| ML | PyTorch |
| API Externa | nba_api |
| Contenedores | Docker, Docker Compose |
| Testing | pytest |

---

## Estructura del Proyecto

```
Dateados/
├── db/                          # Capa de Base de Datos
│   ├── __init__.py              # Exports: models, queries, connections
│   ├── connection.py            # Pool de conexiones y sesiones
│   ├── models.py                # Modelos SQLAlchemy (ORM)
│   ├── query.py                 # Consultas optimizadas de alto nivel
│   ├── summary.py               # Generador de resúmenes de estado
│   └── utils/                   # Herramientas de mantenimiento
│       ├── query_cli.py         # CLI interactivo de consultas
│       ├── view_logs.py         # Visualizador de logs
│       ├── check_db_status.py   # Estado de tareas del sistema
│       ├── clean_database.py    # Limpieza selectiva de datos
│       ├── clean_players.py     # Limpieza de jugadores
│       └── logging_handler.py   # Handler de logging a BD
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
│   ├── restart.py               # Sistema de autoreinicio
│   └── utils.py                 # Utilidades comunes
│
├── web/                         # Aplicación Web (FastAPI)
│   ├── __init__.py
│   ├── app.py                   # Configuración del servidor
│   ├── pencil_logic.py          # Lógica del juego Alto el Lápiz
│   ├── routes/                  # Controladores por módulo
│   │   ├── home.py              # Página principal
│   │   ├── players.py           # Jugadores
│   │   ├── teams.py             # Equipos
│   │   ├── seasons.py           # Temporadas y standings
│   │   ├── leaders.py           # Líderes estadísticos
│   │   ├── games.py             # Partidos
│   │   ├── pencil.py            # Juego Alto el Lápiz
│   │   ├── admin.py             # Panel de administración
│   │   └── outliers.py          # Dashboard de outliers
│   ├── templates/               # Vistas Jinja2
│   └── static/                  # Assets estáticos
│
├── outliers/                    # Sistema de Detección de Anomalías
│   ├── __init__.py              # Exports principales
│   ├── base.py                  # BaseDetector, OutlierResult
│   ├── models.py                # LeagueOutlier, PlayerOutlier, StreakRecord
│   ├── runner.py                # OutlierRunner, orquestador
│   ├── cli.py                   # CLI de outliers
│   ├── stats/                   # Métodos estadísticos
│   │   ├── player_zscore.py     # Detector Z-Score por jugador
│   │   └── streaks.py           # Detector de rachas
│   └── ml/                      # Machine Learning
│       ├── data_pipeline.py     # StandardScaler, preparación de datos
│       ├── autoencoder.py       # Modelo Autoencoder (PyTorch)
│       ├── train.py             # Entrenamiento del modelo
│       ├── inference.py         # Inferencia de outliers
│       └── models/              # Modelos entrenados (.pt, .pkl)
│
├── tests/                       # Suite de pruebas
│   ├── conftest.py              # Fixtures de pytest
│   ├── test_ingest.py           # Tests de ingesta
│   ├── test_models.py           # Tests de modelos
│   ├── test_outliers.py         # Tests de outliers
│   └── test_utils.py            # Tests de utilidades
│
├── docker-compose.yml           # Infraestructura PostgreSQL
├── requirements.txt             # Dependencias Python
├── render.yaml                  # Configuración Render.com
├── DEPLOY.md                    # Guía de despliegue
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

### 2. Levantar Base de Datos

```bash
docker-compose up -d
```

### 3. Configurar Variables de Entorno

Crear archivo `.env` en la raíz del proyecto:

```env
DATABASE_URL=postgresql://nba:nba@localhost:5432/nba_stats
```

### 4. Inicializar Esquema

```bash
python -m ingestion.cli --init-db
```

---

## Manual de Operaciones

### Ingesta de Datos

**CLI Principal:** `python -m ingestion.cli`

#### Modos de Ingesta

| Modo | Descripción |
|------|-------------|
| `incremental` | Procesa partidos nuevos retrocediendo en el tiempo hasta encontrar uno ya existente y finalizado. |
| `full` | Ingesta histórica completa desde 1983-84 (o rango definido). Sincroniza biografías para todos los jugadores con datos faltantes al finalizar. |

#### Ejemplos

```bash
# Ingesta incremental (procesa hasta encontrar partido existente)
python -m ingestion.cli --mode incremental

# Ingesta incremental limitada a las últimas 2 temporadas
python -m ingestion.cli --mode incremental --limit-seasons 2

# Ingesta completa desde 1983-84
python -m ingestion.cli --mode full --start-season 1983-84

# Ingesta de temporadas específicas
python -m ingestion.cli --mode full --start-season 2020-21 --end-season 2023-24

# Reanudar ingesta desde checkpoint
python -m ingestion.cli --mode full --resume

# Solo inicializar BD (sin ingestar)
python -m ingestion.cli --init-db
```

#### Parámetros Completos

| Parámetro | Tipo | Default | Descripción |
|-----------|------|---------|-------------|
| `--mode` | choice | (requerido) | `full` o `incremental` |
| `--start-season` | str | `1983-84` | Temporada inicial (modo full) |
| `--end-season` | str | actual | Temporada final (modo full) |
| `--resume` | flag | - | Reanudar desde checkpoint |
| `--limit-seasons` | int | None | Límite de temporadas (modo incremental) |
| `--init-db` | flag | - | Inicializar esquema antes de ingestar |

---

### Sistema de Outliers (Detección de Anomalías)

**CLI Principal:** `python -m outliers.cli <comando>`

#### Comandos Disponibles

| Comando | Descripción |
|---------|-------------|
| `train` | Entrena el modelo autoencoder |
| `backfill` | Procesa datos históricos para detectar outliers |
| `top` | Muestra los outliers más extremos (por defecto última semana, solo activos) |
| `stats` | Muestra estadísticas del sistema |
| `validate-model` | Valida que el modelo entrenado funcione |
| `clear` | Limpia datos de outliers |

#### Ejemplos

```bash
# Entrenar modelo autoencoder
python -m outliers.cli train --epochs 100 --hidden-dims 64,32,16

# Procesar datos históricos (backfill)
python -m outliers.cli backfill
python -m outliers.cli backfill --season 2023-24

# Ver top outliers de la semana (por defecto)
python -m outliers.cli top --limit 20

# Ver top outliers del último mes
python -m outliers.cli top --limit 20 --window month

# Ver top outliers de la temporada
python -m outliers.cli top --limit 10 --season 2023-24 --window season

# Ver estadísticas del sistema
python -m outliers.cli stats

# Validar modelo entrenado
python -m outliers.cli validate-model

# Limpiar datos
python -m outliers.cli clear --confirm --what all
python -m outliers.cli clear --confirm --what league
```

#### Flujo de Uso

1. **Entrenar modelo** (una vez, con datos suficientes):
   ```bash
   python -m outliers.cli train --epochs 100
   ```

2. **Procesar histórico** (una vez):
   ```bash
   python -m outliers.cli backfill
   ```

3. **Detección automática**: Los nuevos partidos se analizan automáticamente durante la ingesta incremental.

---

### Utilidades de Base de Datos

#### CLI de Consultas

**Entrada:** `python -m db.utils.query_cli`

```bash
# Resumen de registros en BD
python -m db.utils.query_cli --summary

# Listar equipos
python -m db.utils.query_cli --teams
python -m db.utils.query_cli --teams --conference West

# Buscar jugadores
python -m db.utils.query_cli --players --name "LeBron"
python -m db.utils.query_cli --players --position G --active-only

# Stats de un jugador
python -m db.utils.query_cli --player "LeBron James"
python -m db.utils.query_cli --player "LeBron James" --season 2023-24

# Listar partidos
python -m db.utils.query_cli --games --season 2023-24 --limit 10
python -m db.utils.query_cli --game 0022300123

# Top jugadores por stat
python -m db.utils.query_cli --top pts --season 2023-24
python -m db.utils.query_cli --top ast --limit 20
```

#### Limpieza de Datos

```bash
# Limpiar partidos y stats (preserva equipos/jugadores)
python -m db.utils.clean_database

# Limpiar jugadores
python -m db.utils.clean_players

# Ver estado de tareas (monitor)
python -m db.utils.check_db_status

# Limpiar estados de tareas manualmente
python -m db.utils.check_db_status --clear
```

---

### Monitoreo de Logs

**CLI:** `python -m db.utils.view_logs`

```bash
# Ver últimos 50 logs
python -m db.utils.view_logs

# Ver más logs
python -m db.utils.view_logs --limit 100

# Filtrar por nivel
python -m db.utils.view_logs --level ERROR
python -m db.utils.view_logs --level WARNING --limit 50

# Modo monitor (dashboard en tiempo real)
python -m db.utils.view_logs --monitor
python -m db.utils.view_logs --monitor --interval 1
```

---

## Interfaz Web

### Iniciar Servidor

```bash
# Desarrollo (con reload automático)
uvicorn web.app:app --reload --port 8000

# Producción
uvicorn web.app:app --host 0.0.0.0 --port 8000
```

**Acceso:** http://localhost:8000

### Páginas Disponibles

| Ruta | Descripción |
|------|-------------|
| `/` | Página principal con stats de BD y partidos recientes |
| `/players` | Lista de jugadores (búsqueda, filtros) |
| `/players/{id}` | Detalle de jugador (stats, carrera, premios) |
| `/players/{id}/teammates` | Compañeros históricos de un jugador |
| `/teams` | Lista de equipos por conferencia |
| `/teams/{id}` | Detalle de equipo (roster, récord, partidos) |
| `/seasons` | Temporadas disponibles |
| `/seasons/{season}` | Standings, Playoffs y NBA Cup bracket |
| `/leaders` | Top 10 en PTS, REB, AST, STL, BLK |
| `/games` | Lista de partidos (filtros por temporada, equipo) |
| `/games/{id}` | Box score completo del partido |
| `/pencil` | Juego "Alto el Lápiz" |
| `/admin/ingest` | Panel de control de ingesta |
| `/outliers` | Dashboard de detección de anomalías |

### API Endpoints

| Ruta | Método | Descripción |
|------|--------|-------------|
| `/api/pencil/validate` | GET | Validar respuesta del juego |
| `/api/pencil/hint` | GET | Obtener pista para el juego |
| `/admin/ingest/run` | POST | Ejecutar ingesta incremental |
| `/admin/ingest/status` | GET | Estado de la ingesta en curso |
| `/outliers/api/league` | GET | Top outliers de liga (JSON) |
| `/outliers/api/player` | GET | Top outliers de jugador (JSON) |
| `/outliers/api/streaks` | GET | Rachas activas e históricas (JSON) |
| `/outliers/api/stats` | GET | Estadísticas del sistema (JSON) |

---

## Tests

### Ejecutar Suite Completa

```bash
# Todos los tests
python -m pytest tests/ -v

# Tests específicos
python -m pytest tests/test_outliers.py -v
python -m pytest tests/test_ingest.py -v
python -m pytest tests/test_models.py -v
```

### Cobertura por Módulo

| Módulo | Tests |
|--------|-------|
| `outliers/` | 56 tests (StandardScaler, detectores, runner, temporal weighting) |
| `ingestion/` | Tests de parseo y utilidades |
| `db/models.py` | Tests de modelos SQLAlchemy |

---

## Despliegue

### Render.com

El proyecto incluye configuración para Render.com en `render.yaml`.

### Docker (Local)

```bash
# Levantar PostgreSQL
docker-compose up -d

# Verificar
docker-compose ps
docker-compose logs postgres
```

### Variables de Entorno Requeridas

| Variable | Descripción |
|----------|-------------|
| `DATABASE_URL` | URL de conexión PostgreSQL |

---

## Estadísticas del Proyecto

- **Temporadas cubiertas:** 1983-84 a presente
- **Registros estimados:** 1.2M+ estadísticas de jugador
- **Endpoints web:** 22 (14 páginas + 8 APIs)
- **Detectores de outliers:** 3 (Autoencoder, Z-Score, Streaks)
- **Tests:** 56+ automatizados

---

## Licencia

Proyecto privado - Todos los derechos reservados.
