# Dateados - Sistema de Datos NBA

![Python](https://img.shields.io/badge/python-3.11+-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.109+-green.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-blue.svg)
![PyTorch](https://img.shields.io/badge/PyTorch-2.0+-red.svg)
![Tests](https://img.shields.io/badge/tests-215-brightgreen.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

Sistema integral de alto rendimiento para la ingesta, procesamiento, análisis y visualización de datos de la NBA. Construye una base de datos histórica exhaustiva desde la temporada 1983-84 hasta la actualidad, con capacidades avanzadas de detección de anomalías mediante machine learning, análisis de rachas de rendimiento y dashboard web interactivo.

---

## 📚 Tabla de Contenidos

1. [Características Principales](#-características-principales)
2. [Quick Start](#-quick-start)
3. [Arquitectura](#-arquitectura)
4. [Model Context Protocol (MCP) Server](#-model-context-protocol-mcp-server)
5. [Guía de Uso](#-guía-de-uso)
   - [Ingesta de Datos](#1️⃣-ingesta-de-datos)
   - [Sistema de Outliers](#2️⃣-sistema-de-outliers)
   - [Interfaz Web](#3️⃣-interfaz-web)
   - [Utilidades](#4️⃣-utilidades)
6. [Testing](#-testing)
7. [Despliegue](#-despliegue)
8. [Automatización](#-automatización)
9. [Troubleshooting](#-troubleshooting)
10. [Estadísticas del Proyecto](#-estadísticas-del-proyecto)

---

## 🎯 Características Principales

### ⚡ Ingesta Inteligente con Procesamiento Paralelo
- **Estrategia Smart** (por defecto): Detecta automáticamente huecos en la BD y combina carga incremental + masiva
- **Procesamiento paralelo**: Workers independientes con staggering automático para evitar rate limits
- **Supervisión automática**: Detecta y relanza workers fallidos sin intervención manual
- **Checkpoints frecuentes**: Guarda progreso cada 10-20 partidos para reinicio seguro
- **Resiliencia total**: Sistema de autoreinicio ante errores fatales de API

### 🔬 Detección de Anomalías Multi-Metodología
Sistema de outliers con **tres detectores complementarios**:

1. **Autoencoder (PyTorch)**: Detecta anomalías a nivel de liga comparando contra el universo global
   - 14 features normalizadas
   - Pesado temporal (más importancia a temporadas recientes)
   - Percentiles de reconstrucción para ranking

2. **Z-Score por Jugador**: Identifica explosiones y crisis comparando contra historial personal
   - 12 features estadísticas
   - Umbral de 2.0 desviaciones estándar
   - Detección de tendencias en ventanas de 7 y 30 días

3. **Detector de Rachas**: Rastrea 9 tipos de rachas de rendimiento
   - Rachas de puntos (20+, 30+, 40+)
   - Triple-dobles, rebotes, asistencias
   - Precisión de tiro (FG 60%, 3P 50%, FT 90%)
   - Soporte multi-competición (RS, PO, NBA Cup, Play-In)
   - Congelamiento inteligente (no rompe si no juega)

### 🎨 Dashboard Web Interactivo
- **FastAPI** con templates Jinja2 y Tailwind CSS
- Navegación completa: jugadores, equipos, temporadas, partidos
- Líderes estadísticos en tiempo real
- Panel de administración con monitoreo de ingestas
- Visualizadores de outliers y rachas con filtros avanzados
- Box scores completos con estadísticas detalladas

### 📊 Sistema de Logging Centralizado
- **Persistencia dual**: Consola + tabla `log_entries` en PostgreSQL
- **Modo monitor**: Dashboard de logs en tiempo real con auto-refresh
- **Filtrado avanzado**: Por nivel (DEBUG, INFO, WARNING, ERROR)
- **Handler SQLAlchemy**: Integración transparente con Python logging

### 🤖 Automatización y CI/CD
- **GitHub Actions**: Ingesta diaria automática a las 07:00 UTC
- **Actualización sin intervención**: Captura partidos de la noche anterior
- **Compatible con Render Free Tier**: Mantiene la BD actualizada incluso con servicio en reposo

---

## ⚡ Quick Start

### Requisitos Previos

- **Python 3.11+**
- **PostgreSQL 18** (o Docker)
- **Git**

### Instalación en 5 Pasos

**1. Clonar el repositorio**
```bash
git clone <repo-url>
cd Dateados
```

**2. Crear y activar entorno virtual**
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate    # Windows
```

**3. Instalar dependencias**
```bash
# Dependencias base (ingesta + web)
pip install -r requirements.txt

# Opcional: ML dependencies para entrenar autoencoder localmente
pip install -r requirements-ml.txt
```

**4. Configurar base de datos**
```bash
# Opción A: Docker (recomendado)
docker-compose up -d

# Opción B: PostgreSQL local
# Crear base de datos 'nba_stats' manualmente
```

Crear archivo `.env` en la raíz:
```env
DATABASE_URL=postgresql://nba:nba@localhost:5432/nba_stats
```

**5. Inicializar esquema**
```bash
python -m ingestion.cli --init-db
```

### Primer Uso

**Cargar datos de la temporada actual**
```bash
# Modo smart: detecta automáticamente qué falta y lo carga
python -m ingestion.cli
```

Este comando:
- ✅ Sincroniza equipos y jugadores base
- ✅ Analiza el estado de la BD
- ✅ Carga solo los partidos faltantes (incremental inteligente)
- ✅ Sincroniza premios y biografías
- ✅ Ejecuta detección de outliers automáticamente

**Iniciar interfaz web**
```bash
uvicorn web.app:app --reload --port 8000
```

Acceder a: http://localhost:8000

### Verificación

```bash
# Ver resumen de la base de datos
python -m db.utils.query_cli --summary

# Ver últimos logs
python -m db.utils.view_logs --limit 50

# Ver estado del sistema
python -m db.utils.check_db_status
```

---

## 🏗️ Arquitectura

### Stack Tecnológico

| Categoría | Tecnologías |
|-----------|-------------|
| **Lenguaje** | Python 3.11+ |
| **Web Framework** | FastAPI, Jinja2, Tailwind CSS |
| **ORM** | SQLAlchemy 2.0 |
| **Base de Datos** | PostgreSQL 18 |
| **Machine Learning** | PyTorch (con soporte ROCm 7.1 para AMD GPU) |
| **API Externa** | nba_api |
| **Contenedores** | Docker, Docker Compose |
| **Testing** | pytest (215 tests) |
| **CI/CD** | GitHub Actions |

### Organización de Módulos

```
Dateados/
├── db/                          # Capa de Base de Datos
│   ├── models.py                # 16 modelos SQLAlchemy (ORM)
│   ├── connection.py            # Pool de conexiones y sesiones
│   ├── query.py                 # Consultas optimizadas de alto nivel
│   ├── services.py              # Servicios (get_or_create patterns)
│   ├── logging.py               # Sistema centralizado de logging
│   ├── constants.py             # Constantes del dominio
│   ├── maintenance.py           # Scripts de mantenimiento
│   └── utils/                   # Herramientas CLI
│       ├── query_cli.py         # CLI interactivo de consultas
│       ├── view_logs.py         # Visualizador de logs
│       └── check_db_status.py   # Estado de tareas del sistema
│
├── ingestion/                   # Motor de Ingesta (ETL)
│   ├── cli.py                   # Punto de entrada CLI
│   ├── strategies.py            # SmartIngestion, FullIngestion
│   ├── ingestors.py             # GameIngestion, SeasonIngestion
│   ├── parallel.py              # Orquestador de multiprocessing
│   ├── workers.py               # Workers especializados
│   ├── api_client.py            # Cliente NBA API con retry
│   ├── models_sync.py           # Sync de biografías y premios
│   ├── derived_tables.py        # Generación de tablas agregadas
│   ├── checkpoints.py           # Sistema de checkpoints
│   └── utils.py                 # Utilidades (safe_int, ProgressReporter)
│
├── outliers/                    # Sistema de Detección de Anomalías
│   ├── cli.py                   # CLI (train, backfill, top, stats)
│   ├── runner.py                # OutlierRunner (orquestador)
│   ├── base.py                  # BaseDetector, OutlierResult
│   ├── models.py                # Modelos de outliers (6 tablas)
│   ├── stats/                   # Detectores estadísticos
│   │   ├── player_zscore.py     # Z-Score detector
│   │   └── streaks.py           # Streak detector (9 tipos)
│   └── ml/                      # Machine Learning
│       ├── autoencoder.py       # Modelo autoencoder
│       ├── train.py             # Entrenamiento
│       ├── data_pipeline.py     # Normalización de datos
│       └── inference.py         # Inferencia en producción
│
└── web/                         # Aplicación Web (FastAPI)
    ├── app.py                   # Configuración del servidor
    ├── contest_logic.py         # Lógica de gamificación
    ├── routes/                  # Controladores (10 archivos)
    │   ├── home.py              # Página principal
    │   ├── players.py           # Jugadores
    │   ├── teams.py             # Equipos
    │   ├── seasons.py           # Temporadas y standings
    │   ├── games.py             # Partidos
    │   ├── leaders.py           # Líderes estadísticos
    │   ├── outliers.py          # Dashboard de outliers
    │   ├── streaks.py           # Dashboard de rachas
    │   ├── contest.py           # Juego "Alto el Lápiz"
    │   └── admin.py             # Panel de administración
    └── templates/               # Vistas Jinja2
        └── ... (21 templates organizados por módulo)
```

### Flujo de Datos

```mermaid
graph TD
    A[NBA API] -->|ingestion| B[PostgreSQL]
    B -->|outliers.ml| C[Autoencoder]
    B -->|outliers.stats| D[Z-Score Detector]
    B -->|outliers.stats| E[Streak Detector]
    C -->|results| B
    D -->|results| B
    E -->|results| B
    B -->|web.query| F[FastAPI]
    F -->|render| G[Web Dashboard]
    H[GitHub Actions] -->|daily| A
```

### Arquitectura de Base de Datos

El sistema utiliza **16 tablas** organizadas en **3 capas lógicas**:

**Capa 1: Datos Core** (7 tablas)
- `teams`: Equipos con información organizativa
- `players`: Jugadores con biografía completa
- `games`: Partidos con resultados y marcadores
- `player_game_stats`: Estadísticas individuales por partido (tabla principal)
- `player_team_seasons`: Agregados por jugador/equipo/temporada
- `team_game_stats`: Agregados por equipo/partido
- `player_awards`: Premios y reconocimientos

**Capa 2: Sistema de Outliers** (6 tablas)
- `outliers_league`: Anomalías detectadas por autoencoder
- `outliers_player`: Explosiones/crisis detectadas por Z-Score
- `outliers_player_trends`: Cambios sostenidos de rendimiento
- `outliers_player_season_state`: Estado acumulativo para cálculo O(1) de Z-Score
- `outliers_streaks`: Registro de rachas de rendimiento
- `outliers_streak_all_time_records`: Caché de récords absolutos

**Capa 3: Sistema y Auditoría** (3 tablas)
- `ingestion_checkpoints`: Checkpoints para ingesta resumible
- `system_status`: Estado de tareas del sistema
- `log_entries`: Logs persistentes

📖 **Ver [SCHEMA.md](SCHEMA.md) para arquitectura detallada de cada tabla**

---

## 🤖 Model Context Protocol (MCP) Server

El proyecto incluye un servidor MCP (**Model Context Protocol**) nativo que permite a agentes de inteligencia artificial (como Claude, Cursor, o agentes custom) conectarse e interactuar directamente con la base de datos de estadísticas de la NBA.

**Iniciar el servidor:**
```bash
python -m mcp_server
```

**Características principales:**
- **Soporte de transporte dual:** Funciona a través de `stdio` (para integraciones locales) o `sse` (Server-Sent Events para integraciones web/remotas).
- **24 herramientas de solo lectura (read-only):** Expuestas de forma segura para explorar la base de datos sin riesgo de modificación.
- **5 Categorías de herramientas:**
  1. **Jugadores (Players):** Búsqueda de jugadores, biografías, stats de carrera y compañeros históricos.
  2. **Equipos (Teams):** Rosters, historial de franquicias, y récords.
  3. **Partidos (Games):** Box scores detallados, resultados recientes y filtrado avanzado.
  4. **Temporadas (Seasons):** Standings, brackets de playoffs e in-season tournament.
  5. **Anomalías (Outliers):** Acceso al sistema de machine learning para detectar actuaciones históricas o rachas activas.

Este servidor convierte al sistema Dateados en una herramienta poderosa para flujos de trabajo autónomos y análisis asistido por IA.

---

## 📖 Guía de Uso

### 1️⃣ Ingesta de Datos

**CLI Principal:** `python -m ingestion.cli`

#### Modo Smart (Recomendado, por defecto)

La **estrategia inteligente** analiza el estado actual de la base de datos y combina automáticamente:
- **Carga incremental** para temporadas con datos (frontera)
- **Carga masiva paralela** para temporadas vacías (huecos históricos)

**¿Cuándo usar?**
- ✅ Actualización diaria de partidos
- ✅ Primera carga del sistema
- ✅ Recuperación tras interrupciones
- ✅ Cualquier escenario de uso general

**Ejemplos:**

```bash
# Ingesta inteligente (analiza automáticamente qué falta)
python -m ingestion.cli

# Limitar a las últimas 2 temporadas (útil para actualizaciones rápidas)
python -m ingestion.cli --limit-seasons 2

# Sin ejecutar detección de outliers al finalizar
python -m ingestion.cli --skip-outliers
```

**¿Qué hace internamente?**
1. Sincroniza equipos y jugadores base desde la API
2. Analiza temporadas cronológicamente inverso (2024-25 → 1983-84)
3. Detecta la **frontera** (última temporada con datos completos)
4. Aplica carga incremental a la frontera
5. Lanza workers paralelos para llenar huecos históricos
6. Sincroniza premios y biografías faltantes
7. Regenera tablas derivadas
8. Ejecuta detección de outliers (opcional)

#### Modo Full

Fuerza una **ingesta histórica completa** con procesamiento paralelo, útil para reparaciones o reconstrucción.

**¿Cuándo usar?**
- 🔧 Reparar datos corruptos
- 🔧 Reconstruir desde cero con parámetros específicos
- 🔧 Cargar rango de temporadas exacto

**Ejemplos:**

```bash
# Ingesta completa desde 1983-84 hasta presente
python -m ingestion.cli --mode full

# Rango específico de temporadas
python -m ingestion.cli --mode full --start-season 2020-21 --end-season 2023-24

# Reanudar desde checkpoint guardado
python -m ingestion.cli --mode full --resume
```

#### Checkpoints y Reinicio

El sistema guarda **checkpoints automáticos** cada 10-20 partidos en la tabla `ingestion_checkpoints`.

**Reanudar tras interrupción:**
```bash
# Modo smart siempre reanuda automáticamente
python -m ingestion.cli

# Modo full requiere flag --resume
python -m ingestion.cli --mode full --resume
```

**Ver checkpoints guardados:**
```bash
python -m db.utils.query_cli --summary
# Buscar sección "Checkpoints"
```

#### Arquitectura de Workers

El sistema utiliza **procesamiento paralelo** con las siguientes características:

- **Staggering automático**: Retrasos de 5-15 segundos entre workers para distribuir carga API
- **Supervisión continua**: Monitoreo cada 5 segundos
- **Relanzamiento automático**: Workers fallidos se reinician sin intervención
- **Capacidad dinámica**: Se adapta a los cores del CPU disponibles

**Workers en acción:**
```
Worker-2023-24 → [▓▓▓▓▓▓▓▓░░] 80% (650/820 partidos)
Worker-2022-23 → [▓▓▓▓▓▓▓▓▓▓] 100% Finalizado
Worker-2021-22 → [▓▓▓░░░░░░░] 30% (250/820 partidos)
```

#### Parámetros Completos

| Parámetro | Tipo | Default | Descripción |
|-----------|------|---------|-------------|
| `--mode` | choice | `smart` | Estrategia de ingesta: `smart` (auto), `full` (histórica) o `awards` (solo premios) |
| `--start-season` | str | `1983-84` | Temporada inicial (solo modo full) |
| `--end-season` | str | actual | Temporada final (solo modo full) |
| `--limit-seasons` | int | None | Límite de temporadas a analizar (solo modo smart) |
| `--resume` | flag | - | Reanudar desde checkpoint (solo modo full) |
| `--skip-outliers` | flag | - | No ejecutar detección de outliers al finalizar |
| `--init-db` | flag | - | Inicializar esquema antes de ingestar |

#### Sincronización de Datos Adicionales

Durante la ingesta, el sistema sincroniza automáticamente:

- **Premios** (`player_awards`): MVP, Championships, All-Star, All-NBA, etc.
- **Biografías** (`players`): Altura, peso, país, draft, universidad
- **Tablas derivadas**: `player_team_seasons`, `team_game_stats`

**Sincronizar manualmente:**
```bash
# Premios de jugadores activos
python -m ingestion.models_sync --awards

# Biografías faltantes
python -m ingestion.models_sync --bios
```

---

### 2️⃣ Sistema de Outliers

**CLI Principal:** `python -m outliers.cli <comando>`

#### ¿Qué son los outliers?

Los **outliers** (anomalías estadísticas) son partidos donde un jugador tuvo un rendimiento extraordinario comparado con:
- **Outliers de Liga** (Autoencoder): El universo global de la NBA
- **Outliers de Jugador** (Z-Score): Su propio historial personal
- **Rachas**: Secuencias notables de partidos consecutivos con criterio específico

#### Tres Metodologías Complementarias

**1. Autoencoder (PyTorch) - Anomalías Globales**
- Entrena con 14 features: pts, ast, reb, stl, blk, tov, pf, fg_pct, fg3_pct, ft_pct, fga, fta, fg3a, min
- Pesado temporal: Temporadas recientes tienen más peso
- Output: Error de reconstrucción + percentil
- Tabla: `outliers_league`

**2. Z-Score por Jugador - Explosiones/Crisis**
- Calcula desviación estándar en 12 features
- Umbral: 2.0 (2 desviaciones estándar)
- Mínimo: 10 partidos históricos
- Clasifica: `explosion` (excepcional) o `crisis` (bajo)
- Tabla: `outliers_player`

**3. Detector de Rachas - Rendimiento Sostenido**
- 9 tipos de rachas monitoreadas simultáneamente
- Soporte multi-competición (RS, PO, NBA Cup, Play-In)
- Congelamiento: No rompe si jugador no juega o no intenta tiros
- Tabla: `outliers_streaks`

**Tipos de rachas:**
- `pts_20`, `pts_30`, `pts_40`: Partidos con 20+, 30+, 40+ puntos
- `triple_double`: Triple-dobles (10+ en 3 categorías)
- `reb_10`, `ast_10`: 10+ rebotes/asistencias
- `fg_pct_60`, `fg3_pct_50`, `ft_pct_90`: Precisión de tiro (con mínimo de intentos)

#### Comandos Principales

##### Primera Configuración

**1. Entrenar el modelo Autoencoder** (requiere ≥3 temporadas de datos)

```bash
# Entrenamiento básico
python -m outliers.cli train --epochs 100

# Entrenamiento con configuración personalizada
python -m outliers.cli train --epochs 200 --hidden-dims 64,32,16

# Con logging detallado de métricas
python -m outliers.cli train --epochs 100 --experiment
```

**Parámetros:**
- `--epochs N`: Número de épocas de entrenamiento (default: 50)
- `--hidden-dims DIMS`: Dimensiones de capas ocultas, separadas por coma (default: 64,32,16)
- `--experiment`: Activa logging detallado de métricas

**Output:** Genera archivos en `outliers/ml/models/`:
- `autoencoder.pt`: Pesos del modelo PyTorch
- `scaler.pkl`: StandardScaler para normalización

**2. Validar el modelo**

```bash
python -m outliers.cli validate-model
```

Verifica que el modelo cargue correctamente y genere predicciones válidas.

**3. Procesar histórico (backfill)**

```bash
# Todas las temporadas
python -m outliers.cli backfill

# Solo una temporada específica
python -m outliers.cli backfill --season 2023-24

# Sin algún detector específico
python -m outliers.cli backfill --skip-league    # Sin autoencoder
python -m outliers.cli backfill --skip-player    # Sin Z-Score
python -m outliers.cli backfill --skip-streaks   # Sin rachas
```

**Tiempo estimado:** 10-30 minutos por temporada completa.

##### Uso Diario

**Detección automática:** Los nuevos partidos se analizan automáticamente al ejecutar `python -m ingestion.cli`.

**Consulta de outliers:**

```bash
# Top 20 outliers de la semana (default)
python -m outliers.cli top --limit 20

# Top outliers del último mes
python -m outliers.cli top --limit 20 --window month

# Top outliers de una temporada completa
python -m outliers.cli top --limit 10 --season 2023-24 --window season

# Solo del último partido
python -m outliers.cli top --limit 20 --window last_game
```

**Ventanas disponibles:**
- `last_game`: Solo el partido más reciente
- `week`: Últimos 7 días
- `month`: Últimos 30 días
- `season`: Toda la temporada especificada

**Estadísticas del sistema:**

```bash
python -m outliers.cli stats
```

**Output:**
- Total de outliers por tipo (league, player, streaks)
- Distribución por ventana temporal
- Top jugadores con más outliers
- Rachas activas por tipo
- Récords históricos por competición

##### Mantenimiento

**Limpiar outliers:**

```bash
# Limpiar todos los outliers
python -m outliers.cli clear --confirm --what all

# Limpiar solo outliers de liga (autoencoder)
python -m outliers.cli clear --confirm --what league

# Limpiar solo outliers de jugador (Z-Score)
python -m outliers.cli clear --confirm --what player

# Limpiar solo rachas
python -m outliers.cli clear --confirm --what streaks
```

**Regenerar solo rachas desde cero:**

```bash
# 1. Limpiar rachas
python -m outliers.cli clear --confirm --what streaks

# 2. Regenerar
python -m outliers.cli backfill --skip-league --skip-player
```

#### Interpretación de Resultados

**Ejemplo de output de `top`:**

```
=== TOP OUTLIERS DE LA SEMANA ===

OUTLIERS DE LIGA (Autoencoder)
Rank | Jugador           | Fecha      | PTS/REB/AST | Percentil | Features Principales
-----|-------------------|------------|-------------|-----------|---------------------
1    | LeBron James      | 2024-01-28 | 40/10/9     | 99.2%     | pts(0.45), ast(0.30)
2    | Luka Doncic       | 2024-01-27 | 35/12/15    | 98.8%     | ast(0.52), reb(0.28)

OUTLIERS DE JUGADOR (Z-Score)
Rank | Jugador           | Fecha      | PTS/REB/AST | Tipo      | Z-Scores
-----|-------------------|------------|-------------|-----------|----------
1    | Stephen Curry     | 2024-01-28 | 45/5/8      | explosion | pts(3.2), fg3m(2.8)
2    | Draymond Green    | 2024-01-27 | 2/8/12      | crisis    | pts(-2.1), fga(-2.4)

RACHAS ACTIVAS
Jugador           | Tipo          | Cuenta | Competición | Desde
------------------|---------------|--------|-------------|------------
Joel Embiid       | pts_30        | 8      | RS          | 2024-01-15
Nikola Jokic      | triple_double | 5      | RS          | 2024-01-20
```

---

### 3️⃣ Interfaz Web

**Servidor FastAPI con Jinja2 + Tailwind CSS**

#### Iniciar Servidor

```bash
# Desarrollo (con auto-reload al cambiar código)
uvicorn web.app:app --reload --port 8000

# Producción
uvicorn web.app:app --host 0.0.0.0 --port 8000
```

**Acceso:** http://localhost:8000

#### Páginas Principales

| Ruta | Descripción |
|------|-------------|
| `/` | Página principal con estadísticas de BD y partidos recientes |
| `/players` | Lista de jugadores con búsqueda y filtros (posición, conferencia) |
| `/players/{id}` | Detalle completo: estadísticas, carrera, premios, biografía |
| `/players/{id}/teammates` | Compañeros históricos por temporada y equipo |
| `/teams` | Lista de equipos organizados por conferencia y división |
| `/teams/{id}` | Detalle de equipo: roster, récord, partidos recientes |
| `/seasons` | Redirección a la temporada más reciente |
| `/seasons/{season}` | Standings + Playoffs bracket + NBA Cup bracket |
| `/leaders` | Top 10 líderes estadísticos (PTS, REB, AST, STL, BLK) |
| `/games` | Lista de partidos con filtros por temporada y equipo |
| `/games/{game_id}` | Box score completo con estadísticas detalladas |
| `/contest` | Juego "Alto el Lápiz" (trivia interactiva tipo Scattergories) |
| `/outliers` | Dashboard de detección de anomalías con filtros |
| `/streaks` | Dashboard de rachas (activas, rotas, récords) |
| `/admin/ingest` | Panel de administración para ejecutar y monitorear ingestas |

**Total:** 15 páginas

#### Dashboard de Administración

**Acceso:** http://localhost:8000/admin/ingest

**Características:**
- **Ejecutar ingesta incremental** desde la interfaz (botón "Ejecutar Ingesta")
- **Monitoreo en tiempo real**: Barra de progreso actualizada vía polling cada 2 segundos
- **Stream de logs**: Últimos 50 logs del proceso en vivo
- **Estado de tareas**: Visualización de todos los procesos del sistema (ingesta, outliers, sync)

**Flujo:**
1. Click en "Ejecutar Ingesta"
2. Backend lanza proceso en background (no bloquea)
3. Frontend hace polling a `/admin/ingest/status` cada 2s
4. Muestra barra de progreso + mensaje + logs
5. Al finalizar, muestra resultado (éxito/error)

#### API Endpoints

| Ruta | Método | Descripción |
|------|--------|-------------|
| `/api/contest/validate` | GET | Validar respuesta del juego (params: category, answer) |
| `/api/contest/hint` | GET | Obtener pista inteligente (param: category) |
| `/admin/ingest/run` | POST | Iniciar ingesta en background |
| `/admin/ingest/status` | GET | Estado actual de la ingesta (progress, message, status) |
| `/admin/ingest/logs` | GET | Últimos logs de la ingesta (param: limit=50) |
| `/admin/ingest/cron` | POST | Disparador automatizado de ingesta diaria |
| `/admin/ingest/reset` | POST | Fuerza el reinicio del estado del sistema y procesos atascados |
| `/admin/update/awards` | POST | Forzar actualización de premios de jugadores activos |
| `/admin/update/outliers` | POST | Forzar recálculo completo de outliers |
| `/outliers/api/league` | GET | Top outliers de liga en JSON (params: limit, window, season) |
| `/outliers/api/player` | GET | Top outliers de jugador en JSON (params: limit, window, season) |
| `/outliers/api/stats` | GET | Estadísticas del sistema en JSON |

**Total:** 12 endpoints de API

#### Características Destacadas

**Box Score Completo:**
- Estadísticas por jugador: min, pts, reb, ast, stl, blk, tov, pf, +/-
- Shooting detallado: FGM-FGA (%), 3PM-3PA (%), FTM-FTA (%)
- Totales por equipo agregados automáticamente
- Indicadores visuales: 🏀 Triple-doble, ⭐ Doble-doble
- Marcadores por cuarto + overtimes

**Standings Dinámicos:**
- Clasificación en tiempo real: W-L, %, racha
- Playoff bracket: Visualización de rondas completas
- NBA Cup bracket: Torneo In-Season separado
- Parseo automático de Game IDs para posicionamiento

**Dashboard de Rachas:**
- 🔥 Rachas activas ordenadas por duración
- ⭐ Rachas notables (≥5% del récord)
- 🏆 Rachas históricas (≥70% del récord, badges)
- 💔 Rachas recientemente rotas (últimas 30)
- 📊 Récords absolutos por tipo y competición

---

### 4️⃣ Utilidades

#### CLI de Consultas

**Entrada:** `python -m db.utils.query_cli`

##### Resumen de Base de Datos

```bash
# Resumen completo de registros
python -m db.utils.query_cli --summary
```

**Output:**
```
=== RESUMEN DE BASE DE DATOS ===
Equipos: 30
Jugadores: 4,523
Partidos: 52,341
Stats de jugador: 1,245,672
Temporadas cubiertas: 1983-84 a 2024-25
```

##### Consultas de Equipos

```bash
# Listar todos los equipos
python -m db.utils.query_cli --teams

# Filtrar por conferencia
python -m db.utils.query_cli --teams --conference West

# Filtrar por división
python -m db.utils.query_cli --teams --division Pacific
```

##### Consultas de Jugadores

```bash
# Listar jugadores (paginado)
python -m db.utils.query_cli --players

# Buscar por nombre
python -m db.utils.query_cli --players --name "LeBron"

# Filtrar por posición
python -m db.utils.query_cli --players --position G

# Solo jugadores activos
python -m db.utils.query_cli --players --position F --active-only

# Stats de un jugador específico
python -m db.utils.query_cli --player "LeBron James"
python -m db.utils.query_cli --player "LeBron James" --season 2023-24
```

##### Consultas de Partidos

```bash
# Listar partidos recientes
python -m db.utils.query_cli --games --season 2023-24 --limit 10

# Partidos de un equipo
python -m db.utils.query_cli --games --team LAL --limit 20

# Detalle de un partido específico
python -m db.utils.query_cli --game 0022300123
```

##### Top Líderes Estadísticos

```bash
# Top 10 anotadores de la temporada
python -m db.utils.query_cli --top pts --season 2023-24

# Top 20 asistidores (all-time)
python -m db.utils.query_cli --top ast --limit 20

# Top 15 reboteadores de la temporada
python -m db.utils.query_cli --top reb --season 2023-24 --limit 15
```

**Estadísticas disponibles:** `pts`, `reb`, `ast`, `stl`, `blk`, `fg_pct`, `fg3_pct`, `ft_pct`

#### Visualizador de Logs

**CLI:** `python -m db.utils.view_logs`

##### Modo Normal

```bash
# Ver últimos 50 logs (default)
python -m db.utils.view_logs

# Ver más logs
python -m db.utils.view_logs --limit 100

# Filtrar por nivel
python -m db.utils.view_logs --level ERROR
python -m db.utils.view_logs --level WARNING --limit 50
```

**Niveles disponibles:** `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`

##### Modo Monitor (Dashboard en Tiempo Real)

```bash
# Monitor con refresh cada 3 segundos (default)
python -m db.utils.view_logs --monitor

# Refresh cada segundo (más reactivo)
python -m db.utils.view_logs --monitor --interval 1

# Refresh cada 5 segundos (menos carga)
python -m db.utils.view_logs --monitor --interval 5
```

**Características del modo monitor:**
- ✅ Auto-refresh configurable
- ✅ Color coding: errores en rojo, warnings en amarillo, info en verde
- ✅ Estadísticas por nivel
- ✅ Scroll automático a logs más recientes
- ✅ Ctrl+C para salir

#### Mantenimiento de Base de Datos

**Estado del sistema:**

```bash
# Ver estado de todas las tareas
python -m db.utils.check_db_status

# Limpiar estados manualmente (resetear a idle)
python -m db.utils.check_db_status --clear
```

**Limpieza de logs antiguos:**

```bash
# Limpia logs con más de 30 días (default)
python -m db.utils.log_cleanup

# Mantener últimos 7 días
python -m db.utils.log_cleanup --days 7

# Mantener últimos 90 días
python -m db.utils.log_cleanup --days 90
```

**Limpieza de datos:**

```bash
# Limpiar partidos y stats (preserva equipos/jugadores)
python -m db.utils.clean_database

# Limpiar jugadores sin estadísticas
python -m db.utils.clean_players

# Reparar biografías incompletas
python -m db.utils.repair_bios
```

---

## 🧪 Testing

**Framework:** pytest con 215 tests automatizados

### Ejecutar Tests

```bash
# Suite completa
python -m pytest tests/ -v

# Tests específicos por archivo
python -m pytest tests/test_outliers.py -v
python -m pytest tests/test_ingest.py -v
python -m pytest tests/test_models.py -v

# Test individual
python -m pytest tests/test_outliers.py::TestStandardScaler::test_fit_calculates_mean_and_std -v

# Tests que coincidan con patrón
python -m pytest tests/ -k "zscore" -v
python -m pytest tests/ -k "streak" -v

# Con cobertura de código
python -m pytest tests/ --cov=outliers --cov=ingestion --cov=db -v
```

### Cobertura por Módulo

| Archivo | Tests | Áreas Cubiertas |
|---------|-------|-----------------|
| `test_outliers.py` | 56+ | StandardScaler, Z-Score, Streaks, Autoencoder, OutlierRunner, Temporal Weighting |
| `test_models.py` | 15+ | 16 modelos SQLAlchemy, relaciones, constraints, propiedades calculadas |
| `test_utils.py` | 20+ | safe_int/float, parse_date, convert_minutes, normalize_season, get_or_create |
| `test_ingest.py` | 10+ | Parseo de game IDs, deducción de temporada, validación de API |

**Total:** 215 tests con ~80% de cobertura en módulos core

### Fixtures Disponibles

El archivo `tests/conftest.py` provee fixtures reutilizables:

**Datos de ejemplo:**
- `sample_team_data`, `sample_player_data`, `sample_game_data`
- `triple_double_stats`, `double_double_stats`

**Mocks de API:**
- `mock_nba_api_response`, `mock_boxscore_response`, `mock_game_summary_response`

**Game IDs:**
- `regular_season_game_id`, `playoff_game_id`, `playin_game_id`, `ist_game_id`

---

## 🚀 Despliegue

### Docker Local

**Levantar PostgreSQL:**

```bash
# Iniciar
docker-compose up -d

# Verificar estado
docker-compose ps

# Ver logs
docker-compose logs postgres
docker-compose logs -f postgres  # Modo follow

# Detener
docker-compose down

# Detener y eliminar datos (⚠️ borra todo)
docker-compose down -v
```

**Configuración** (`docker-compose.yml`):
- **Imagen:** PostgreSQL 18 Alpine
- **Puerto:** 5432
- **Usuario/Password:** nba/nba (configurable en `.env`)
- **Base de datos:** nba_stats
- **Persistencia:** Volumen `./postgres_data`
- **Health checks:** `pg_isready` cada 10s

### Render.com (Producción)

**Servicios definidos en `render.yaml`:**

El archivo `render.yaml` define únicamente la **Aplicación web FastAPI** corriendo bajo Python 3.10.12. (La base de datos PostgreSQL debe provisionarse de forma separada o utilizando una conexión externa inyectada vía entorno).

- **Nombre:** `dateados-web`
- **Runtime:** Python 3.10.12
- **Build:** `pip install -r requirements.txt`
- **Pre-deploy:** `python -m ingestion.cli --init-db`
- **Start:** `uvicorn web.app:app --host 0.0.0.0 --port $PORT`
- **Health check:** `/`

**Variables de entorno automáticas:**
- `DATABASE_URL`: URL de conexión PostgreSQL (inyectada por Render)
- `PORT`: Puerto asignado (inyectado por Render)

**Notas importantes:**
- ❌ **No incluye PyTorch** en producción (solo `requirements.txt`)
- ✅ **Autoencoder pre-entrenado**: Subir modelos `.pt` y `.pkl` al repo
- ✅ **Detección de outliers**: Funciona con modelo pre-entrenado (solo inferencia)
- ⚠️ **No se puede entrenar** en Render Free (requiere GPU/mucha CPU)

### Variables de Entorno Requeridas

| Variable | Descripción | Ejemplo |
|----------|-------------|---------|
| `DATABASE_URL` | URL completa de conexión PostgreSQL | `postgresql://nba:nba@localhost:5432/nba_stats` |

**Formato:**
```
postgresql://[usuario]:[password]@[host]:[puerto]/[database]
```

**Configuración en Render:**
1. Ir a **Database** > Connections
2. Copiar **External Connection String**
3. Configurar en **Settings** > **Secrets** de GitHub Actions

---

## 🤖 Automatización

### GitHub Actions - Ingesta Diaria

**Workflow:** `.github/workflows/nba_daily_ingest.yml`

**Horario:** Todos los días a las **07:00 UTC** (automático).

**¿Cómo funciona?**
1. GitHub Actions realiza una petición `POST` segura al servidor de Render.
2. La petición "despierta" el servicio si está en reposo (Render Free Tier).
3. El servidor ejecuta la **ingesta inteligente** en segundo plano.
4. Los datos se actualizan directamente desde el entorno de Render, evitando bloqueos de IP de la NBA API.

### GitHub Actions - Actualización de Premios

**Workflow:** `.github/workflows/update_awards.yml`

**Uso:** Manual (`workflow_dispatch`).

**¿Qué hace?**
- Dispara una actualización de premios y biografías de todos los jugadores activos.
- Útil para sincronizar reconocimientos oficiales de la NBA tan pronto como se anuncian.

### GitHub Actions - Actualización de Outliers

**Workflow:** `.github/workflows/update_outliers.yml`

**Uso:** Manual (`workflow_dispatch`).

**¿Qué hace?**
- Ejecuta el recálculo completo de outliers (liga, jugador y rachas).
- Útil para corregir anomalías estadísticas o refrescar las tendencias tras cambios en el modelo.

### GitHub Actions - Reset del Sistema (Botón de Pánico)

**Workflow:** `.github/workflows/nba_reset_system.yml`

**Uso:** Manual (`workflow_dispatch`).

**¿Qué hace?**
- Fuerza la parada de todos los procesos de ingesta (`pkill`).
- Limpia el estado de la base de datos (resetea a `idle`).
- Borra los checkpoints pendientes para permitir un inicio limpio.

### Configuración Necesaria

Para que la automatización funcione, debes configurar los siguientes secretos en tu repositorio de GitHub (**Settings > Secrets and variables > Actions**):

| Secreto | Descripción | Valor Ejemplo |
|---------|-------------|---------------|
| `API_BASE_URL` | URL base de tu aplicación en Render (NO RENDER_URL) | `https://dateados-web.onrender.com` |
| `SECURE_TOKEN` | Token de seguridad para la API (Header: `X-Secure-Token`) | `tu_clave_secreta_aqui` |

*Nota: Asegúrate de añadir también `SECURE_TOKEN` en las **Environment Variables** de tu servicio en el Dashboard de Render. El secreto de URL debe llamarse `API_BASE_URL` (NO RENDER_URL).*

---

## 🔧 Troubleshooting

### Problemas Comunes

#### Error: "Ya hay una ingesta en curso" (Sistema bloqueado)

**Causa:** Una ingesta previa falló o se interrumpió sin limpiar el estado en la base de datos.

**Solución:**
1. Ve a la pestaña **Actions** en tu repositorio de GitHub.
2. Selecciona el workflow **"NBA System Reset"**.
3. Haz clic en **"Run workflow"**.
4. Una vez finalizado, puedes volver a lanzar la ingesta normal.

#### Error: "Database does not exist"

**Causa:** La base de datos no está creada o la URL es incorrecta.

**Solución:**
```bash
# 1. Verificar que PostgreSQL esté corriendo
docker-compose ps

# 2. Verificar .env
cat .env  # DATABASE_URL debe ser correcto

# 3. Inicializar esquema
python -m ingestion.cli --init-db
```

#### Error: "Workers failing repeatedly"

**Causa:** Rate limit de la NBA API o problemas de conectividad.

**Solución:**
- El sistema reintentará automáticamente con backoff exponencial
- Si persiste, revisar logs: `python -m db.utils.view_logs --level ERROR`
- Ajustar `API_DELAY` en `ingestion/config.py` (aumentar a 1.0-1.5s)

#### Error: "Autoencoder model not found"

**Causa:** Modelo no entrenado o archivos `.pt`/`.pkl` faltantes.

**Solución:**
```bash
# Entrenar modelo localmente
python -m outliers.cli train --epochs 100

# Verificar que se generaron los archivos
ls -la outliers/ml/models/
# Debe existir: autoencoder.pt, scaler.pkl
```

#### Logs no aparecen en la base de datos

**Causa:** Handler SQLAlchemy no configurado.

**Solución:**
```bash
# Verificar que la tabla existe
python -m db.utils.check_db_status

# Si no existe, inicializar
python -m ingestion.cli --init-db

# Limpiar logs antiguos si la tabla está llena
python -m db.utils.log_cleanup --days 7
```

#### Outliers no se detectan en ingesta incremental

**Causa:** Flag `--skip-outliers` activado o módulo de outliers no instalado.

**Solución:**
```bash
# Ejecutar sin flag
python -m ingestion.cli

# O ejecutar detección manual
python -m outliers.cli backfill --season 2024-25
```

### Logs y Debugging

**Ver logs en tiempo real:**
```bash
# Modo monitor
python -m db.utils.view_logs --monitor --interval 1
```

**Ver logs de un proceso específico:**
```bash
# Solo logs de ingesta
python -m db.utils.view_logs --limit 200 | grep "ingestion"

# Solo errores
python -m db.utils.view_logs --level ERROR --limit 100
```

**Limpiar logs antiguos para mejorar rendimiento:**
```bash
# Mantener solo últimos 7 días
python -m db.utils.log_cleanup --days 7
```

**Verificar estado de checkpoints:**
```bash
python -m db.utils.query_cli --summary
# Ver sección "Checkpoints" para progreso guardado
```

### FAQ

**¿Cuánto tiempo tarda la primera carga completa?**
- Depende del rango de temporadas y CPU disponibles
- ~2-4 horas para todas las temporadas (1983-84 a presente)
- ~30-60 minutos para una temporada completa

**¿Puedo pausar la ingesta y reanudar después?**
- Sí, usa Ctrl+C para interrumpir
- El sistema guarda checkpoints cada 10-20 partidos
- Reanudar: `python -m ingestion.cli` (modo smart reanuda automáticamente)

**¿Cómo actualizo solo la temporada actual?**
```bash
python -m ingestion.cli --limit-seasons 1
```

**¿Puedo ejecutar ingesta y web server simultáneamente?**
- Sí, son procesos independientes
- Recomendado: Ejecutar ingesta en terminal aparte
- La web mostrará datos en tiempo real a medida que se cargan

**¿Qué hacer si un worker se queda bloqueado?**
- El sistema detectará automáticamente y relanzará tras timeout
- Si persiste, matar manualmente con Ctrl+C e iniciar de nuevo
- Revisar logs para identificar la causa

---

## 📊 Estadísticas del Proyecto

### Datos

- **Temporadas cubiertas:** 42+ (1983-84 a 2024-25)
- **Registros estimados:** 1.2M+ estadísticas de jugador
- **Jugadores históricos:** 4,500+
- **Equipos:** 30 actuales + históricos
- **Partidos:** 50,000+ (Regular Season + Playoffs + NBA Cup + Play-In)

### Código

- **Archivos Python:** 74
- **Líneas de código:** ~17,200
- **Tests automatizados:** 215
- **Cobertura de tests:** ~80% en módulos core

### Base de Datos

- **Tablas:** 16
- **Índices:** 25+ (optimización de consultas)
- **Constraints:** 20+ (validación de integridad)
- **Relaciones:** 12 foreign keys

### Performance

- **Ingesta:** ~200-400 partidos/minuto (modo paralelo)
- **Detección de outliers:** ~1,000 partidos/minuto (Z-Score)
- **Queries web:** <100ms promedio (con índices)
- **Workers paralelos:** Hasta 8 simultáneos (dependiente de CPU)

---

*Última actualización: Febrero 2026*