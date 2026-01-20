# Dateados

Sistema integral de ingesta, almacenamiento y análisis de datos de la NBA. Diseñado para construir una base de datos histórica robusta desde la temporada 1983-84 hasta la actualidad.

## Características Principales

- **Interfaz Web Moderna**: Dashboard completo para explorar estadísticas, jugadores, equipos y partidos (FastAPI + Tailwind CSS).
- **Arquitectura de Ingesta Modular**: Sistema refactorizado en componentes especializados para mayor mantenibilidad y robustez.
- **Ingesta Histórica Completa**: Capacidad de ingerir todos los partidos desde 1983-84 con sistema de **checkpoints** y reanudación automática.
- **Ingesta Incremental Inteligente**: Procesa solo los partidos nuevos de las últimas temporadas, deteniéndose al encontrar registros existentes.
- **Recuperación Automática de Errores**: Detección de errores fatales de API (rate limit, timeouts) que dispara un reinicio automático del proceso preservando el progreso.
- **Regeneración Selectiva de Estadísticas**: Recálculo optimizado de tablas agregadas solo para las temporadas afectadas por la nueva data.
- **Esquema Relacional Optimizado**: PostgreSQL con soporte para tipos JSON (marcadores por cuarto) e Interval (minutos jugados).

## Estructura del Proyecto

```
NBA/
├── db/                          # Módulo de base de datos
│   ├── connection.py            # Pool de conexiones y configuración
│   ├── models.py                # Modelos SQLAlchemy (ORM)
│   └── utils/                   # Utilidades de BD
│       ├── query.py             # Funciones de consulta de alto nivel
│       ├── query_cli.py         # CLI para consultas interactivas
│       ├── summary.py           # Resúmenes de estado de la BD
│       ├── clean_database.py    # Script de limpieza completa
│       └── clean_players.py     # Script de limpieza de jugadores
├── ingestion/                   # Sistema de Ingesta
│   ├── cli.py                   # Punto de entrada principal (reemplaza runner.py)
│   ├── api_client.py            # Cliente unificado para la API de la NBA
│   ├── core.py                  # Lógica central de ingesta (Games, Seasons)
│   ├── models_sync.py           # Sincronización de entidades (Players, Awards, Career)
│   ├── derived_tables.py        # Generador de tablas agregadas y estadísticas
│   ├── checkpoints.py           # Sistema de persistencia de progreso
│   ├── restart.py               # Lógica de reinicio automático ante errores fatales
│   ├── config.py                # Configuración de API, Delays y Timeouts
│   ├── utils.py                 # Funciones auxiliares de transformación de datos
│   ├── nba_static_data.py       # Información estática de temporadas
│   └── repair_legacy_players.py # Herramientas de reparación de datos históricos
├── web/                         # Interfaz Web (FastAPI)
│   ├── app.py                   # Aplicación principal y configuración
│   ├── routes/                  # Manejadores de rutas por sección
│   ├── templates/               # Vistas Jinja2 (Organizadas por módulo)
│   └── static/                  # Archivos estáticos (Tailwind CSS, Imágenes)
├── tests/                       # Suite de tests unitarios y de integración
├── logs/                        # Trazas detalladas de ejecución de ingesta
└── docker-compose.yml           # Infraestructura PostgreSQL (Docker)
```

## Requisitos

- Python 3.10+
- Docker y Docker Compose
- PostgreSQL 12+ (Gestionado vía Docker)

## Instalación y Configuración

### 1. Clonar y preparar entorno
```bash
git clone <repo-url>
cd NBA

python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configurar variables de entorno
Crear archivo `.env` en la raíz del proyecto:
```env
DATABASE_URL=postgresql://nba:nba@localhost:5432/nba_stats
```

### 3. Levantar base de datos
```bash
docker-compose up -d
```

### 4. Inicializar esquema
```bash
python -m ingestion.cli --init-db
```

## Manual de Ingesta

El módulo `ingestion.cli` es el punto de entrada unificado para todas las operaciones de datos.

### Modo Incremental (Uso diario)
Sincroniza los últimos partidos jugados y actualiza las estadísticas de las temporadas recientes:
```bash
python -m ingestion.cli --mode incremental
```

### Modo Histórico (Full)
Realiza una ingesta masiva desde la temporada especificada:
```bash
# Ingesta completa desde 1983-84 hasta hoy
python -m ingestion.cli --mode full --start-season 1983-84

# Temporada o rango específico
python -m ingestion.cli --mode full --start-season 1995-96 --end-season 1995-96
```

### Sistema de Reanudación
Si el proceso se interrumpe por errores de red o rate limiting:
1. El sistema guarda un **checkpoint** en `ingestion/.checkpoint.json`.
2. Se activa el **reinicio automático** (usando `restart.py`).
3. La ingesta continúa exactamente donde se quedó.

Para reanudar manualmente un proceso detenido:
```bash
python -m ingestion.cli --mode full --resume
```

## Interfaz Web

Para iniciar el servidor con recarga automática:
```bash
uvicorn web.app:app --reload --reload-dir web --reload-dir db --port 8000
```
Accede a: **http://localhost:8000**

## Notas Técnicas

- **Dorsales**: Capturados dinámicamente desde boxscores; sincronización oficial vía roster como fallback.
- **Agregados**: Las tablas `PlayerTeamSeason` y `TeamGameStats` se recalculan de forma selectiva para minimizar el impacto en BD.
- **Manejo de API**: Implementa backoff exponencial ante errores 403/429 para respetar los límites de la NBA API.
