# Dateados - NBA Data System

Sistema integral de alto rendimiento para la ingesta, procesamiento y visualización de datos de la NBA. Diseñado para construir una base de datos histórica exhaustiva desde la temporada 1983-84 hasta la actualidad, con capacidades avanzadas de análisis y gamificación.

## 🚀 Características Principales

- **Motor de Ingesta Paralelizado**: Sistema basado en `multiprocessing` que optimiza la descarga de datos históricos y diarios, gestionando workers independientes y staggering para evitar bloqueos de API.
- **Resiliencia Extrema**: Arquitectura con checkpoints automáticos y sistema de autoreinicio (`restart_process`) ante errores fatales o límites de la API.
- **Interfaz Web Premium**: Dashboard moderno desarrollado con FastAPI, Jinja2 y Tailwind CSS. Incluye secciones para equipos, jugadores, temporadas, líderes estadísticos y un panel de administración.
- **Gamificación - "Alto el lápiz"**: Juego de trivia integrado (tipo Tutti Frutti) que utiliza los datos reales de la BD para validar conocimientos sobre jugadores de la NBA.
- **Monitoreo Avanzado**: Sistema de logging persistente en base de datos con visualizador por CLI para un seguimiento detallado de las operaciones de ingesta.
- **Esquema Relacional Robusto**: PostgreSQL optimizado con soporte para tipos complejos (JSON para anotaciones por cuarto, Interval para minutos) y sincronización inteligente de biografía y premios.

## 📂 Estructura del Proyecto

```text
Dateados/
├── db/                          # Núcleo de Base de Datos
│   ├── models.py                # Modelos SQLAlchemy (ORM)
│   ├── connection.py            # Gestión del pool de conexiones
│   ├── query.py                 # Consultas optimizadas de alto nivel
│   ├── summary.py               # Generador de resúmenes de estado
│   └── utils/                   # Herramientas de mantenimiento y CLI
│       ├── query_cli.py         # Interfaz interactiva de consulta
│       ├── view_logs.py         # Visualizador de logs en BD
│       └── clean_database.py    # Scripts de limpieza selectiva
├── ingestion/                   # Pipeline de Datos (ETL)
│   ├── cli.py                   # Punto de entrada unificado
│   ├── core.py                  # Lógica de ciclos (Games, Seasons)
│   ├── parallel.py              # Orquestador de multiprocessing
│   ├── api_client.py            # Wrapper de nba_api con backoff
│   ├── models_sync.py           # Sincronización de Biografías y Premios
│   └── checkpoints.py           # Persistencia del progreso
├── web/                         # Aplicación Web (FastAPI)
│   ├── app.py                   # Configuración y servidor
│   ├── routes/                  # Controladores por módulo (Pencil, Teams, etc.)
│   ├── templates/               # Vistas Jinja2 modulares
│   └── pencil_logic.py          # Lógica de validación del juego
├── tests/                       # Suite de pruebas unitarias
├── scripts/                     # Utilidades de despliegue e inicialización
└── docker-compose.yml           # Infraestructura PostgreSQL
```

## 🛠 Instalación y Configuración

### 1. Preparar Entorno
```bash
git clone <repo-url>
cd Dateados
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Infraestructura y Base de Datos
1. Levantar PostgreSQL: `docker-compose up -d`
2. Crear archivo `.env`:
   ```env
   DATABASE_URL=postgresql://nba:nba@localhost:5432/nba_stats
   ```
3. Inicializar esquema: `python -m ingestion.cli --init-db`

## 📊 Manual de Operaciones

### Ingesta de Datos
- **Incremental (Diario)**: `python -m ingestion.cli --mode incremental --limit-seasons 3` (Procesa temporadas recientes en paralelo).
- **Histórico (Full)**: `python -m ingestion.cli --mode full --start-season 1983-84` (Inicia descarga masiva).
- **Reanudación**: `python -m ingestion.cli --mode full --resume` (Continúa tras una interrupción).

### Monitoreo
- **Ver Logs**: `python -m db.utils.view_logs --limit 100 --level ERROR`
- **Resumen BD**: `python -m db.utils.query_cli --summary`

### Servidor Web
```bash
uvicorn web.app:app --reload --reload-dir web --reload-dir db --port 8000
```
Acceso: **http://localhost:8000**
