from fastapi.templating import Jinja2Templates
from pathlib import Path
from web.utils import height_to_cm, lbs_to_kg

# Configurar rutas de templates
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Registrar filtros personalizados
templates.env.filters["to_cm"] = height_to_cm
templates.env.filters["to_kg"] = lbs_to_kg
