import re
from typing import Optional

def height_to_cm(height_str: Optional[str]) -> Optional[int]:
    """Convierte altura de pies-pulgadas (6-9) a centímetros."""
    if not height_str or '-' not in height_str:
        return None
    try:
        parts = height_str.split('-')
        feet = int(parts[0])
        inches = int(parts[1])
        total_inches = (feet * 12) + inches
        return int(round(total_inches * 2.54))
    except (ValueError, IndexError):
        return None

def lbs_to_kg(lbs: Optional[int]) -> Optional[int]:
    """Convierte peso de libras a kilogramos."""
    if lbs is None or lbs <= 0:
        return None
    try:
        return int(round(lbs * 0.453592))
    except (ValueError, TypeError):
        return None
