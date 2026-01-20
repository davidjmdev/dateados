"""Utilidad para mostrar un resumen del número de registros en la base de datos.

Este módulo proporciona funciones simples para obtener y mostrar
el conteo de registros en cada tabla de la base de datos.
"""

import sys
from pathlib import Path
from typing import Dict

# Agregar el directorio raíz al PYTHONPATH
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from db import get_session
from db.models import (
    Team, Player, Game, PlayerGameStats, TeamGameStats,
    PlayerTeamSeason, AnomalyScore, PlayerAward
)


def get_record_counts() -> Dict[str, int]:
    """Obtiene el número de registros en cada tabla de la base de datos.
    
    Returns:
        Diccionario con el nombre de la tabla como clave y el conteo como valor
    """
    session = get_session()
    try:
        return {
            'teams': session.query(Team).count(),
            'players': session.query(Player).count(),
            'games': session.query(Game).count(),
            'player_game_stats': session.query(PlayerGameStats).count(),
            'team_game_stats': session.query(TeamGameStats).count(),
            'player_team_seasons': session.query(PlayerTeamSeason).count(),
            'player_awards': session.query(PlayerAward).count(),
            'ml_anomaly_scores': session.query(AnomalyScore).count(),
        }
    finally:
        session.close()


def print_summary():
    """Imprime un resumen visual del número de registros en cada tabla."""
    counts = get_record_counts()
    
    # Calcular el total
    total = sum(counts.values())
    
    # Encontrar el ancho máximo del nombre de tabla para alineación
    max_table_name_width = max(len(name) for name in counts.keys())
    
    print("\n" + "=" * 70)
    print("RESUMEN DE REGISTROS EN LA BASE DE DATOS")
    print("=" * 70)
    
    for table_name, count in sorted(counts.items()):
        # Formatear el nombre de la tabla de forma más legible
        display_name = table_name.replace('_', ' ').title()
        print(f"  {display_name:<{max_table_name_width + 5}} {count:>12,}")
    
    print("-" * 70)
    print(f"  {'TOTAL':<{max_table_name_width + 5}} {total:>12,}")
    print("=" * 70 + "\n")


def get_summary_string() -> str:
    """Retorna un resumen del número de registros como string.
    
    Returns:
        String con el resumen formateado
    """
    counts = get_record_counts()
    total = sum(counts.values())
    
    max_table_name_width = max(len(name) for name in counts.keys())
    
    lines = []
    lines.append("=" * 70)
    lines.append("RESUMEN DE REGISTROS EN LA BASE DE DATOS")
    lines.append("=" * 70)
    
    for table_name, count in sorted(counts.items()):
        display_name = table_name.replace('_', ' ').title()
        lines.append(f"  {display_name:<{max_table_name_width + 5}} {count:>12,}")
    
    lines.append("-" * 70)
    lines.append(f"  {'TOTAL':<{max_table_name_width + 5}} {total:>12,}")
    lines.append("=" * 70)
    
    return "\n".join(lines)
