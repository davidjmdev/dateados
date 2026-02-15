"""Herramientas MCP para consultas de temporadas y líderes NBA.

Tools:
- get_season_leaders: Top jugadores por estadística en una temporada
- get_db_stats: Estadísticas generales de la base de datos
- get_league_standings: Clasificación completa de ambas conferencias
- get_available_seasons: Lista de temporadas disponibles
- get_playoff_bracket: Bracket de playoffs de una temporada
- get_nba_cup_bracket: Bracket de la NBA Cup de una temporada
"""

from typing import Optional

from mcp.server.fastmcp import FastMCP

from db.query import (
    get_top_players,
    get_database_stats,
    get_all_seasons,
    get_season_standings,
    get_playoff_bracket,
    get_nba_cup_bracket,
)
from mcp_server.serializers import to_json, round_floats


def register_season_tools(mcp: FastMCP) -> None:
    """Registra las herramientas de temporadas y líderes en el servidor MCP."""

    @mcp.tool()
    def get_season_leaders(
        stat: str = "pts",
        season: Optional[str] = None,
        limit: int = 10,
    ) -> str:
        """Obtiene los líderes de la liga en una estadística específica.

        Args:
            stat: Estadística a ordenar. Opciones:
                  Promedios por partido: "pts", "reb", "ast", "stl", "blk",
                  "tov", "fgm", "fg3m", "ftm"
                  Porcentajes de tiro: "fg_pct", "fg3_pct", "ft_pct"
            season: Temporada (ej: "2024-25"). Si no se especifica,
                    usa toda la historia.
            limit: Número de jugadores (default: 10, max: 50)

        Returns:
            JSON con ranking de jugadores por la estadística
        """
        valid_stats = {
            'pts', 'reb', 'ast', 'stl', 'blk', 'tov',
            'fgm', 'fg3m', 'ftm',
            'fg_pct', 'fg3_pct', 'ft_pct',
        }
        if stat not in valid_stats:
            return to_json({
                "error": f"Estadística '{stat}' no válida",
                "valid_stats": sorted(valid_stats)
            })

        limit = min(limit, 50)
        data = get_top_players(stat=stat, season=season, limit=limit)
        return to_json({
            "stat": stat,
            "season": season or "all-time",
            "leaders": round_floats(data)
        })

    @mcp.tool()
    def get_db_stats() -> str:
        """Obtiene estadísticas generales de la base de datos Dateados.

        Retorna la cantidad de equipos, jugadores, partidos,
        estadísticas de jugadores, estadísticas de equipos,
        registros de temporada y premios almacenados.

        Returns:
            JSON con conteos de cada tabla principal
        """
        data = get_database_stats()
        return to_json(data)

    @mcp.tool()
    def get_league_standings(season: str) -> str:
        """Obtiene la clasificación completa de la liga para una temporada.

        Retorna las tablas de ambas conferencias (Este y Oeste) con
        victorias, derrotas, porcentaje y posición de cada equipo.
        Solo cuenta partidos de Regular Season finalizados.

        Args:
            season: Temporada (ej: "2023-24", "2024-25")

        Returns:
            JSON con clasificación de conferencia Este y Oeste
        """
        data = get_season_standings(season)
        if not data.get('east') and not data.get('west'):
            return to_json({
                "season": season,
                "message": "No se encontraron datos de clasificación para esta temporada",
                "east": [],
                "west": []
            })
        return to_json(round_floats(data))

    @mcp.tool()
    def get_available_seasons() -> str:
        """Obtiene la lista de todas las temporadas disponibles en la base de datos.

        Útil para saber qué temporadas se pueden consultar.

        Returns:
            JSON con lista de temporadas ordenadas de más reciente a más antigua
        """
        seasons = get_all_seasons()
        return to_json({"count": len(seasons), "seasons": seasons})

    @mcp.tool()
    def get_playoffs_bracket(season: str) -> str:
        """Obtiene el bracket de playoffs para una temporada.

        Muestra todas las series de playoffs organizadas por ronda:
        Primera Ronda, Semis de Conferencia, Finales de Conferencia y Finales NBA.
        Cada serie incluye los equipos enfrentados y el resultado (victorias de cada uno).

        Args:
            season: Temporada (ej: "2023-24", "2024-25")

        Returns:
            JSON con las rondas de playoffs y sus series
        """
        data = get_playoff_bracket(season)
        if not data:
            return to_json({
                "season": season,
                "message": "No se encontraron datos de playoffs para esta temporada",
                "rounds": []
            })
        return to_json({"season": season, "rounds": data})

    @mcp.tool()
    def get_nba_cup(season: str) -> str:
        """Obtiene el bracket de la NBA Cup (In-Season Tournament) para una temporada.

        Muestra la fase eliminatoria de la NBA Cup: Cuartos de Final,
        Semifinales y Final. Solo disponible desde la temporada 2023-24.

        Args:
            season: Temporada (ej: "2023-24", "2024-25")

        Returns:
            JSON con las rondas de la NBA Cup y sus series
        """
        data = get_nba_cup_bracket(season)
        if not data:
            return to_json({
                "season": season,
                "message": "No se encontraron datos de NBA Cup para esta temporada",
                "rounds": []
            })
        return to_json({"season": season, "rounds": data})
