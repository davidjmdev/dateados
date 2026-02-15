"""Herramientas MCP para consultas de partidos NBA.

Tools:
- search_games: Buscar partidos con filtros
- get_game_details: Detalles completos de un partido (boxscore)
- search_high_scoring_games: Buscar partidos por puntuación total
"""

from typing import Optional
from datetime import date

from mcp.server.fastmcp import FastMCP

from db.query import get_games, get_game_details, search_games_by_score as _search_games_by_score
from mcp_server.serializers import to_json, serialize_game, round_floats


def register_game_tools(mcp: FastMCP) -> None:
    """Registra todas las herramientas de partidos en el servidor MCP."""

    @mcp.tool()
    def search_games(
        season: Optional[str] = None,
        team_id: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        finished_only: bool = False,
        game_type: Optional[str] = None,
        limit: int = 20,
    ) -> str:
        """Busca partidos NBA con filtros opcionales.

        Args:
            season: Temporada (ej: "2024-25")
            team_id: ID del equipo para filtrar
            start_date: Fecha inicio en formato YYYY-MM-DD
            end_date: Fecha fin en formato YYYY-MM-DD
            finished_only: Si True, solo partidos finalizados
            game_type: Tipo de partido ("rs"=Regular Season, "po"=Playoffs,
                       "pi"=PlayIn, "ist"=NBA Cup)
            limit: Máximo de resultados (default: 20, max: 100)

        Returns:
            JSON con lista de partidos encontrados
        """
        limit = min(limit, 100)

        parsed_start = date.fromisoformat(start_date) if start_date else None
        parsed_end = date.fromisoformat(end_date) if end_date else None

        games = get_games(
            season=season,
            team_id=team_id,
            start_date=parsed_start,
            end_date=parsed_end,
            finished_only=finished_only,
            game_type=game_type,
            limit=limit,
        )
        data = [serialize_game(g) for g in games]
        return to_json({"count": len(data), "games": data})

    @mcp.tool()
    def get_game_boxscore(game_id: str) -> str:
        """Obtiene el boxscore completo de un partido: marcadores, stats de
        jugadores y stats de equipos.

        Args:
            game_id: ID del partido (ej: "0022400001")

        Returns:
            JSON con info del partido, estadísticas de cada jugador
            y estadísticas agregadas de cada equipo
        """
        data = get_game_details(game_id)
        if not data:
            return to_json({"error": f"Partido {game_id} no encontrado"})
        return to_json(round_floats(data))

    @mcp.tool()
    def search_high_scoring_games(
        min_total: Optional[int] = None,
        max_total: Optional[int] = None,
        season: Optional[str] = None,
        limit: int = 10,
    ) -> str:
        """Busca partidos por puntuación total combinada (ambos equipos).

        Útil para encontrar los partidos más anotadores o de menor puntuación.
        Ordena por puntuación total descendente.

        Args:
            min_total: Puntuación mínima combinada (ej: 300 para partidos muy anotadores)
            max_total: Puntuación máxima combinada (ej: 150 para partidos defensivos)
            season: Temporada opcional para filtrar (ej: "2024-25")
            limit: Número máximo de resultados (default: 10, max: 50)

        Returns:
            JSON con partidos ordenados por puntuación total descendente
        """
        limit = min(limit, 50)
        games = _search_games_by_score(
            min_total=min_total,
            max_total=max_total,
            season=season,
            limit=limit,
        )
        data = [serialize_game(g) for g in games]
        return to_json({"count": len(data), "games": data})
