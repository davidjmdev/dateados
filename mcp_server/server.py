"""Servidor MCP para Dateados NBA Stats System.

Crea una instancia de FastMCP y registra todas las herramientas
de consulta read-only sobre la base de datos NBA.

24 herramientas agrupadas en 5 módulos:
- players (9): search, career, highs, awards, season avg, game log, teammates, rankings, award leaders
- teams (3): search, standings/record, roster
- games (3): search, boxscore, high-scoring search
- seasons (6): leaders, db stats, league standings, available seasons, playoffs bracket, nba cup bracket
- outliers (3): league outliers, player outliers, active streaks
"""

from mcp.server.fastmcp import FastMCP

from mcp_server.tools.players import register_player_tools
from mcp_server.tools.teams import register_team_tools
from mcp_server.tools.games import register_game_tools
from mcp_server.tools.seasons import register_season_tools
from mcp_server.tools.outliers import register_outlier_tools


def create_server() -> FastMCP:
    """Crea y configura el servidor MCP con todas las herramientas.
    
    Returns:
        FastMCP: Instancia del servidor configurada y lista para ejecutar
    """
    mcp = FastMCP(
        "Dateados NBA Stats",
        instructions=(
            "Servidor MCP de estadísticas NBA. Proporciona acceso read-only "
            "a datos de jugadores, equipos, partidos, temporadas, "
            "detección de anomalías (outliers) y rachas históricas. "
            "Los datos abarcan desde 1983 hasta la temporada actual. "
            "Usa las herramientas search_* para buscar y get_* para obtener detalles."
        ),
    )

    # Registrar todas las herramientas
    register_player_tools(mcp)
    register_team_tools(mcp)
    register_game_tools(mcp)
    register_season_tools(mcp)
    register_outlier_tools(mcp)

    return mcp
