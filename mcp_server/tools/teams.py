"""Herramientas MCP para consultas de equipos NBA.

Tools:
- search_teams: Buscar equipos con filtros
- get_team_record: Récord de un equipo y posición en la conferencia
- get_team_roster: Roster de un equipo en una temporada
"""

from typing import Optional

from mcp.server.fastmcp import FastMCP

from db.query import get_teams, get_team_record, get_team_roster as _get_team_roster
from mcp_server.serializers import to_json, serialize_team, round_floats


def register_team_tools(mcp: FastMCP) -> None:
    """Registra todas las herramientas de equipos en el servidor MCP."""

    @mcp.tool()
    def search_teams(
        conference: Optional[str] = None,
        division: Optional[str] = None,
    ) -> str:
        """Busca equipos NBA con filtros opcionales.

        Args:
            conference: Conferencia ("East" o "West")
            division: División ("Atlantic", "Central", "Southeast",
                      "Northwest", "Pacific", "Southwest")

        Returns:
            JSON con lista de equipos encontrados
        """
        teams = get_teams(conference=conference, division=division)
        data = [serialize_team(t) for t in teams]
        return to_json({"count": len(data), "teams": data})

    @mcp.tool()
    def get_team_standings(
        team_id: int,
        season: Optional[str] = None,
    ) -> str:
        """Obtiene el récord (victorias/derrotas) de un equipo y su posición en la conferencia.

        Args:
            team_id: ID del equipo NBA
            season: Temporada opcional (ej: "2024-25"). Si no se especifica,
                    usa toda la historia.

        Returns:
            JSON con wins, losses, win_percentage y conf_rank
        """
        data = get_team_record(team_id, season=season)
        if not data:
            return to_json({"error": f"Equipo {team_id} no encontrado"})
        return to_json(round_floats(data))

    @mcp.tool()
    def get_roster(
        team_id: int,
        season: Optional[str] = None,
    ) -> str:
        """Obtiene el roster (plantilla) de un equipo para una temporada.

        Incluye todos los jugadores que participaron con el equipo en la
        temporada especificada, con sus promedios de puntos, rebotes,
        asistencias y minutos. Deduplica jugadores que aparecen en
        múltiples competiciones (Regular Season, Playoffs, NBA Cup).

        Args:
            team_id: ID del equipo NBA
            season: Temporada (ej: "2024-25"). Si no se especifica,
                    usa la temporada más reciente del equipo.

        Returns:
            JSON con lista de jugadores del equipo y sus promedios
        """
        data = _get_team_roster(team_id, season=season)
        if not data or not data.get('players'):
            return to_json({
                "error": f"No se encontró roster para equipo {team_id}",
                "team_id": team_id,
                "season": season
            })
        return to_json(round_floats(data))
