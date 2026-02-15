"""Herramientas MCP para consultas de jugadores NBA.

Tools:
- search_players: Buscar jugadores con filtros
- get_player_info: Info detallada de un jugador
- get_player_career: Estadísticas de carrera
- get_player_career_highs: Récords personales
- get_player_awards: Premios y reconocimientos
- get_player_season_avg: Promedios por temporada
- get_player_game_log: Últimos partidos de un jugador
- get_player_rankings: Ranking de jugadores por criterio (edad, peso, etc.)
- get_award_leaders: Jugadores con más premios de un tipo
"""

from typing import Optional

from mcp.server.fastmcp import FastMCP

from db.query import (
    get_players,
    get_player_career_stats,
    get_player_career_highs,
    get_player_awards,
    get_player_season_averages,
    get_player_stats,
    get_current_teammates,
    get_historical_teammates,
    get_player_rankings as _get_player_rankings,
    get_award_leaders as _get_award_leaders,
)
from mcp_server.serializers import (
    to_json,
    serialize_player,
    serialize_player_game_stats,
    round_floats,
)


def register_player_tools(mcp: FastMCP) -> None:
    """Registra todas las herramientas de jugadores en el servidor MCP."""

    @mcp.tool()
    def search_players(
        name: Optional[str] = None,
        position: Optional[str] = None,
        active_only: bool = False,
        team_id: Optional[int] = None,
        season: Optional[str] = None,
    ) -> str:
        """Busca jugadores NBA con filtros opcionales.

        Args:
            name: Nombre parcial del jugador (ej: "LeBron", "Curry")
            position: Posición (ej: "Guard", "Forward", "Center")
            active_only: Si True, solo jugadores activos
            team_id: ID del equipo para filtrar
            season: Temporada (ej: "2024-25")

        Returns:
            JSON con lista de jugadores encontrados
        """
        players = get_players(
            name=name,
            position=position,
            active_only=active_only,
            team_id=team_id,
            season=season,
        )
        data = [serialize_player(p) for p in players]
        return to_json({"count": len(data), "players": data})

    @mcp.tool()
    def get_player_career(player_id: int) -> str:
        """Obtiene estadísticas completas de carrera de un jugador.

        Incluye promedios recientes (últimos 7 días, último mes),
        historial por temporada (Regular Season, Playoffs, NBA Cup),
        y totales de carrera.

        Args:
            player_id: ID del jugador NBA

        Returns:
            JSON con estadísticas de carrera desglosadas
        """
        data = get_player_career_stats(player_id)
        if not data:
            return to_json({"error": f"No se encontraron datos para player_id={player_id}"})

        # Serializar los game stats de los periodos recientes
        for period in ('last_7_days', 'last_month'):
            if data.get(period) and data[period].get('games'):
                data[period]['games'] = [
                    serialize_player_game_stats(s) for s in data[period]['games']
                ]

        # Limpiar campos internos (_total_*) de los registros por temporada
        for key in ('regular_season', 'playoffs', 'ist'):
            if data.get(key):
                data[key] = [
                    {k: v for k, v in record.items() if not k.startswith('_')}
                    for record in data[key]
                ]

        return to_json(round_floats(data))

    @mcp.tool()
    def get_player_highs(player_id: int) -> str:
        """Obtiene los récords personales (career highs) de un jugador.

        Incluye máximos en puntos, rebotes, asistencias, robos, tapones,
        triples, tiros de campo, tiros libres, minutos y plus/minus.
        También incluye conteos de doble-dobles, triple-dobles y
        partidos con 40+, 50+, 60+ puntos.

        Args:
            player_id: ID del jugador NBA

        Returns:
            JSON con récords personales y detalles del partido donde ocurrieron
        """
        data = get_player_career_highs(player_id)
        # Serializar dates dentro de los high details
        result = {}
        for key, value in data.items():
            if isinstance(value, dict) and 'date' in value:
                value['date'] = value['date'].isoformat() if value['date'] else None
            result[key] = value
        return to_json(round_floats(result))

    @mcp.tool()
    def get_player_awards_list(player_id: int) -> str:
        """Obtiene todos los premios y reconocimientos de un jugador.

        Incluye MVPs, campeonatos, All-Star, All-NBA, DPOY, ROY,
        medallas olímpicas, y más, agrupados por tipo.

        Args:
            player_id: ID del jugador NBA

        Returns:
            JSON con premios agrupados por tipo y conteo
        """
        data = get_player_awards(player_id)
        if not data:
            return to_json({"message": "No se encontraron premios", "awards": []})
        return to_json({"count": sum(a['count'] for a in data), "awards": data})

    @mcp.tool()
    def get_player_season_avg(player_id: int, season: str) -> str:
        """Obtiene los promedios de un jugador en una temporada específica.

        Calcula PPG, RPG, APG, SPG, BPG, TOV, MPG y porcentajes de tiro
        para Regular Season.

        Args:
            player_id: ID del jugador NBA
            season: Temporada (ej: "2024-25", "2023-24")

        Returns:
            JSON con promedios de la temporada
        """
        data = get_player_season_averages(player_id, season)
        if not data:
            return to_json({"error": f"Sin datos para player_id={player_id} en {season}"})
        return to_json(round_floats(data))

    @mcp.tool()
    def get_player_game_log(
        player_id: int,
        season: Optional[str] = None,
        limit: int = 10,
    ) -> str:
        """Obtiene los últimos partidos de un jugador con sus estadísticas.

        Args:
            player_id: ID del jugador NBA
            season: Temporada opcional para filtrar (ej: "2024-25")
            limit: Número máximo de partidos (default: 10, max: 50)

        Returns:
            JSON con lista de game stats ordenados por fecha descendente
        """
        limit = min(limit, 50)
        stats = get_player_stats(
            player_id=player_id,
            season=season,
            limit=limit,
            order_by_date=True,
        )
        data = [serialize_player_game_stats(s) for s in stats]
        return to_json({"count": len(data), "games": data})

    @mcp.tool()
    def get_player_teammates(
        player_id: int,
        historical: bool = False,
    ) -> str:
        """Obtiene los compañeros de equipo de un jugador.

        Args:
            player_id: ID del jugador NBA
            historical: Si True, retorna todos los compañeros históricos.
                        Si False (default), solo los del equipo actual.

        Returns:
            JSON con lista de compañeros (nombre, posición, temporadas juntos si histórico)
        """
        if historical:
            data = get_historical_teammates(player_id)
        else:
            data = get_current_teammates(player_id)

        return to_json({"count": len(data), "teammates": data})

    @mcp.tool()
    def get_player_rankings(
        criteria: str,
        active_only: bool = True,
        limit: int = 10,
    ) -> str:
        """Obtiene un ranking de jugadores según un criterio biográfico o físico.

        Permite responder preguntas como "¿quién es el jugador más joven?",
        "¿quién es el más alto en activo?", "¿quién tiene más experiencia?".

        Args:
            criteria: Criterio de ranking. Opciones:
                      "youngest" = más jóvenes
                      "oldest" = más veteranos por edad
                      "heaviest" = más pesados
                      "lightest" = más ligeros
                      "tallest" = más altos
                      "shortest" = más bajos
                      "most_experienced" = más temporadas de experiencia
                      "highest_draft_pick" = picks más altos (número más bajo)
                      "lowest_draft_pick" = picks más bajos (número más alto)
            active_only: Si True (default), solo jugadores activos
            limit: Número de resultados (default: 10, max: 50)

        Returns:
            JSON con ranking de jugadores según el criterio
        """
        valid_criteria = {
            'youngest', 'oldest', 'heaviest', 'lightest',
            'tallest', 'shortest', 'most_experienced',
            'highest_draft_pick', 'lowest_draft_pick'
        }
        if criteria not in valid_criteria:
            return to_json({
                "error": f"Criterio '{criteria}' no válido",
                "valid_criteria": sorted(valid_criteria)
            })

        limit = min(limit, 50)
        data = _get_player_rankings(
            criteria=criteria,
            active_only=active_only,
            limit=limit,
        )
        return to_json({
            "criteria": criteria,
            "active_only": active_only,
            "count": len(data),
            "players": data,
        })

    @mcp.tool()
    def get_award_leaders(
        award_type: Optional[str] = None,
        active_only: bool = False,
        limit: int = 10,
    ) -> str:
        """Obtiene los jugadores con más premios de un tipo específico.

        Permite responder preguntas como "¿quién tiene más MVPs?",
        "¿qué jugador activo tiene más campeonatos?",
        "¿quién tiene más selecciones All-Star?".

        Args:
            award_type: Tipo de premio a filtrar. Opciones:
                        "MVP", "Champion", "All-Star", "All-NBA",
                        "DPOY", "Finals MVP", "ROY", "6MOY", "MIP",
                        "All-Defensive", "All-Rookie", "All-Star MVP",
                        "NBA Cup", "NBA Cup MVP", "Olympic Gold",
                        "Olympic Silver", "Olympic Bronze".
                        Si no se especifica, cuenta todos los premios.
            active_only: Si True, solo jugadores activos (default: False)
            limit: Número de resultados (default: 10, max: 50)

        Returns:
            JSON con ranking de jugadores por cantidad de premios
        """
        limit = min(limit, 50)
        
        # Normalizar award_type (case-insensitive)
        valid_award_types = {
            'mvp': 'MVP', 'champion': 'Champion', 'all-star': 'All-Star',
            'all-nba': 'All-NBA', 'dpoy': 'DPOY', 'finals mvp': 'Finals MVP',
            'roy': 'ROY', '6moy': '6MOY', 'mip': 'MIP',
            'all-defensive': 'All-Defensive', 'all-rookie': 'All-Rookie',
            'all-star mvp': 'All-Star MVP', 'nba cup': 'NBA Cup',
            'nba cup mvp': 'NBA Cup MVP', 'olympic gold': 'Olympic Gold',
            'olympic silver': 'Olympic Silver', 'olympic bronze': 'Olympic Bronze',
        }
        
        if award_type:
            normalized = valid_award_types.get(award_type.lower())
            if not normalized:
                return to_json({
                    "error": f"Tipo de premio '{award_type}' no válido",
                    "valid_award_types": sorted(set(valid_award_types.values()))
                })
            award_type = normalized
        
        data = _get_award_leaders(
            award_type=award_type,
            active_only=active_only,
            limit=limit,
        )
        return to_json({
            "award_type": award_type or "all",
            "active_only": active_only,
            "count": len(data),
            "leaders": data,
        })
