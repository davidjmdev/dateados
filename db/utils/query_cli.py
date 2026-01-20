#!/usr/bin/env python3
"""CLI interactivo para consultar la base de datos NBA.

Este script proporciona una interfaz de línea de comandos para consultar
información de la base de datos de manera fácil.

Uso:
    python -m db.utils.query_cli
    python -m db.utils.query_cli --summary
    python -m db.utils.query_cli --team LAL
    python -m db.utils.query_cli --player "LeBron James"
    python -m db.utils.query_cli --game 0022300123
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional

# Agregar el directorio raíz al PYTHONPATH
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from db.query import (
    get_teams,
    get_players,
    get_games,
    get_player_stats,
    get_player_season_averages,
    get_top_players,
    get_team_record,
    get_game_details,
    search_games_by_score
)
from db.summary import print_summary
from db import Team, Player, get_session


def print_teams(conference: Optional[str] = None, division: Optional[str] = None):
    """Imprime lista de equipos."""
    teams = get_teams(conference=conference, division=division)
    print(f"\n{'=' * 80}")
    print(f"EQUIPOS" + (f" - {conference}" if conference else "") + 
          (f" - {division}" if division else ""))
    print("=" * 80)
    print(f"{'ID':<5} {'Nombre':<30} {'Abrev':<8} {'Conferencia':<12} {'División':<20}")
    print("-" * 80)
    for team in teams:
        print(f"{team.id:<5} {team.full_name:<30} {team.abbreviation:<8} "
              f"{team.conference or 'N/A':<12} {team.division or 'N/A':<20}")
    print(f"{'=' * 80}\nTotal: {len(teams)} equipos\n")


def print_players(name: Optional[str] = None, position: Optional[str] = None,
                 active_only: bool = False):
    """Imprime lista de jugadores."""
    players = get_players(name=name, position=position, active_only=active_only)
    print(f"\n{'=' * 110}")
    print(f"JUGADORES" + 
          (f" - Nombre: {name}" if name else "") +
          (f" - Posición: {position}" if position else "") +
          (f" - Solo activos" if active_only else ""))
    print("=" * 110)
    print(f"{'ID':<10} {'Nombre':<35} {'Posición':<15} {'Altura':<10} {'País':<20} {'Carrera'}")
    print("-" * 110)
    for player in players:
        years = f"{player.from_year or '?'}-{player.to_year or 'Presente'}"
        print(f"{player.id:<10} {player.full_name:<35} {player.position or 'N/A':<15} "
              f"{player.height or 'N/A':<10} {player.country or 'N/A':<20} {years}")
    print(f"{'=' * 110}\nTotal: {len(players)} jugadores\n")


def print_games(season: Optional[str] = None, team: Optional[str] = None,
               limit: int = 20):
    """Imprime lista de partidos."""
    team_id = None
    if team:
        session = get_session()
        try:
            team_obj = session.query(Team).filter(
                Team.abbreviation.ilike(f"%{team}%")
            ).first()
            if team_obj:
                team_id = team_obj.id
            else:
                print(f"⚠️  No se encontró el equipo: {team}")
                return
        finally:
            session.close()
    
    games = get_games(season=season, team_id=team_id, finished_only=True, limit=limit)
    print(f"\n{'=' * 100}")
    print(f"PARTIDOS" + 
          (f" - Temporada: {season}" if season else "") +
          (f" - Equipo: {team}" if team else ""))
    print("=" * 100)
    print(f"{'ID':<15} {'Fecha':<12} {'Temporada':<12} {'Local':<25} {'Visitante':<25} {'Resultado'}")
    print("-" * 100)
    for game in games:
        home_name = game.home_team.full_name if game.home_team else "N/A"
        away_name = game.away_team.full_name if game.away_team else "N/A"
        result = f"{game.home_score or 0}-{game.away_score or 0}" if game.home_score else "N/A"
        date_str = game.date.strftime("%Y-%m-%d") if game.date else "N/A"
        print(f"{game.id:<15} {date_str:<12} {game.season:<12} "
              f"{home_name:<25} {away_name:<25} {result}")
    print(f"{'=' * 100}\nTotal: {len(games)} partidos\n")


def print_player_stats(player_name: str, season: Optional[str] = None):
    """Imprime estadísticas de un jugador."""
    from sqlalchemy import and_, or_
    from sqlalchemy.orm import joinedload
    
    session = get_session()
    try:
        words = player_name.strip().split()
        
        # Si hay múltiples palabras, buscar jugadores que contengan TODAS las palabras
        if len(words) > 1:
            filters = [Player.full_name.ilike(f"%{word}%") for word in words]
            players = session.query(Player).filter(
                and_(*filters)
            ).all()
            
            # Si no hay resultados con todas las palabras, intentar búsqueda con apellido
            # (última palabra, que suele ser el apellido)
            if not players and len(words) >= 2:
                last_name = words[-1]
                first_name_initial = words[0][0] if words[0] else ""
                # Buscar por apellido y posible inicial del nombre (ej: "M. Jordan" para "Michael Jordan")
                players = session.query(Player).filter(
                    and_(
                        Player.full_name.ilike(f"%{last_name}%"),
                        or_(
                            Player.full_name.ilike(f"%{first_name_initial}.%"),
                            Player.full_name.ilike(f"%{first_name_initial} %"),
                            Player.full_name.ilike(f"{first_name_initial}.%"),
                            Player.full_name.ilike(f"{first_name_initial} %")
                        )
                    )
                ).all()
            
            # Si aún no hay resultados, intentar búsqueda parcial con la cadena completa
            if not players:
                players = session.query(Player).filter(
                    Player.full_name.ilike(f"%{player_name}%")
                ).all()
        else:
            # Búsqueda simple con una palabra
            players = session.query(Player).filter(
                Player.full_name.ilike(f"%{player_name}%")
            ).all()
        
        # Si aún no hay resultados, mostrar sugerencias
        if not players:
            print(f"⚠️  No se encontró el jugador: {player_name}")
            print(f"\n💡 Sugerencia: Intenta buscar con:")
            if words:
                print(f"   python -m db.utils.query_cli --players --name \"{words[0]}\"")
            return
        
        # Si hay múltiples resultados, mostrar lista para seleccionar
        if len(players) > 1:
            print(f"\n{'=' * 80}")
            print(f"Se encontraron {len(players)} jugadores con nombre similar a '{player_name}':")
            print("=" * 80)
            print(f"{'#':<4} {'ID':<8} {'Nombre':<40} {'Posición':<15}")
            print("-" * 80)
            for i, p in enumerate(players[:20], 1):  # Limitar a 20 resultados
                print(f"{i:<4} {p.id:<8} {p.full_name:<40} {p.position or 'N/A':<15}")
            if len(players) > 20:
                print(f"\n... y {len(players) - 20} más. Usa --players --name para ver todos.")
            print("=" * 80)
            print(f"\n💡 Para ver estadísticas de un jugador específico, usa:")
            print(f"   python -m db.utils.query_cli --player \"<nombre exacto>\"")
            print(f"   o busca primero con: --players --name \"{player_name}\"")
            return
        
        # Si hay solo un resultado, usarlo
        player = players[0]
        
        print(f"\n{'=' * 80}")
        print(f"JUGADOR: {player.full_name}")
        print("=" * 80)
        print(f"ID: {player.id}")
        print(f"Posición: {player.position or 'N/A'}")
        print(f"Altura: {player.height or 'N/A'}")
        print(f"País: {player.country or 'N/A'}")
        print(f"Experiencia: {player.experience} {'temporada' if player.experience == 1 else 'temporadas'}")
        
        if season:
            averages = get_player_season_averages(player.id, season)
            if averages:
                print(f"\nPromedios - Temporada {season}:")
                print(f"  Partidos: {averages['games']}")
                print(f"  Puntos: {averages['pts']:.1f}")
                print(f"  Rebotes: {averages['reb']:.1f}")
                print(f"  Asistencias: {averages['ast']:.1f}")
                print(f"  Robos: {averages['stl']:.1f}")
                print(f"  Tapones: {averages['blk']:.1f}")
                print(f"  % TC: {averages['fg_pct']:.3f}")
                print(f"  % 3P: {averages['fg3_pct']:.3f}")
                print(f"  % TL: {averages['ft_pct']:.3f}")
            else:
                print(f"\n⚠️  No hay datos para la temporada {season}")
        else:
            # Mostrar últimas 10 estadísticas usando la función de utilidad optimizada
            stats = get_player_stats(player_id=player.id, limit=10, session=session)
            
            if stats:
                print(f"\nÚltimos 10 partidos:")
                print(f"{'Fecha':<12} {'Equipo':<8} {'PTS':<5} {'REB':<5} {'AST':<5} {'STL':<5} {'BLK':<5}")
                print("-" * 60)
                for stat in stats:
                    game_date = stat.game.date.strftime("%Y-%m-%d") if stat.game and stat.game.date else "N/A"
                    team_abbr = stat.team.abbreviation if stat.team else "N/A"
                    print(f"{game_date:<12} {team_abbr:<8} {stat.pts:<5} {stat.reb:<5} "
                          f"{stat.ast:<5} {stat.stl:<5} {stat.blk:<5}")
            else:
                print("\n⚠️  No hay estadísticas disponibles")
        
        print("=" * 80 + "\n")
    finally:
        session.close()


def print_team_record(team: str, season: Optional[str] = None):
    """Imprime récord de un equipo."""
    session = get_session()
    try:
        team_obj = session.query(Team).filter(
            Team.abbreviation.ilike(f"%{team}%")
        ).first()
        
        if not team_obj:
            print(f"⚠️  No se encontró el equipo: {team}")
            return
        
        record = get_team_record(team_obj.id, season=season)
        print(f"\n{'=' * 60}")
        print(f"EQUIPO: {team_obj.full_name} ({team_obj.abbreviation})")
        if season:
            print(f"TEMPORADA: {season}")
        print("=" * 60)
        print(f"Victorias: {record['wins']}")
        print(f"Derrotas: {record['losses']}")
        print(f"Total: {record['total']}")
        print(f"Porcentaje de victorias: {record['win_percentage']:.3f}")
        print("=" * 60 + "\n")
    finally:
        session.close()


def print_game_details(game_id: str):
    """Imprime detalles completos de un partido."""
    details = get_game_details(game_id)
    
    if not details:
        print(f"⚠️  No se encontró el partido: {game_id}")
        return
    
    game = details['game']
    print(f"\n{'=' * 80}")
    print(f"PARTIDO: {game['id']}")
    print("=" * 80)
    print(f"Fecha: {game['date']}")
    print(f"Temporada: {game['season']}")
    print(f"{game['away_team']} @ {game['home_team']}")
    print(f"Resultado: {game['away_score']} - {game['home_score']}")
    
    if game['quarter_scores']:
        print(f"\nMarcadores por cuarto:")
        home_scores = game['quarter_scores'].get('home', [])
        away_scores = game['quarter_scores'].get('away', [])
        quarters = max(len(home_scores), len(away_scores))
        for i in range(quarters):
            home = home_scores[i] if i < len(home_scores) else 0
            away = away_scores[i] if i < len(away_scores) else 0
            q_name = f"Q{i+1}" if i < 4 else f"OT{i-3}"
            print(f"  {q_name}: {away} - {home}")
    
    if details['player_stats']:
        print(f"\nTop 10 Jugadores:")
        print(f"{'Jugador':<25} {'Equipo':<8} {'PTS':<5} {'REB':<5} {'AST':<5} {'STL':<5} {'BLK':<5}")
        print("-" * 70)
        for stat in details['player_stats'][:10]:
            print(f"{stat['player']:<25} {stat['team']:<8} {stat['pts']:<5} "
                  f"{stat['reb']:<5} {stat['ast']:<5} {stat['stl']:<5} {stat['blk']:<5}")
    
    print("=" * 80 + "\n")


def print_top_players(stat: str = 'pts', season: Optional[str] = None, limit: int = 10):
    """Imprime los mejores jugadores por estadística."""
    players = get_top_players(stat=stat, season=season, limit=limit)
    print(f"\n{'=' * 70}")
    print(f"TOP {limit} JUGADORES - {stat.upper()}" + 
          (f" - Temporada: {season}" if season else ""))
    print("=" * 70)
    print(f"{'#':<4} {'Jugador':<30} {'Promedio':<12} {'Partidos':<10}")
    print("-" * 70)
    for i, player in enumerate(players, 1):
        avg_key = f'avg_{stat}'
        print(f"{i:<4} {player['full_name']:<30} {player[avg_key]:<12.2f} {player['games']:<10}")
    print("=" * 70 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='CLI para consultar la base de datos NBA',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python -m db.utils.query_cli --summary
  python -m db.utils.query_cli --teams
  python -m db.utils.query_cli --teams --conference West
  python -m db.utils.query_cli --players --name "LeBron"
  python -m db.utils.query_cli --player "LeBron James" --season "2023-24"
  python -m db.utils.query_cli --team LAL --season "2023-24"
  python -m db.utils.query_cli --games --season "2023-24" --limit 10
  python -m db.utils.query_cli --game 0022300123
  python -m db.utils.query_cli --top pts --season "2023-24"
        """
    )
    
    parser.add_argument('--summary', action='store_true',
                       help='Mostrar resumen de registros en la BD')
    parser.add_argument('--teams', action='store_true',
                       help='Listar todos los equipos')
    parser.add_argument('--conference', type=str, choices=['East', 'West'],
                       help='Filtrar equipos por conferencia')
    parser.add_argument('--division', type=str,
                       help='Filtrar equipos por división')
    parser.add_argument('--players', action='store_true',
                       help='Listar jugadores')
    parser.add_argument('--player', type=str,
                       help='Mostrar estadísticas de un jugador')
    parser.add_argument('--name', type=str,
                       help='Buscar jugadores por nombre')
    parser.add_argument('--position', type=str,
                       help='Filtrar jugadores por posición')
    parser.add_argument('--active-only', action='store_true',
                       help='Solo jugadores activos')
    parser.add_argument('--games', action='store_true',
                       help='Listar partidos')
    parser.add_argument('--game', type=str,
                       help='Mostrar detalles de un partido (ID)')
    parser.add_argument('--team', type=str,
                       help='Filtrar por equipo (abreviatura)')
    parser.add_argument('--season', type=str,
                       help='Filtrar por temporada (ej: 2023-24)')
    parser.add_argument('--top', type=str, choices=['pts', 'reb', 'ast', 'stl', 'blk'],
                       help='Mostrar top jugadores por estadística')
    parser.add_argument('--limit', type=int, default=20,
                       help='Límite de resultados (default: 20)')
    
    args = parser.parse_args()
    
    # Si no hay argumentos, mostrar ayuda
    if len(sys.argv) == 1:
        parser.print_help()
        return
    
    try:
        if args.summary:
            print_summary()
        
        if args.teams:
            print_teams(conference=args.conference, division=args.division)
        
        if args.players:
            print_players(name=args.name, position=args.position, active_only=args.active_only)
        
        if args.player:
            print_player_stats(args.player, season=args.season)
        
        if args.games:
            print_games(season=args.season, team=args.team, limit=args.limit)
        
        if args.game:
            print_game_details(args.game)
        
        if args.team and not args.games:
            print_team_record(args.team, season=args.season)
        
        if args.top:
            print_top_players(stat=args.top, season=args.season, limit=args.limit)
    
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
