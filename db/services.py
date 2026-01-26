from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from db.models import Team, Player
from db.constants import SPECIAL_EVENT_TEAM_IDS

def is_valid_team_id(team_id: int, allow_special_events: bool = False, session: Optional[Session] = None) -> bool:
    """Verifica si un team_id es válido."""
    if allow_special_events and team_id in SPECIAL_EVENT_TEAM_IDS: return True
    if session and session.query(Team).filter(Team.id == team_id).first(): return True
    try:
        from nba_api.stats.static import teams as nba_teams_static
        if any(t['id'] == team_id for t in nba_teams_static.get_teams()): return True
    except: pass
    return 1610612737 <= team_id <= 1610612766


def get_or_create_team(session: Session, team_id: int, team_data: Optional[Dict[str, Any]] = None) -> Team:
    """Obtiene un equipo de la BD o lo crea si no existe."""
    team = session.query(Team).filter(Team.id == team_id).first()
    if team:
        if team_data:
            for k, v in team_data.items():
                if v and hasattr(team, k): setattr(team, k, v)
        return team
    
    # Intento de creación atómico para evitar race conditions
    savepoint = session.begin_nested()
    try:
        final_data = team_data.copy() if team_data else {}
        if not final_data.get('full_name') or not final_data.get('abbreviation'):
            try:
                from nba_api.stats.static import teams as nba_teams_static
                for t in nba_teams_static.get_teams():
                    if t['id'] == team_id:
                        if not final_data.get('full_name'): final_data['full_name'] = t['full_name']
                        if not final_data.get('abbreviation'): final_data['abbreviation'] = t['abbreviation']
                        if not final_data.get('city'): final_data['city'] = t['city']
                        if not final_data.get('nickname'): final_data['nickname'] = t['nickname']
                        break
            except: pass

        if not final_data.get('abbreviation'):
            final_data['abbreviation'] = f"TM_{team_id}"
        if not final_data.get('full_name'):
            final_data['full_name'] = f"Team {team_id}"

        new_team = Team(id=team_id, **final_data)
        session.add(new_team)
        session.flush()
        savepoint.commit()
        return new_team
    except Exception:
        savepoint.rollback()
        # Si falló, es que otro worker lo creó justo antes
        return session.query(Team).filter(Team.id == team_id).first()


def get_or_create_player(session: Session, player_id: int, player_data: Optional[Dict[str, Any]] = None) -> Player:
    """Obtiene un jugador de la BD o lo crea si no existe."""
    player = session.query(Player).filter(Player.id == player_id).first()
    
    if player:
        if player_data:
            for k, v in player_data.items():
                if v is not None and hasattr(player, k):
                    setattr(player, k, v)
        return player

    # Intento de creación atómico
    savepoint = session.begin_nested()
    try:
        # Creación rápida sin llamada a API de biografía (se delega a la fase final)
        name = player_data.get('full_name') if player_data else f'Player {player_id}'
        new_player = Player(id=player_id, full_name=name)
        
        # Si vienen datos en player_data (como jersey o posición del boxscore), los usamos
        if player_data:
            for k in ['position', 'jersey']:
                if k in player_data and player_data[k] is not None:
                    setattr(new_player, k, player_data[k])
                    
        session.add(new_player)
        session.flush()
        savepoint.commit()
        return new_player
    except Exception:
        savepoint.rollback()
        # Si falló, es que otro worker lo creó justo antes
        return session.query(Player).filter(Player.id == player_id).first()
