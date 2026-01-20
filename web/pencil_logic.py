"""Lógica de negocio para el juego 'Alto el lápiz'.

Este módulo contiene las funciones para validar y filtrar jugadores
según las categorías especiales del juego.
"""

import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import func, or_, and_, distinct, desc, exists
from sqlalchemy.orm import Session, joinedload

from db.models import Player, Team, PlayerTeamSeason, PlayerAward, Game, PlayerGameStats
from db.connection import get_session

logger = logging.getLogger(__name__)

# Lista completa de países europeos (excluyendo Turquía e Israel)
EUROPEAN_COUNTRIES = [
    'Albania', 'Andorra', 'Armenia', 'Austria', 'Azerbaijan', 'Belarus', 
    'Belgium', 'Bosnia and Herzegovina', 'Bulgaria', 'Croatia', 'Cyprus', 
    'Czech Republic', 'Denmark', 'Estonia', 'Finland', 'France', 'Georgia', 
    'Germany', 'Greece', 'Hungary', 'Iceland', 'Ireland', 'Italy', 'Kazakhstan', 
    'Latvia', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Malta', 'Moldova', 
    'Monaco', 'Montenegro', 'Netherlands', 'North Macedonia', 'Norway', 
    'Poland', 'Portugal', 'Romania', 'Russia', 'San Marino', 'Serbia', 
    'Slovakia', 'Slovenia', 'Spain', 'Sweden', 'Switzerland', 'Ukraine', 
    'United Kingdom', 'Vatican City',
    # Nombres históricos o variantes que aparecen en datos de la NBA
    'USSR', 'Yugoslavia', 'Serbia and Montenegro', 'Macedonia', 'Scotland',
    'Czechia'
]

class PencilGameLogic:
    """Maneja las reglas y validaciones del juego Alto el lápiz."""

    @staticmethod
    def get_players_by_letter(session: Session, letter: str) -> List[Player]:
        """Obtiene jugadores cuyo nombre o apellido empieza por la letra."""
        l = letter.lower()
        return session.query(Player).filter(
            or_(
                func.lower(Player.full_name).like(f"{l}%"),
                func.lower(Player.full_name).like(f"% {l}%")
            )
        ).all()

    @staticmethod
    def is_champion(session: Session, player_id: int) -> bool:
        """Verifica si el jugador ha ganado al menos un campeonato."""
        return session.query(PlayerAward).filter(
            PlayerAward.player_id == player_id,
            PlayerAward.award_type == 'Champion'
        ).first() is not None

    @staticmethod
    def is_all_star(session: Session, player_id: int) -> bool:
        """Verifica si el jugador ha sido All-Star."""
        return session.query(PlayerAward).filter(
            PlayerAward.player_id == player_id,
            PlayerAward.award_type == 'All-Star'
        ).first() is not None

    @staticmethod
    def is_lottery_pick(session: Session, player_id: int) -> bool:
        """Verifica si el jugador fue elegido en el top 14 del Draft."""
        player = session.query(Player).filter(Player.id == player_id).first()
        return player and player.draft_number is not None and player.draft_number <= 14

    @staticmethod
    def played_both_conferences(session: Session, player_id: int) -> bool:
        """Verifica si el jugador ha militado en equipos de ambas conferencias."""
        conferences = session.query(distinct(Team.conference))\
            .join(PlayerTeamSeason, PlayerTeamSeason.team_id == Team.id)\
            .filter(PlayerTeamSeason.player_id == player_id)\
            .all()
        
        conf_list = [c[0] for c in conferences if c[0]]
        return 'East' in conf_list and 'West' in conf_list

    @staticmethod
    def is_non_mvp_award_winner(session: Session, player_id: int) -> bool:
        """Verifica si el jugador ganó un premio individual que NO sea el MVP de temporada."""
        valid_awards = ['Finals MVP', 'DPOY', 'ROY', '6MOY', 'MIP', 'NBA Cup MVP', 'All-NBA', 'All-Defensive', 'All-Rookie', 'POM', 'POW', 'ROM']
        return session.query(PlayerAward).filter(
            PlayerAward.player_id == player_id,
            PlayerAward.award_type != 'MVP',
            PlayerAward.award_type.in_(valid_awards)
        ).first() is not None

    @staticmethod
    def had_spanish_teammate(session: Session, player_id: int) -> bool:
        """Verifica si el jugador compartió vestuario con un jugador español."""
        player_stints = session.query(PlayerTeamSeason.team_id, PlayerTeamSeason.season)\
            .filter(PlayerTeamSeason.player_id == player_id).subquery()
        
        return session.query(PlayerTeamSeason)\
            .join(Player, Player.id == PlayerTeamSeason.player_id)\
            .join(player_stints, and_(
                PlayerTeamSeason.team_id == player_stints.c.team_id,
                PlayerTeamSeason.season == player_stints.c.season
            ))\
            .filter(
                PlayerTeamSeason.player_id != player_id,
                Player.country == 'Spain'
            ).first() is not None

    @staticmethod
    def is_european(session: Session, player_id: int) -> bool:
        """Verifica si el jugador es europeo (excluyendo Turquía e Israel)."""
        player = session.query(Player).filter(Player.id == player_id).first()
        return player and player.country in EUROPEAN_COUNTRIES

    @staticmethod
    def played_with_lebron(session: Session, player_id: int) -> bool:
        """Verifica si el jugador ha sido compañero de LeBron James (ID: 2544)."""
        lebron_id = 2544
        if player_id == lebron_id:
            return False
            
        lebron_stints = session.query(PlayerTeamSeason.team_id, PlayerTeamSeason.season)\
            .filter(PlayerTeamSeason.player_id == lebron_id).subquery()
        
        return session.query(PlayerTeamSeason).join(lebron_stints, and_(
            PlayerTeamSeason.team_id == lebron_stints.c.team_id,
            PlayerTeamSeason.season == lebron_stints.c.season
        )).filter(PlayerTeamSeason.player_id == player_id).first() is not None

    def validate_player(self, session: Session, player_name: str, category: str, letter: str) -> Dict[str, Any]:
        """Valida si un jugador cumple con la letra y la categoría."""
        p_name = player_name.strip().lower()
        
        # Búsqueda más estricta: coincidencia exacta de palabras completas
        # Esto evita que "lou will" encuentre a "Lou Williams"
        players = session.query(Player).filter(
            or_(
                func.lower(Player.full_name) == p_name,
                func.lower(Player.full_name).like(f"{p_name} %"),
                func.lower(Player.full_name).like(f"% {p_name}"),
                func.lower(Player.full_name).like(f"% {p_name} %")
            )
        ).all()
        
        if not players:
            return {'valid': False, 'message': 'El jugador no existe'}
            
        for player in players:
            player_id = int(player.id)
            name_parts = player.full_name.lower().split()
            valid_letter = any(part.startswith(letter.lower()) for part in name_parts)
            
            if not valid_letter: continue
                
            is_valid_cat = False
            if category == 'champion': is_valid_cat = self.is_champion(session, player_id)
            elif category == 'all_star': is_valid_cat = self.is_all_star(session, player_id)
            elif category == 'lottery': is_valid_cat = self.is_lottery_pick(session, player_id)
            elif category == 'conferences': is_valid_cat = self.played_both_conferences(session, player_id)
            elif category == 'non_mvp': is_valid_cat = self.is_non_mvp_award_winner(session, player_id)
            elif category == 'spanish_mate': is_valid_cat = self.had_spanish_teammate(session, player_id)
            elif category == 'european': is_valid_cat = self.is_european(session, player_id)
            elif category == 'lebron_mate': is_valid_cat = self.played_with_lebron(session, player_id)
            
            if is_valid_cat:
                return {
                    'valid': True, 
                    'player': {'id': player_id, 'full_name': player.full_name}
                }
        
        return {'valid': False, 'message': 'No cumple los requisitos'}

    def get_hints(self, session: Session, category: str, letter: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Obtiene los 10 mejores jugadores (por partidos jugados) que cumplen los requisitos."""
        l = letter.lower()
        
        # Subquery para sumar partidos totales por jugador desde PlayerTeamSeason
        stats_count = session.query(
            PlayerTeamSeason.player_id,
            func.coalesce(func.sum(PlayerTeamSeason.games_played), 0).label('total_games')
        ).group_by(PlayerTeamSeason.player_id).subquery()

        # Query base filtrada por letra
        query = session.query(Player, stats_count.c.total_games)\
            .outerjoin(stats_count, Player.id == stats_count.c.player_id)\
            .filter(
                or_(
                    func.lower(Player.full_name).like(f"{l}%"),
                    func.lower(Player.full_name).like(f"% {l}%")
                )
            )

        # Filtros por categoría usando EXISTS para precisión y rendimiento
        if category == 'champion':
            query = query.filter(exists().where(and_(
                PlayerAward.player_id == Player.id, 
                PlayerAward.award_type == 'Champion'
            )))
        elif category == 'all_star':
            query = query.filter(exists().where(and_(
                PlayerAward.player_id == Player.id, 
                PlayerAward.award_type == 'All-Star'
            )))
        elif category == 'lottery':
            query = query.filter(Player.draft_number <= 14)
        elif category == 'non_mvp':
            valid_awards = ['Finals MVP', 'DPOY', 'ROY', '6MOY', 'MIP', 'NBA Cup MVP', 'All-NBA', 'All-Defensive', 'All-Rookie', 'POM', 'POW', 'ROM']
            query = query.filter(exists().where(and_(
                PlayerAward.player_id == Player.id, 
                PlayerAward.award_type != 'MVP', 
                PlayerAward.award_type.in_(valid_awards)
            )))
        elif category == 'european':
            query = query.filter(Player.country.in_(EUROPEAN_COUNTRIES))
        elif category == 'conferences':
            # Jugadores que tengan al menos un equipo en cada conferencia
            east_exists = exists().where(and_(
                PlayerTeamSeason.player_id == Player.id, 
                PlayerTeamSeason.team_id == Team.id, 
                Team.conference == 'East'
            ))
            west_exists = exists().where(and_(
                PlayerTeamSeason.player_id == Player.id, 
                PlayerTeamSeason.team_id == Team.id, 
                Team.conference == 'West'
            ))
            query = query.filter(and_(east_exists, west_exists))
        elif category == 'spanish_mate':
            # Jugadores que compartieron equipo/temporada con un español
            spanish_stints = session.query(
                PlayerTeamSeason.team_id, 
                PlayerTeamSeason.season,
                PlayerTeamSeason.player_id.label('spanish_id')
            ).join(Player, Player.id == PlayerTeamSeason.player_id)\
             .filter(Player.country == 'Spain').subquery()
            
            query = query.filter(exists().where(and_(
                PlayerTeamSeason.player_id == Player.id,
                PlayerTeamSeason.team_id == spanish_stints.c.team_id,
                PlayerTeamSeason.season == spanish_stints.c.season,
                PlayerTeamSeason.player_id != spanish_stints.c.spanish_id
            )))
        elif category == 'lebron_mate':
            lebron_id = 2544
            lebron_stints = session.query(PlayerTeamSeason.team_id, PlayerTeamSeason.season)\
                .filter(PlayerTeamSeason.player_id == lebron_id).subquery()
            
            query = query.filter(exists().where(and_(
                PlayerTeamSeason.player_id == Player.id,
                PlayerTeamSeason.team_id == lebron_stints.c.team_id,
                PlayerTeamSeason.season == lebron_stints.c.season,
                Player.id != lebron_id
            )))

        # Ordenar por partidos jugados (descendente, nulos al final) y limitar
        results = query.order_by(desc(stats_count.c.total_games).nulls_last()).limit(limit * 2).all()
        
        all_matches = []
        seen_names = set()
        for p, count in results:
            if p.full_name not in seen_names:
                all_matches.append({'id': int(p.id), 'full_name': p.full_name})
                seen_names.add(p.full_name)
                if len(all_matches) >= limit: break
                
        return all_matches
