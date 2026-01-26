"""Cliente unificado para la API de la NBA.

Este módulo centraliza todas las llamadas a la API de la NBA,
proporcionando una interfaz consistente con manejo automático
de errores, reintentos y reinicio en caso de fallos persistentes.
"""

import logging
import pandas as pd
from typing import List, Optional, Any

from nba_api.stats.endpoints import (
    LeagueGameFinder,
    BoxScoreTraditionalV3,
    BoxScoreTraditionalV2,
    BoxScoreSummaryV3,
    CommonTeamRoster,
    PlayerAwards,
    PlayerCareerStats,
    CommonPlayerInfo,
)

from ingestion.config import API_TIMEOUT
from ingestion.utils import fetch_with_retry, FatalIngestionError

logger = logging.getLogger(__name__)


class NBAApiClient:
    """Cliente que maneja todas las llamadas a la API de la NBA.
    
    Todas las llamadas API usan fetch_with_retry con fatal=True por defecto,
    garantizando que errores persistentes lancen FatalIngestionError para
    que el proceso se reinicie automáticamente.
    """
    
    def fetch_game_summary(self, game_id: str) -> Optional[Any]:
        """Obtiene resumen del partido (equipos, marcador, estado).
        
        Args:
            game_id: ID del partido (ej: "0022300123")
            
        Returns:
            Objeto BoxScoreSummaryV3 o None si datos no disponibles
            
        Raises:
            FatalIngestionError: Si la API falla persistentemente tras reintentos
        """
        result = fetch_with_retry(
            lambda: BoxScoreSummaryV3(game_id=game_id, timeout=API_TIMEOUT),
            error_context=f"BoxScoreSummaryV3({game_id})",
            fatal=True  # Crítico: debe lanzar FatalIngestionError si falla
        )
        return result
    
    def fetch_game_boxscore(self, game_id: str) -> Optional[Any]:
        """Obtiene estadísticas de jugadores del partido (V3).
        
        Args:
            game_id: ID del partido
            
        Returns:
            Objeto BoxScoreTraditionalV3 o None si datos no disponibles
            
        Raises:
            FatalIngestionError: Si la API falla persistentemente tras reintentos
        """
        result = fetch_with_retry(
            lambda: BoxScoreTraditionalV3(game_id=game_id, timeout=API_TIMEOUT),
            error_context=f"BoxScoreTraditionalV3({game_id})",
            fatal=True
        )
        return result
    
    def fetch_game_boxscore_v2_fallback(self, game_id: str) -> Optional[Any]:
        """Obtiene estadísticas usando V2 como fallback (para nombres faltantes).
        
        Args:
            game_id: ID del partido
            
        Returns:
            Objeto BoxScoreTraditionalV2 o None si datos no disponibles
            
        Raises:
            FatalIngestionError: Si la API falla persistentemente
        """
        result = fetch_with_retry(
            lambda: BoxScoreTraditionalV2(game_id=game_id, timeout=API_TIMEOUT),
            error_context=f"BoxScoreTraditionalV2-Fallback({game_id})",
            fatal=False  # Fallback no crítico, puede retornar None
        )
        return result
    
    def fetch_season_games(self, season: str) -> List[dict]:
        """Obtiene todos los partidos de una temporada con su clasificación consolidada.
        
        Consulta la API de la NBA para cada tipo de competición (RS, PO, PI).
        La clasificación de la NBA Cup (IST) se realiza mediante patrones de ID iniciales
        que luego son refinados por GameIngestion usando el gameSubtype oficial.
        
        Args:
            season: Temporada en formato "YYYY-YY" (ej: "2023-24")
            
        Returns:
            Lista de diccionarios con 'game_id', 'game_date' y flags de tipo:
            'is_rs', 'is_po', 'is_pi', 'is_ist'
            
        Raises:
            FatalIngestionError: Si la API falla persistentemente
        """
        logger.info(f"Obteniendo partidos para temporada {season}...")
        
        dfs = []
        
        # Mapeo de tipos de temporada oficiales
        season_types = {
            'Regular Season': 'is_rs',
            'Playoffs': 'is_po',
            'PlayIn': 'is_pi'
        }
        
        for nba_type, flag_name in season_types.items():
            if nba_type == 'PlayIn' and int(season.split('-')[0]) < 2020:
                continue
                
            res = fetch_with_retry(
                lambda nt=nba_type: LeagueGameFinder(
                    season_nullable=season, 
                    season_type_nullable=nt, 
                    timeout=API_TIMEOUT
                ),
                error_context=f"LeagueGameFinder({season}, {nba_type})",
                fatal=True
            )
            
            if res:
                df = res.get_data_frames()[0]
                if not df.empty:
                    df[flag_name] = True
                    dfs.append(df)
        
        if not dfs:
            logger.warning(f"No se encontraron partidos para {season}")
            return []
        
        # Combinar todos los DataFrames
        combined_df = pd.concat(dfs)
        
        # Asegurar columnas de tipo y llenar NaNs
        for flag in season_types.values():
            if flag not in combined_df.columns:
                combined_df[flag] = False
            else:
                combined_df[flag] = combined_df[flag].fillna(False)
        
        # Consolidar flags por GAME_ID
        consolidated = combined_df.groupby('GAME_ID').agg({
            'GAME_DATE': 'first',
            'WL': 'first', # Win/Loss solo se rellena cuando el partido ha terminado
            'is_rs': 'max',
            'is_po': 'max',
            'is_pi': 'max'
        }).reset_index()
        
        # Ordenar por fecha descendente
        consolidated = consolidated.sort_values(['GAME_DATE', 'GAME_ID'], ascending=[False, False])
        
        from ingestion.utils import parse_date
        games = []
        for _, row in consolidated.iterrows():
            gid = str(row['GAME_ID'])
            prefix = gid[:3]
            suffix = gid[5:]
            
            # Predicción inicial de IST basada en patrones de ID conocidos de la NBA
            # Esto será refinado por el gameSubtype durante la ingesta real del partido.
            is_ist = False
            if prefix == '006': # Final
                is_ist = True
            elif prefix == '002':
                # Eliminatorias (Cuartos y Semis)
                if suffix in ['01201', '01202', '01203', '01204', '01229', '01230']:
                    is_ist = True
                # Grupos (00001-00060 reservado en temporadas modernas)
                elif int(season.split('-')[0]) >= 2024:
                    try:
                        suffix_int = int(suffix)
                        if 1 <= suffix_int <= 60:
                            is_ist = True
                    except ValueError: pass

            # La columna WL (Win/Loss) solo tiene valor si el partido ha terminado oficialmente.
            wl_val = row['WL']
            api_finished = False
            if isinstance(wl_val, str) and wl_val.strip():
                api_finished = wl_val.strip().upper() in ['W', 'L']

            games.append({
                'game_id': gid,
                'game_date': parse_date(row['GAME_DATE']),
                'is_rs': bool(row['is_rs']),
                'is_po': bool(row['is_po']),
                'is_pi': bool(row['is_pi']),
                'is_ist': is_ist,
                'is_finished': api_finished
            })
        
        logger.info(f"Encontrados {len(games)} partidos consolidados para {season}")
        return games
    
    def fetch_team_roster(self, team_id: int, season: str) -> Optional[pd.DataFrame]:
        """Obtiene roster de equipo (para dorsales).
        
        Args:
            team_id: ID del equipo
            season: Temporada en formato "YYYY-YY"
            
        Returns:
            DataFrame con roster o None si no disponible
            
        Raises:
            FatalIngestionError: Si la API falla persistentemente
        """
        result = fetch_with_retry(
            lambda: CommonTeamRoster(team_id=team_id, season=season, timeout=API_TIMEOUT),
            error_context=f"CommonTeamRoster({team_id}, {season})",
            fatal=True  # Crítico para sincronización de dorsales
        )
        
        if result:
            df = result.get_data_frames()[0]
            return df if not df.empty else None
        
        return None
    
    def fetch_player_awards(self, player_id: int, fatal: bool = True) -> Optional[Any]:
        """Obtiene premios de un jugador.
        
        Args:
            player_id: ID del jugador
            fatal: Si True, lanza FatalIngestionError en caso de fallo persistente
            
        Returns:
            Objeto PlayerAwards o None si no disponible
            
        Raises:
            FatalIngestionError: Si fatal=True y la API falla persistentemente
        """
        result = fetch_with_retry(
            lambda: PlayerAwards(player_id=player_id, timeout=API_TIMEOUT),
            error_context=f"PlayerAwards({player_id})",
            fatal=fatal
        )
        return result
    
    def fetch_player_career(self, player_id: int, fatal: bool = True) -> Optional[Any]:
        """Obtiene resúmenes de carrera de un jugador.
        
        Args:
            player_id: ID del jugador
            fatal: Si True, lanza FatalIngestionError en caso de fallo persistente
            
        Returns:
            Objeto PlayerCareerStats o None si no disponible
            
        Raises:
            FatalIngestionError: Si fatal=True y la API falla persistentemente
        """
        result = fetch_with_retry(
            lambda: PlayerCareerStats(player_id=player_id, timeout=API_TIMEOUT),
            error_context=f"PlayerCareerStats({player_id})",
            fatal=fatal
        )
        return result
    
    def fetch_player_info(self, player_id: int, fatal: bool = True) -> Optional[Any]:
        """Obtiene ficha detallada de un jugador (biografía).
        
        Args:
            player_id: ID del jugador
            fatal: Si True, lanza FatalIngestionError en caso de fallo persistente
            
        Returns:
            Objeto CommonPlayerInfo o None si no disponible
            
        Raises:
            FatalIngestionError: Si fatal=True y la API falla persistentemente
        """
        result = fetch_with_retry(
            lambda: CommonPlayerInfo(player_id=player_id, timeout=API_TIMEOUT),
            error_context=f"CommonPlayerInfo({player_id})",
            fatal=fatal
        )
        return result
