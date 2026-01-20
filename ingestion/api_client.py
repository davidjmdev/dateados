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
        """Obtiene todos los partidos de una temporada con su fecha local.
        
        Args:
            season: Temporada en formato "YYYY-YY" (ej: "2023-24")
            
        Returns:
            Lista de diccionarios con 'game_id' y 'game_date' (date object)
            ordenados por fecha descendente.
            
        Raises:
            FatalIngestionError: Si la API falla persistentemente
        """
        logger.info(f"Obteniendo partidos para temporada {season}...")
        
        # Lista de DataFrames para concatenar
        dfs = []
        
        # Regular Season
        rs = fetch_with_retry(
            lambda: LeagueGameFinder(
                season_nullable=season, 
                season_type_nullable='Regular Season', 
                timeout=API_TIMEOUT
            ),
            error_context=f"LeagueGameFinder({season}, Regular Season)",
            fatal=True
        )
        
        if rs:
            dfs.append(rs.get_data_frames()[0])
        
        # Playoffs
        po = fetch_with_retry(
            lambda: LeagueGameFinder(
                season_nullable=season,
                season_type_nullable='Playoffs',
                timeout=API_TIMEOUT
            ),
            error_context=f"LeagueGameFinder({season}, Playoffs)",
            fatal=True
        )
        
        if po:
            dfs.append(po.get_data_frames()[0])
        
        # PlayIn (desde 2020-21)
        if int(season.split('-')[0]) >= 2020:
            pi = fetch_with_retry(
                lambda: LeagueGameFinder(
                    season_nullable=season,
                    season_type_nullable='PlayIn',
                    timeout=API_TIMEOUT
                ),
                error_context=f"LeagueGameFinder({season}, PlayIn)",
                fatal=False  # PlayIn puede no existir en todas las temporadas
            )
            
            if pi:
                dfs.append(pi.get_data_frames()[0])
        
        # NBA Cup / IST (desde 2023-24)
        if int(season.split('-')[0]) >= 2023:
            ist = fetch_with_retry(
                lambda: LeagueGameFinder(
                    season_nullable=season,
                    timeout=API_TIMEOUT
                ),
                error_context=f"LeagueGameFinder({season}, IST)",
                fatal=False
            )
            
            if ist:
                ist_df = ist.get_data_frames()[0]
                if not ist_df.empty:
                    # Filtrar solo partidos de NBA Cup (ID empieza con '006')
                    # Nota: Los partidos 002 ya se capturan en el bloque de Regular Season
                    ist_only = ist_df[ist_df['GAME_ID'].str.startswith('006')]
                    if not ist_only.empty:
                        dfs.append(ist_only)
        
        # Filtrar DataFrames vacíos
        dfs = [d for d in dfs if not d.empty]
        
        if not dfs:
            logger.warning(f"No se encontraron partidos para {season}")
            return []
        
        # Combinar todos los DataFrames
        df = pd.concat(dfs)
        
        # Eliminar duplicados de partidos (un partido puede salir en RS e IST)
        df = df.drop_duplicates('GAME_ID')
        
        # Ordenar por fecha descendente
        df = df.sort_values(['GAME_DATE', 'GAME_ID'], ascending=[False, False])
        
        from ingestion.utils import parse_date
        games = []
        for _, row in df.iterrows():
            games.append({
                'game_id': row['GAME_ID'],
                'game_date': parse_date(row['GAME_DATE'])
            })
        
        logger.info(f"Encontrados {len(games)} partidos para {season}")
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
