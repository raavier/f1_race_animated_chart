"""
Coletor especializado para dados de posições da F1
"""

import pandas as pd
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging

from .base import BaseCollector
from ..models.position import Position, LapPosition

logger = logging.getLogger(__name__)


class PositionCollector(BaseCollector):
    """Coletor para dados de posições da F1"""
    
    def collect(self, session_key: int, driver_number: Optional[int] = None) -> pd.DataFrame:
        """
        Coleta dados de posições de uma sessão
        
        Args:
            session_key: ID da sessão
            driver_number: Número específico do piloto (opcional)
            
        Returns:
            DataFrame com dados de posições
        """
        logger.info(f"Coletando dados de posições da sessão {session_key}")
        
        params = {"session_key": session_key}
        if driver_number:
            params["driver_number"] = driver_number
        
        data = self._make_cached_request('position', **params)
        
        if not data:
            logger.warning(f"Nenhuma posição encontrada para sessão {session_key}")
            return pd.DataFrame()
        
        positions = [Position.from_api_data(item) for item in data]
        df = pd.DataFrame([position.to_dict() for position in positions])
        
        # Converte data para datetime se necessário
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        
        # Ordena por data e posição
        df = df.sort_values(['date', 'position']).reset_index(drop=True)
        
        logger.info(f"Coletados {len(df)} registros de posição")
        return df
    
    def get_positions_by_lap(self, session_key: int, avg_lap_time: float = 90.0) -> pd.DataFrame:
        """
        Organiza posições por volta estimada
        
        Args:
            session_key: ID da sessão
            avg_lap_time: Tempo médio de volta em segundos
            
        Returns:
            DataFrame com posições organizadas por volta
        """
        df = self.collect(session_key)
        
        if df.empty:
            return df
        
        # Calcula tempo desde o início
        session_start = df['date'].min()
        df['seconds_from_start'] = (df['date'] - session_start).dt.total_seconds()
        
        # Estima número da volta
        df['estimated_lap'] = (df['seconds_from_start'] // avg_lap_time) + 1
        df['estimated_lap'] = df['estimated_lap'].astype(int)
        
        # Pega a última posição conhecida para cada piloto em cada volta
        lap_positions = df.groupby(['estimated_lap', 'driver_number']).agg({
            'position': 'last',
            'date': 'last',
            'session_key': 'first',
            'meeting_key': 'first'
        }).reset_index()
        
        return lap_positions.sort_values(['estimated_lap', 'position'])
    
    def get_position_changes(self, session_key: int) -> pd.DataFrame:
        """
        Analisa mudanças de posição durante a sessão
        
        Args:
            session_key: ID da sessão
            
        Returns:
            DataFrame com análise de mudanças de posição
        """
        df = self.collect(session_key)
        
        if df.empty:
            return pd.DataFrame()
        
        # Calcula estatísticas por piloto
        position_stats = df.groupby('driver_number').agg({
            'position': ['first', 'last', 'min', 'max', 'mean'],
            'date': ['first', 'last', 'count']
        }).round(2)
        
        # Achata colunas multilevel
        position_stats.columns = [f"{col[0]}_{col[1]}" for col in position_stats.columns]
        position_stats = position_stats.reset_index()
        
        # Calcula mudanças
        position_stats['position_change'] = (
            position_stats['position_first'] - position_stats['position_last']
        )
        position_stats['best_position'] = position_stats['position_min']
        position_stats['worst_position'] = position_stats['position_max']
        position_stats['avg_position'] = position_stats['position_mean']
        position_stats['total_records'] = position_stats['date_count']
        
        # Renomeia colunas para clareza
        position_stats = position_stats.rename(columns={
            'position_first': 'starting_position',
            'position_last': 'ending_position',
            'date_first': 'first_recorded',
            'date_last': 'last_recorded'
        })
        
        # Seleciona colunas relevantes
        result_columns = [
            'driver_number', 'starting_position', 'ending_position',
            'position_change', 'best_position', 'worst_position',
            'avg_position', 'total_records', 'first_recorded', 'last_recorded'
        ]
        
        return position_stats[result_columns].sort_values('ending_position')
    
    def get_leaders_history(self, session_key: int) -> pd.DataFrame:
        """
        Obtém histórico de líderes da sessão
        
        Args:
            session_key: ID da sessão
            
        Returns:
            DataFrame com mudanças de liderança
        """
        df = self.collect(session_key)
        
        if df.empty:
            return pd.DataFrame()
        
        # Filtra apenas posições P1
        leaders = df[df['position'] == 1].copy()
        
        if leaders.empty:
            return pd.DataFrame()
        
        # Ordena por data
        leaders = leaders.sort_values('date')
        
        # Identifica mudanças de líder
        leaders['leader_changed'] = leaders['driver_number'] != leaders['driver_number'].shift(1)
        
        # Calcula duração da liderança
        leaders['next_change'] = leaders['date'].shift(-1)
        leaders['leadership_duration'] = (
            leaders['next_change'] - leaders['date']
        ).dt.total_seconds()
        
        return leaders[['date', 'driver_number', 'leader_changed', 'leadership_duration']]
    
    def get_position_at_time(self, session_key: int, target_time: datetime) -> pd.DataFrame:
        """
        Obtém posições em um momento específico
        
        Args:
            session_key: ID da sessão
            target_time: Momento específico
            
        Returns:
            DataFrame com posições no momento especificado
        """
        df = self.collect(session_key)
        
        if df.empty:
            return df