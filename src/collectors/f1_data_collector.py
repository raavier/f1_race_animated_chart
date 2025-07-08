"""
Coletor principal de dados da F1
Responsável por orquestrar a coleta e consolidação dos dados usando coletores especializados
"""

import pandas as pd
import logging
from typing import Optional, Dict, Any
import os

# Importações relativas corretas
from ..api.client import OpenF1Client

logger = logging.getLogger(__name__)


class F1DataCollector:
    """Coletor principal de dados da F1 - orquestra coletores especializados"""
    
    def __init__(self, cache_enabled: bool = True):
        self.client = OpenF1Client()
        self.cache_enabled = cache_enabled
        
        # Inicializa coletores especializados (importação lazy para evitar circular)
        self._session_collector = None
        self._driver_collector = None
        self._position_collector = None
    
    @property
    def session_collector(self):
        """Lazy loading do SessionCollector"""
        if self._session_collector is None:
            from .session_collector import SessionCollector
            self._session_collector = SessionCollector(self.client, self.cache_enabled)
        return self._session_collector
    
    @property
    def driver_collector(self):
        """Lazy loading do DriverCollector"""
        if self._driver_collector is None:
            from .driver_collector import DriverCollector
            self._driver_collector = DriverCollector(self.client, self.cache_enabled)
        return self._driver_collector
    
    @property
    def position_collector(self):
        """Lazy loading do PositionCollector"""
        if self._position_collector is None:
            from .position_collector import PositionCollector
            self._position_collector = PositionCollector(self.client, self.cache_enabled)
        return self._position_collector
    
    def find_sessions(self, year: int, country_name: Optional[str] = None, 
                     session_name: Optional[str] = None) -> pd.DataFrame:
        """
        Busca sessões disponíveis usando SessionCollector
        
        Args:
            year: Ano da temporada
            country_name: Nome do país (opcional)
            session_name: Nome da sessão (opcional)
            
        Returns:
            DataFrame com as sessões encontradas
        """
        filters = {"year": year}
        if country_name:
            filters["country_name"] = country_name
        if session_name:
            filters["session_name"] = session_name
            
        return self.session_collector.collect(**filters)
    
    def get_session_info(self, session_key: int):
        """
        Obtém informações de uma sessão específica
        
        Args:
            session_key: ID da sessão
            
        Returns:
            Objeto Session ou None se não encontrada
        """
        return self.session_collector.get_session_info(session_key)
    
    def get_drivers_data(self, session_key: int) -> Dict[int, Any]:
        """
        Obtém dados dos pilotos de uma sessão usando DriverCollector
        
        Args:
            session_key: ID da sessão
            
        Returns:
            Dicionário com dados dos pilotos (número -> Driver)
        """
        return self.driver_collector.get_drivers_dict(session_key)
    
    def get_positions_data(self, session_key: int) -> pd.DataFrame:
        """
        Obtém dados de posições de uma sessão usando PositionCollector
        
        Args:
            session_key: ID da sessão
            
        Returns:
            DataFrame com dados de posições
        """
        return self.position_collector.collect(session_key)
    
    def collect_race_data(self, session_key: int) -> pd.DataFrame:
        """
        Coleta e consolida todos os dados de uma sessão
        
        Args:
            session_key: ID da sessão
            
        Returns:
            DataFrame consolidado com todos os dados
        """
        logger.info(f"Coletando dados da sessão {session_key}")
        
        # 1. Informações da sessão
        session = self.get_session_info(session_key)
        if not session:
            logger.error(f"Sessão {session_key} não encontrada")
            return pd.DataFrame()
        
        # 2. Dados dos pilotos
        drivers = self.get_drivers_data(session_key)
        if not drivers:
            logger.error(f"Nenhum piloto encontrado para sessão {session_key}")
            return pd.DataFrame()
        
        # 3. Dados de posições (agora retorna DataFrame)
        positions_df = self.get_positions_data(session_key)
        if positions_df.empty:
            logger.error(f"Nenhuma posição encontrada para sessão {session_key}")
            return pd.DataFrame()
        
        # 4. Consolida dados
        consolidated_data = []
        
        for _, position_row in positions_df.iterrows():
            driver_number = position_row['driver_number']
            driver = drivers.get(driver_number)
            
            if driver:
                # Dados da sessão
                session_data = session.to_dict() if hasattr(session, 'to_dict') else {
                    'session_key': session_key,
                    'session_name': getattr(session, 'session_name', ''),
                    'session_type': getattr(session, 'session_type', ''),
                    'country_name': getattr(session, 'country_name', ''),
                    'circuit_short_name': getattr(session, 'circuit_short_name', ''),
                    'year': getattr(session, 'year', 0),
                    'date_start': getattr(session, 'date_start', None)
                }
                
                # Dados do piloto
                driver_data = driver.to_dict() if hasattr(driver, 'to_dict') else {
                    'driver_number': driver_number,
                    'full_name': getattr(driver, 'full_name', ''),
                    'name_acronym': getattr(driver, 'name_acronym', ''),
                    'team_name': getattr(driver, 'team_name', ''),
                    'team_colour': getattr(driver, 'team_colour', ''),
                    'country_code': getattr(driver, 'country_code', ''),
                    'headshot_url': getattr(driver, 'headshot_url', '')
                }
                
                # Combina todos os dados
                row_data = {
                    # Dados da sessão
                    'session_key': session_data.get('session_key', session_key),
                    'session_name': session_data.get('session_name', ''),
                    'session_type': session_data.get('session_type', ''),
                    'country_name': session_data.get('country_name', ''),
                    'circuit_name': session_data.get('circuit_short_name', ''),
                    'year': session_data.get('year', 0),
                    'session_date': session_data.get('date_start'),
                    
                    # Dados do piloto
                    'driver_number': driver_data.get('driver_number', driver_number),
                    'driver_name': driver_data.get('full_name', ''),
                    'driver_acronym': driver_data.get('name_acronym', ''),
                    'team_name': driver_data.get('team_name', ''),
                    'team_colour': driver_data.get('team_colour', ''),
                    'country_code': driver_data.get('country_code', ''),
                    'headshot_url': driver_data.get('headshot_url', ''),
                    
                    # Dados de posição
                    'position': position_row.get('position'),
                    'timestamp': position_row.get('date'),
                    'lap_number': position_row.get('lap_number'),
                    'meeting_key': position_row.get('meeting_key'),
                }
                
                consolidated_data.append(row_data)
        
        df = pd.DataFrame(consolidated_data)
        
        if not df.empty:
            # Processa timestamps
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Calcula tempo desde o início
            if not df['timestamp'].isna().all():
                session_start = df['timestamp'].min()
                df['seconds_from_start'] = (df['timestamp'] - session_start).dt.total_seconds()
                
                # Estima números de volta se não disponíveis
                if df['lap_number'].isna().all():
                    df = self._estimate_lap_numbers(df)
            
            # Ordena por tempo e posição
            df = df.sort_values(['timestamp', 'position']).reset_index(drop=True)
            
            logger.info(f"DataFrame consolidado criado com {len(df)} registros")
        
        return df
    
    def _estimate_lap_numbers(self, df: pd.DataFrame, 
                             avg_lap_time_seconds: float = 90) -> pd.DataFrame:
        """
        Estima números de volta baseado no tempo decorrido
        
        Args:
            df: DataFrame com os dados
            avg_lap_time_seconds: Tempo médio de volta em segundos
            
        Returns:
            DataFrame com lap_number estimado
        """
        df = df.copy()
        df['lap_number'] = (df['seconds_from_start'] // avg_lap_time_seconds) + 1
        df['lap_number'] = df['lap_number'].astype(int)
        
        logger.info("Números de volta estimados baseado no tempo")
        return df
    
    def export_to_csv(self, df: pd.DataFrame, filename: str, 
                     output_dir: str = "C:/jobs/f1/f1_race_animated_chart/data/outputs") -> str:
        """
        Exporta DataFrame para CSV
        
        Args:
            df: DataFrame para exportar
            filename: Nome do arquivo
            output_dir: Diretório de saída
            
        Returns:
            Caminho completo do arquivo salvo
        """
        # Cria diretório se não existir
        os.makedirs(output_dir, exist_ok=True)
        
        # Garante extensão .csv
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        filepath = os.path.join(output_dir, filename)
        
        df.to_csv(filepath, index=False)
        logger.info(f"Dados exportados para {filepath}")
        
        return filepath
    
    def export_to_json(self, df: pd.DataFrame, filename: str, 
                      output_dir: str = "C:/jobs/f1/f1_race_animated_chart/data/outputs") -> str:
        """
        Exporta DataFrame para JSON
        
        Args:
            df: DataFrame para exportar
            filename: Nome do arquivo
            output_dir: Diretório de saída
            
        Returns:
            Caminho completo do arquivo salvo
        """
        # Cria diretório se não existir
        os.makedirs(output_dir, exist_ok=True)
        
        # Garante extensão .json
        if not filename.endswith('.json'):
            filename += '.json'
        
        filepath = os.path.join(output_dir, filename)
        
        # Converte timestamps para string para serialização JSON
        df_export = df.copy()
        if 'timestamp' in df_export.columns:
            df_export['timestamp'] = df_export['timestamp'].astype(str)
        if 'session_date' in df_export.columns:
            df_export['session_date'] = df_export['session_date'].astype(str)
        
        df_export.to_json(filepath, orient='records', indent=2)
        logger.info(f"Dados exportados para {filepath}")
        
        return filepath
    
    def get_summary_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Gera estatísticas resumidas dos dados coletados
        
        Args:
            df: DataFrame com os dados
            
        Returns:
            Dicionário com estatísticas
        """
        if df.empty:
            return {}
        
        stats = {
            'total_records': len(df),
            'unique_drivers': df['driver_number'].nunique(),
            'total_laps': df['lap_number'].max() if 'lap_number' in df else 0,
            'session_info': {
                'name': df['session_name'].iloc[0],
                'country': df['country_name'].iloc[0],
                'year': df['year'].iloc[0],
                'circuit': df['circuit_name'].iloc[0]
            },
            'time_span': {
                'start': df['timestamp'].min(),
                'end': df['timestamp'].max(),
                'duration_minutes': df['seconds_from_start'].max() / 60 if 'seconds_from_start' in df else 0
            },
            'drivers_list': df.groupby('driver_number').agg({
                'driver_name': 'first',
                'team_name': 'first',
                'position': ['min', 'max']
            }).to_dict('index')
        }
        
        return stats
    
    def close(self):
        """Fecha conexões e limpa cache"""
        if self._session_collector:
            self._session_collector.close()
        if self._driver_collector:
            self._driver_collector.close()
        if self._position_collector:
            self._position_collector.close()
        if self.client:
            self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    # Métodos de conveniência que delegam para coletores especializados
    def get_position_changes(self, session_key: int) -> pd.DataFrame:
        """Análise de mudanças de posição"""
        return self.position_collector.get_position_changes(session_key)
    
    def get_leaders_history(self, session_key: int) -> pd.DataFrame:
        """Histórico de líderes"""
        return self.position_collector.get_leaders_history(session_key)
    
    def get_teams_summary(self, session_key: int) -> pd.DataFrame:
        """Resumo das equipes"""
        return self.driver_collector.get_teams_summary(session_key)
    
    def get_lap_by_lap_data(self, session_key: int) -> pd.DataFrame:
        """Dados volta a volta"""
        return self.position_collector.get_positions_by_lap(session_key)
    
    def search_sessions(self, search_term: str) -> pd.DataFrame:
        """Busca sessões por termo"""
        return self.session_collector.search_sessions(search_term)
    
    def find_latest_sessions(self, limit: int = 10) -> pd.DataFrame:
        """Sessões mais recentes"""
        return self.session_collector.get_latest_sessions(limit)