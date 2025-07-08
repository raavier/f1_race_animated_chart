"""
Coletor especializado para dados de sessões da F1
"""

import pandas as pd
from typing import Optional, List
import logging

from .base import BaseCollector
from ..models.session import Session

logger = logging.getLogger(__name__)


class SessionCollector(BaseCollector):
    """Coletor para dados de sessões da F1"""
    
    def collect(self, session_key: Optional[int] = None, **filters) -> pd.DataFrame:
        """
        Coleta dados de sessões
        
        Args:
            session_key: ID específico da sessão (opcional)
            **filters: Filtros para busca (year, country_name, session_name, etc.)
            
        Returns:
            DataFrame com dados das sessões
        """
        if session_key:
            return self._collect_single_session(session_key)
        else:
            return self._collect_sessions_by_filters(**filters)
    
    def _collect_single_session(self, session_key: int) -> pd.DataFrame:
        """
        Coleta dados de uma sessão específica
        
        Args:
            session_key: ID da sessão
            
        Returns:
            DataFrame com dados da sessão
        """
        logger.info(f"Coletando dados da sessão {session_key}")
        
        data = self._make_cached_request('sessions', session_key=session_key)
        
        if not data:
            logger.warning(f"Sessão {session_key} não encontrada")
            return pd.DataFrame()
        
        sessions = [Session.from_api_data(item) for item in data]
        df = pd.DataFrame([session.to_dict() for session in sessions])
        
        logger.info(f"Sessão {session_key} coletada com sucesso")
        return df
    
    def _collect_sessions_by_filters(self, **filters) -> pd.DataFrame:
        """
        Coleta sessões com base em filtros
        
        Args:
            **filters: Filtros para busca
            
        Returns:
            DataFrame com sessões encontradas
        """
        logger.info(f"Buscando sessões com filtros: {filters}")
        
        data = self._make_cached_request('sessions', **filters)
        
        if not data:
            logger.warning("Nenhuma sessão encontrada com os filtros especificados")
            return pd.DataFrame()
        
        sessions = [Session.from_api_data(item) for item in data]
        df = pd.DataFrame([session.to_dict() for session in sessions])
        
        logger.info(f"Encontradas {len(df)} sessões")
        return df
    
    def find_sessions_by_year(self, year: int) -> pd.DataFrame:
        """
        Busca todas as sessões de um ano específico
        
        Args:
            year: Ano da temporada
            
        Returns:
            DataFrame com sessões do ano
        """
        return self.collect(year=year)
    
    def find_races_by_year(self, year: int) -> pd.DataFrame:
        """
        Busca apenas corridas de um ano específico
        
        Args:
            year: Ano da temporada
            
        Returns:
            DataFrame com corridas do ano
        """
        return self.collect(year=year, session_name="Race")
    
    def find_qualifying_by_year(self, year: int) -> pd.DataFrame:
        """
        Busca apenas qualifyings de um ano específico
        
        Args:
            year: Ano da temporada
            
        Returns:
            DataFrame com qualifyings do ano
        """
        return self.collect(year=year, session_name="Qualifying")
    
    def find_sessions_by_country(self, country_name: str, year: Optional[int] = None) -> pd.DataFrame:
        """
        Busca sessões de um país específico
        
        Args:
            country_name: Nome do país
            year: Ano (opcional)
            
        Returns:
            DataFrame com sessões do país
        """
        filters = {"country_name": country_name}
        if year:
            filters["year"] = year
        
        return self.collect(**filters)
    
    def get_session_info(self, session_key: int) -> Optional[Session]:
        """
        Obtém informações detalhadas de uma sessão
        
        Args:
            session_key: ID da sessão
            
        Returns:
            Objeto Session ou None se não encontrada
        """
        df = self._collect_single_session(session_key)
        
        if df.empty:
            return None
        
        # Converte primeira linha do DataFrame de volta para objeto Session
        session_dict = df.iloc[0].to_dict()
        return Session.from_api_data(session_dict)
    
    def get_latest_sessions(self, limit: int = 10) -> pd.DataFrame:
        """
        Busca as sessões mais recentes
        
        Args:
            limit: Número máximo de sessões
            
        Returns:
            DataFrame com sessões mais recentes
        """
        # Busca sessões do ano atual primeiro
        from datetime import datetime
        current_year = datetime.now().year
        
        df = self.find_sessions_by_year(current_year)
        
        if df.empty:
            # Se não houver do ano atual, tenta ano anterior
            df = self.find_sessions_by_year(current_year - 1)
        
        if not df.empty and 'date_start' in df.columns:
            # Ordena por data e pega as mais recentes
            df['date_start'] = pd.to_datetime(df['date_start'])
            df = df.sort_values('date_start', ascending=False).head(limit)
        
        return df
    
    def search_sessions(self, search_term: str) -> pd.DataFrame:
        """
        Busca sessões por termo de pesquisa
        
        Args:
            search_term: Termo para buscar nos nomes de países, circuitos, etc.
            
        Returns:
            DataFrame com sessões que correspondem ao termo
        """
        # Busca todas as sessões disponíveis (pode ser custoso)
        from datetime import datetime
        current_year = datetime.now().year
        
        all_sessions = []
        
        # Busca nos últimos 2 anos
        for year in [current_year, current_year - 1]:
            year_sessions = self.find_sessions_by_year(year)
            if not year_sessions.empty:
                all_sessions.append(year_sessions)
        
        if not all_sessions:
            return pd.DataFrame()
        
        df = pd.concat(all_sessions, ignore_index=True)
        
        # Filtra por termo de busca (case insensitive)
        search_term = search_term.lower()
        
        mask = (
            df['country_name'].str.lower().str.contains(search_term, na=False) |
            df['circuit_short_name'].str.lower().str.contains(search_term, na=False) |
            df['session_name'].str.lower().str.contains(search_term, na=False) |
            df['location'].str.lower().str.contains(search_term, na=False)
        )
        
        return df[mask].reset_index(drop=True)