"""
Cliente para a API OpenF1
Responsável por fazer as requisições HTTP e tratar respostas
"""

import requests
import time
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


class OpenF1Client:
    """Cliente para interagir com a API OpenF1"""
    
    def __init__(self, base_url: str = "https://api.openf1.org/v1"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'F1DataCollector/1.0'
        })
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None, 
                     max_retries: int = 3) -> Optional[List[Dict]]:
        """
        Faz requisição para a API com retry automático
        
        Args:
            endpoint: Endpoint da API (ex: 'sessions')
            params: Parâmetros da query
            max_retries: Número máximo de tentativas
            
        Returns:
            Lista de dicionários com os dados ou None se falhar
        """
        url = f"{self.base_url}/{endpoint}"
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Fazendo requisição para {endpoint} (tentativa {attempt + 1})")
                
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                logger.info(f"Sucesso: {len(data)} registros obtidos de {endpoint}")
                
                return data
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Erro na tentativa {attempt + 1}: {e}")
                
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Backoff exponencial
                    logger.info(f"Aguardando {wait_time}s antes da próxima tentativa...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Falha após {max_retries} tentativas para {endpoint}")
                    return None
    
    def get_sessions(self, **params) -> Optional[List[Dict]]:
        """Busca sessões da F1"""
        return self._make_request("sessions", params)
    
    def get_drivers(self, **params) -> Optional[List[Dict]]:
        """Busca dados dos pilotos"""
        return self._make_request("drivers", params)
    
    def get_positions(self, **params) -> Optional[List[Dict]]:
        """Busca dados de posições"""
        return self._make_request("position", params)
    
    def get_laps(self, **params) -> Optional[List[Dict]]:
        """Busca dados de voltas"""
        return self._make_request("laps", params)
    
    def get_meetings(self, **params) -> Optional[List[Dict]]:
        """Busca dados de meetings/GPs"""
        return self._make_request("meetings", params)
    
    def find_sessions(self, year: int, country_name: Optional[str] = None, 
                     session_name: Optional[str] = None) -> Optional[List[Dict]]:
        """
        Busca sessões com filtros específicos
        
        Args:
            year: Ano da temporada
            country_name: Nome do país (opcional)
            session_name: Nome da sessão (Race, Qualifying, etc.)
            
        Returns:
            Lista de sessões encontradas
        """
        params = {"year": year}
        
        if country_name:
            params["country_name"] = country_name
        if session_name:
            params["session_name"] = session_name
            
        return self.get_sessions(**params)
    
    def close(self):
        """Fecha a sessão HTTP"""
        self.session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()