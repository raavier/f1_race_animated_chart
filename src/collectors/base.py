"""
Classe base para todos os coletores de dados da F1
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import logging

from ..api.client import OpenF1Client

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Classe base abstrata para coletores de dados da F1"""
    
    def __init__(self, client: Optional[OpenF1Client] = None, cache_enabled: bool = True):
        """
        Inicializa o coletor base
        
        Args:
            client: Cliente da API OpenF1 (cria um novo se None)
            cache_enabled: Se deve usar cache para otimizar requisições
        """
        self.client = client or OpenF1Client()
        self.cache_enabled = cache_enabled
        self._cache = {} if cache_enabled else None
        self._owns_client = client is None  # Se criou o cliente, deve fechá-lo
    
    def _get_cache_key(self, **params) -> str:
        """
        Gera uma chave de cache baseada nos parâmetros
        
        Args:
            **params: Parâmetros da requisição
            
        Returns:
            String única para usar como chave de cache
        """
        # Ordena os parâmetros para garantir consistência
        sorted_params = sorted(params.items())
        return f"{self.__class__.__name__}:{str(sorted_params)}"
    
    def _get_from_cache(self, cache_key: str) -> Optional[Any]:
        """
        Busca dados do cache
        
        Args:
            cache_key: Chave do cache
            
        Returns:
            Dados do cache ou None se não encontrado
        """
        if not self.cache_enabled or not self._cache:
            return None
        
        return self._cache.get(cache_key)
    
    def _save_to_cache(self, cache_key: str, data: Any) -> None:
        """
        Salva dados no cache
        
        Args:
            cache_key: Chave do cache
            data: Dados para salvar
        """
        if not self.cache_enabled or not self._cache:
            return
        
        self._cache[cache_key] = data
        logger.debug(f"Dados salvos no cache: {cache_key}")
    
    def _make_cached_request(self, endpoint: str, **params) -> Optional[List[Dict]]:
        """
        Faz requisição com cache automático
        
        Args:
            endpoint: Endpoint da API
            **params: Parâmetros da requisição
            
        Returns:
            Dados da API ou cache
        """
        cache_key = self._get_cache_key(endpoint=endpoint, **params)
        
        # Tenta buscar do cache primeiro
        cached_data = self._get_from_cache(cache_key)
        if cached_data is not None:
            logger.debug(f"Dados obtidos do cache: {cache_key}")
            return cached_data
        
        # Faz requisição à API
        data = self._make_api_request(endpoint, **params)
        
        # Salva no cache se obteve dados
        if data is not None:
            self._save_to_cache(cache_key, data)
        
        return data
    
    def _make_api_request(self, endpoint: str, **params) -> Optional[List[Dict]]:
        """
        Faz requisição direta à API (deve ser implementado por subclasses)
        
        Args:
            endpoint: Endpoint da API
            **params: Parâmetros da requisição
            
        Returns:
            Dados da API
        """
        # Mapeia endpoints para métodos do cliente
        endpoint_methods = {
            'sessions': self.client.get_sessions,
            'drivers': self.client.get_drivers,
            'position': self.client.get_positions,
            'laps': self.client.get_laps,
            'meetings': self.client.get_meetings
        }
        
        method = endpoint_methods.get(endpoint)
        if not method:
            logger.error(f"Endpoint não suportado: {endpoint}")
            return None
        
        return method(**params)
    
    @abstractmethod
    def collect(self, **kwargs) -> Any:
        """
        Método principal de coleta (deve ser implementado por subclasses)
        
        Args:
            **kwargs: Parâmetros específicos do coletor
            
        Returns:
            Dados coletados no formato apropriado
        """
        pass
    
    def clear_cache(self) -> None:
        """Limpa o cache do coletor"""
        if self._cache:
            self._cache.clear()
            logger.info("Cache do coletor limpo")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Retorna estatísticas do cache
        
        Returns:
            Dicionário com estatísticas do cache
        """
        if not self._cache:
            return {"cache_enabled": False}
        
        return {
            "cache_enabled": True,
            "entries_count": len(self._cache),
            "cache_keys": list(self._cache.keys())
        }
    
    def close(self) -> None:
        """Fecha o coletor e libera recursos"""
        if self._owns_client and self.client:
            self.client.close()
        
        if self._cache:
            self._cache.clear()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()