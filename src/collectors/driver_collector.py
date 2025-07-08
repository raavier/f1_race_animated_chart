"""
Coletor especializado para dados de pilotos da F1
"""

import pandas as pd
from typing import Dict, Optional, List
import logging

from .base import BaseCollector
from ..models.driver import Driver

logger = logging.getLogger(__name__)


class DriverCollector(BaseCollector):
    """Coletor para dados de pilotos da F1"""
    
    def collect(self, session_key: int, driver_number: Optional[int] = None) -> pd.DataFrame:
        """
        Coleta dados de pilotos de uma sessão
        
        Args:
            session_key: ID da sessão
            driver_number: Número específico do piloto (opcional)
            
        Returns:
            DataFrame com dados dos pilotos
        """
        logger.info(f"Coletando dados de pilotos da sessão {session_key}")
        
        params = {"session_key": session_key}
        if driver_number:
            params["driver_number"] = driver_number
        
        data = self._make_cached_request('drivers', **params)
        
        if not data:
            logger.warning(f"Nenhum piloto encontrado para sessão {session_key}")
            return pd.DataFrame()
        
        drivers = [Driver.from_api_data(item) for item in data]
        df = pd.DataFrame([driver.to_dict() for driver in drivers])
        
        logger.info(f"Coletados dados de {len(df)} pilotos")
        return df
    
    def get_drivers_dict(self, session_key: int) -> Dict[int, Driver]:
        """
        Obtém dicionário de pilotos indexado por número
        
        Args:
            session_key: ID da sessão
            
        Returns:
            Dicionário {driver_number: Driver}
        """
        df = self.collect(session_key)
        
        if df.empty:
            return {}
        
        drivers_dict = {}
        for _, row in df.iterrows():
            driver = Driver.from_api_data(row.to_dict())
            drivers_dict[driver.driver_number] = driver
        
        return drivers_dict
    
    def get_driver_by_number(self, session_key: int, driver_number: int) -> Optional[Driver]:
        """
        Obtém dados de um piloto específico
        
        Args:
            session_key: ID da sessão
            driver_number: Número do piloto
            
        Returns:
            Objeto Driver ou None se não encontrado
        """
        df = self.collect(session_key, driver_number)
        
        if df.empty:
            return None
        
        return Driver.from_api_data(df.iloc[0].to_dict())
    
    def get_drivers_by_team(self, session_key: int, team_name: str) -> pd.DataFrame:
        """
        Obtém pilotos de uma equipe específica
        
        Args:
            session_key: ID da sessão
            team_name: Nome da equipe
            
        Returns:
            DataFrame com pilotos da equipe
        """
        df = self.collect(session_key)
        
        if df.empty:
            return df
        
        # Filtra por equipe (case insensitive)
        mask = df['team_name'].str.lower().str.contains(team_name.lower(), na=False)
        return df[mask].reset_index(drop=True)
    
    def get_teams_summary(self, session_key: int) -> pd.DataFrame:
        """
        Obtém resumo das equipes na sessão
        
        Args:
            session_key: ID da sessão
            
        Returns:
            DataFrame com resumo das equipes
        """
        df = self.collect(session_key)
        
        if df.empty:
            return pd.DataFrame()
        
        teams_summary = df.groupby(['team_name', 'team_colour']).agg({
            'driver_number': 'count',
            'driver_name': lambda x: ', '.join(x),
            'name_acronym': lambda x: ', '.join(x),
            'country_code': lambda x: ', '.join(x.unique())
        }).reset_index()
        
        teams_summary.columns = [
            'team_name', 'team_colour', 'driver_count', 
            'driver_names', 'driver_acronyms', 'countries'
        ]
        
        return teams_summary
    
    def search_drivers(self, session_key: int, search_term: str) -> pd.DataFrame:
        """
        Busca pilotos por termo de pesquisa
        
        Args:
            session_key: ID da sessão
            search_term: Termo para buscar nos nomes, acrônimos, etc.
            
        Returns:
            DataFrame com pilotos que correspondem ao termo
        """
        df = self.collect(session_key)
        
        if df.empty:
            return df
        
        search_term = search_term.lower()
        
        mask = (
            df['first_name'].str.lower().str.contains(search_term, na=False) |
            df['last_name'].str.lower().str.contains(search_term, na=False) |
            df['full_name'].str.lower().str.contains(search_term, na=False) |
            df['name_acronym'].str.lower().str.contains(search_term, na=False) |
            df['team_name'].str.lower().str.contains(search_term, na=False) |
            df['country_code'].str.lower().str.contains(search_term, na=False)
        )
        
        return df[mask].reset_index(drop=True)
    
    def get_driver_numbers(self, session_key: int) -> List[int]:
        """
        Obtém lista de números de pilotos na sessão
        
        Args:
            session_key: ID da sessão
            
        Returns:
            Lista de números de pilotos
        """
        df = self.collect(session_key)
        
        if df.empty:
            return []
        
        return sorted(df['driver_number'].tolist())
    
    def validate_driver_data(self, session_key: int) -> Dict[str, any]:
        """
        Valida a qualidade dos dados dos pilotos
        
        Args:
            session_key: ID da sessão
            
        Returns:
            Dicionário com relatório de validação
        """
        df = self.collect(session_key)
        
        if df.empty:
            return {"error": "Nenhum dado de piloto encontrado"}
        
        validation_report = {
            "total_drivers": len(df),
            "missing_data": {},
            "duplicate_numbers": [],
            "teams_count": df['team_name'].nunique(),
            "countries_count": df['country_code'].nunique()
        }
        
        # Verifica dados ausentes
        for column in ['first_name', 'last_name', 'team_name', 'name_acronym']:
            missing_count = df[column].isna().sum()
            if missing_count > 0:
                validation_report["missing_data"][column] = missing_count
        
        # Verifica números duplicados
        duplicates = df[df['driver_number'].duplicated()]['driver_number'].tolist()
        if duplicates:
            validation_report["duplicate_numbers"] = duplicates
        
        # Verifica URLs de fotos
        missing_photos = df['headshot_url'].isna().sum()
        validation_report["missing_photos"] = missing_photos
        
        # Verifica cores de equipe
        missing_colors = df['team_colour'].isna().sum()
        validation_report["missing_team_colors"] = missing_colors
        
        return validation_report
    
    def get_driver_photo_urls(self, session_key: int) -> Dict[int, str]:
        """
        Obtém URLs das fotos dos pilotos
        
        Args:
            session_key: ID da sessão
            
        Returns:
            Dicionário {driver_number: photo_url}
        """
        df = self.collect(session_key)
        
        if df.empty:
            return {}
        
        # Filtra apenas pilotos com fotos
        df_with_photos = df.dropna(subset=['headshot_url'])
        
        return dict(zip(df_with_photos['driver_number'], df_with_photos['headshot_url']))
    
    def export_driver_mapping(self, session_key: int, output_file: str = None) -> Dict[int, Dict]:
        """
        Exporta mapeamento completo de pilotos
        
        Args:
            session_key: ID da sessão
            output_file: Arquivo para salvar (opcional)
            
        Returns:
            Dicionário com mapeamento completo
        """
        drivers_dict = self.get_drivers_dict(session_key)
        
        mapping = {}
        for number, driver in drivers_dict.items():
            mapping[number] = {
                "name": driver.display_name,
                "full_name": driver.full_name,
                "team": driver.team_name,
                "color": driver.team_color_hex,
                "country": driver.country_code,
                "photo_url": driver.headshot_url
            }
        
        if output_file:
            import json
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(mapping, f, indent=2, ensure_ascii=False)
            logger.info(f"Mapeamento de pilotos salvo em {output_file}")
        
        return mapping