"""
Modelos de dados para pilotos
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class Driver:
    """Modelo de dados para um piloto de F1"""
    
    driver_number: int
    first_name: str
    last_name: str
    full_name: str
    name_acronym: str
    broadcast_name: str
    team_name: str
    team_colour: str
    country_code: str
    headshot_url: Optional[str] = None
    meeting_key: Optional[int] = None
    session_key: Optional[int] = None
    
    @classmethod
    def from_api_data(cls, data: dict) -> 'Driver':
        """Cria uma instância de Driver a partir dos dados da API"""
        return cls(
            driver_number=data.get('driver_number'),
            first_name=data.get('first_name', ''),
            last_name=data.get('last_name', ''),
            full_name=data.get('full_name', ''),
            name_acronym=data.get('name_acronym', ''),
            broadcast_name=data.get('broadcast_name', ''),
            team_name=data.get('team_name', ''),
            team_colour=data.get('team_colour', ''),
            country_code=data.get('country_code', ''),
            headshot_url=data.get('headshot_url'),
            meeting_key=data.get('meeting_key'),
            session_key=data.get('session_key')
        )
    
    def to_dict(self) -> dict:
        """Converte o piloto para dicionário"""
        return {
            'driver_number': self.driver_number,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'full_name': self.full_name,
            'name_acronym': self.name_acronym,
            'broadcast_name': self.broadcast_name,
            'team_name': self.team_name,
            'team_colour': self.team_colour,
            'country_code': self.country_code,
            'headshot_url': self.headshot_url,
            'meeting_key': self.meeting_key,
            'session_key': self.session_key
        }
    
    @property
    def display_name(self) -> str:
        """Nome para exibição (acronym ou nome completo)"""
        return self.name_acronym if self.name_acronym else self.full_name
    
    @property
    def team_color_hex(self) -> str:
        """Cor da equipe com # se não tiver"""
        color = self.team_colour
        if color and not color.startswith('#'):
            return f"#{color}"
        return color or "#808080"