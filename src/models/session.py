"""
Modelos de dados para sessões
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Session:
    """Modelo de dados para uma sessão de F1"""
    
    session_key: int
    session_name: str
    session_type: str
    country_name: str
    country_code: str
    circuit_short_name: str
    location: str
    year: int
    meeting_key: int
    date_start: Optional[datetime] = None
    date_end: Optional[datetime] = None
    gmt_offset: Optional[str] = None
    circuit_key: Optional[int] = None
    country_key: Optional[int] = None
    
    @classmethod
    def from_api_data(cls, data: dict) -> 'Session':
        """Cria uma instância de Session a partir dos dados da API"""
        
        def parse_date(date_str: Optional[str]) -> Optional[datetime]:
            """Parse de data da API"""
            if not date_str:
                return None
            try:
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except ValueError:
                return None
        
        return cls(
            session_key=data.get('session_key'),
            session_name=data.get('session_name', ''),
            session_type=data.get('session_type', ''),
            country_name=data.get('country_name', ''),
            country_code=data.get('country_code', ''),
            circuit_short_name=data.get('circuit_short_name', ''),
            location=data.get('location', ''),
            year=data.get('year'),
            meeting_key=data.get('meeting_key'),
            date_start=parse_date(data.get('date_start')),
            date_end=parse_date(data.get('date_end')),
            gmt_offset=data.get('gmt_offset'),
            circuit_key=data.get('circuit_key'),
            country_key=data.get('country_key')
        )
    
    def to_dict(self) -> dict:
        """Converte a sessão para dicionário"""
        return {
            'session_key': self.session_key,
            'session_name': self.session_name,
            'session_type': self.session_type,
            'country_name': self.country_name,
            'country_code': self.country_code,
            'circuit_short_name': self.circuit_short_name,
            'location': self.location,
            'year': self.year,
            'meeting_key': self.meeting_key,
            'date_start': self.date_start.isoformat() if self.date_start else None,
            'date_end': self.date_end.isoformat() if self.date_end else None,
            'gmt_offset': self.gmt_offset,
            'circuit_key': self.circuit_key,
            'country_key': self.country_key
        }
    
    @property
    def display_name(self) -> str:
        """Nome para exibição"""
        return f"{self.country_name} {self.year} - {self.session_name}"
    
    @property
    def is_race(self) -> bool:
        """Verifica se é uma corrida"""
        return self.session_name.lower() in ['race', 'sprint']
    
    @property
    def is_qualifying(self) -> bool:
        """Verifica se é qualifying"""
        return 'qualifying' in self.session_name.lower()