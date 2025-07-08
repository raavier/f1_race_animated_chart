"""
Modelos de dados para posições
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Position:
    """Modelo de dados para posição de um piloto"""
    
    driver_number: int
    position: int
    date: datetime
    meeting_key: int
    session_key: int
    lap_number: Optional[int] = None
    
    @classmethod
    def from_api_data(cls, data: dict) -> 'Position':
        """Cria uma instância de Position a partir dos dados da API"""
        # Converte string de data para datetime
        date_str = data.get('date')
        if isinstance(date_str, str):
            # Remove timezone info se houver para simplificar
            if '+' in date_str:
                date_str = date_str.split('+')[0]
            elif 'Z' in date_str:
                date_str = date_str.replace('Z', '')
            
            try:
                date_obj = datetime.fromisoformat(date_str)
            except ValueError:
                # Fallback para formato com timezone
                date_obj = datetime.fromisoformat(data.get('date').replace('Z', '+00:00'))
        else:
            date_obj = date_str
        
        return cls(
            driver_number=data.get('driver_number'),
            position=data.get('position'),
            date=date_obj,
            meeting_key=data.get('meeting_key'),
            session_key=data.get('session_key'),
            lap_number=data.get('lap_number')
        )
    
    def to_dict(self) -> dict:
        """Converte a posição para dicionário"""
        return {
            'driver_number': self.driver_number,
            'position': self.position,
            'date': self.date.isoformat() if self.date else None,
            'meeting_key': self.meeting_key,
            'session_key': self.session_key,
            'lap_number': self.lap_number
        }
    
    @property
    def timestamp(self) -> float:
        """Timestamp Unix da posição"""
        return self.date.timestamp() if self.date else 0.0


@dataclass
class LapPosition:
    """Posição consolidada por volta"""
    
    driver_number: int
    lap_number: int
    position: int
    session_key: int
    meeting_key: int
    lap_start_time: Optional[datetime] = None
    lap_end_time: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        """Converte para dicionário"""
        return {
            'driver_number': self.driver_number,
            'lap_number': self.lap_number,
            'position': self.position,
            'session_key': self.session_key,
            'meeting_key': self.meeting_key,
            'lap_start_time': self.lap_start_time.isoformat() if self.lap_start_time else None,
            'lap_end_time': self.lap_end_time.isoformat() if self.lap_end_time else None
        }