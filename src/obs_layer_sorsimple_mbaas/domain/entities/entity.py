"""domain/entities/entity.py"""

from typing import Dict, List, Optional, Any
from datetime import datetime


class Entity:
    """
    Representa una entidad de negocio con su información asociada.
    """
    
    def __init__(
        self,
        entity_names: List[str],
        session_id: str,
        tidnid: Optional[str] = None,
        id_service: Optional[str] = None,
        timestamp: Optional[str] = None,
        service_data: Optional[Dict[str, Any]] = None,
        rules_data: Optional[Dict[str, Any]] = None
    ):
        """
        Inicializa una nueva entidad.
        
        Args:
            entity_names: Nombres de las entidades asociadas
            session_id: Identificador único de la sesión
            tidnid: Identificador de tipo y número de documento
            id_service: Identificador del servicio
            timestamp: Marca de tiempo de la entidad
            service_data: Datos específicos del servicio
            rules_data: Datos generados por las reglas de negocio
        """
        self.entity_names = entity_names
        self.session_id = session_id
        self.tidnid = tidnid
        self._data = {
            'id_service': id_service,
            'timestamp': timestamp,
            'service': service_data or {},
            'rules': rules_data or {}
        }
        
    @property
    def data(self) -> Dict[str, Any]:
        """
        Devuelve los datos de la entidad.
        
        Returns:
            Diccionario con los datos de la entidad
        """
        return self._data
        
    def add_rule_data(self, rule_data: Dict[str, Any]) -> None:
        """
        Agrega datos generados por reglas de negocio.
        
        Args:
            rule_data: Datos a agregar
        """
        self._data['rules'].update(rule_data)
        
    def to_dict(self) -> Dict[str, Any]:
        """
        Convierte la entidad a un diccionario para su persistencia.
        
        Returns:
            Representación de la entidad como diccionario
        """
        return {
            'entity_names': self.entity_names,
            'session_id': self.session_id,
            'tidnid': self.tidnid,
            'data': self._data
        }
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Entity':
        """
        Crea una entidad a partir de un diccionario.
        
        Args:
            data: Diccionario con datos de la entidad
            
        Returns:
            Nueva instancia de Entity
        """
        return cls(
            entity_names=data['entity_names'],
            session_id=data['session_id'],
            tidnid=data['tidnid'],
            id_service=data['data'].get('id_service'),
            timestamp=data['data'].get('timestamp'),
            service_data=data['data'].get('service'),
            rules_data=data['data'].get('rules')
        )
