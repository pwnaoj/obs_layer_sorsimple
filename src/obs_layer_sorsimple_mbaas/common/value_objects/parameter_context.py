"""common/value_objects/parameter_context.py"""

import jmespath

from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, Optional, Any

from obs_layer_sorsimple_mbaas.domain.entities.entity import Entity


@dataclass
class ParameterContext:
    """
    Value object que encapsula el contexto necesario para extraer parámetros SQL.
    Auto-extrae valores dinámicamente del evento y configuración S3.
    """
    event: Dict
    s3_config: Dict
    entity: Optional[Entity] = None
    custom_context: Dict = field(default_factory=dict)
    
    # Cache para evitar re-extracciones
    _app_consumer_id: Optional[str] = field(default=None, init=False)
    _id_service: Optional[str] = field(default=None, init=False)
    _session_id: Optional[str] = field(default=None, init=False)
    _db_config: Optional[Dict] = field(default=None, init=False)

    def get_app_consumer_id(self) -> Optional[str]:
        """Extrae app_consumer_id del evento."""
        if self._app_consumer_id is None:
            self._app_consumer_id = jmespath.search(
                'jsonPayload.dataObject.consumer.appConsumer.id', 
                self.event
            )
        return self._app_consumer_id

    def get_id_service(self) -> Optional[str]:
        """Extrae id_service del evento."""
        if self._id_service is None:
            self._id_service = jmespath.search(
                'jsonPayload.dataObject.messages.idService', 
                self.event
            )
        return self._id_service

    def get_session_id(self) -> Optional[str]:
        """Extrae session_id del evento."""
        if self._session_id is None:
            self._session_id = jmespath.search(
                'jsonPayload.dataObject.consumer.appConsumer.sessionId', 
                self.event
            )
        return self._session_id

    def get_date_str(self, format_str: str = '%Y%m%d') -> str:
        """Genera string de fecha actual con formato especificado."""
        return datetime.now().strftime(format_str)

    def get_db_config(self) -> Optional[Dict]:
        """Extrae configuración de DB para el appConsumer actual."""
        if self._db_config is None:
            app_consumer_id = self.get_app_consumer_id()
            if app_consumer_id:
                self._db_config = jmespath.search(
                    f"[?id=='{app_consumer_id}'].config.db | [0]", 
                    self.s3_config
                )
        return self._db_config

    def get_entity_name(self) -> Optional[str]:
        """Extrae el primer entity_name si está disponible."""
        if self.entity and hasattr(self.entity, 'entity_names'):
            return self.entity.entity_names[0] if self.entity.entity_names else None
        return None

    def get_tidnid(self) -> Optional[str]:
        """Extrae tidnid del evento usando múltiples rutas."""
        tidnid_paths = [
            'jsonPayload.dataObject.documento',
            'jsonPayload.dataObject.client.documentClient'
        ]
        
        for path in tidnid_paths:
            doc_data = jmespath.search(path, self.event)
            if doc_data:
                # Extraer tipo y número para formar tidnid
                tipo = doc_data.get('tipo') or doc_data.get('type')
                numero = doc_data.get('numero') or doc_data.get('number')
                if tipo and numero:
                    return f"{tipo}-{numero}"
        return None

    def get_context_value(self, key: str, default: Any = None) -> Any:
        """Obtiene un valor del contexto custom o valores calculados."""
        return self.custom_context.get(key, default)

    def has_entity_data(self) -> bool:
        """Verifica si hay datos de entidad disponibles."""
        return self.entity is not None and hasattr(self.entity, 'data')
