"""application/builders/entity_builders.py"""

import jmespath

from datetime import datetime
from flatten_json import flatten, unflatten_list
from typing import Any, Dict, List, Optional

from obs_layer_sorsimple_mbaas.common.utils.log import logger


class EntityBuilder:
    """
    Constructor para objetos de entidad siguiendo el patrón Builder.
    """
    
    def __init__(self):
        """Inicializa un nuevo constructor de entidades."""
        self.reset()
        
    def reset(self):
        """Reinicia el constructor al estado inicial."""
        self.entity = {
            'entity_names': [],
            'session_id': None,
            'tidnid': None,
            'data': {
                'id_service': None,
                'timestamp': None,
                'service': {},
                'rules': {}
            }
        }
        self._event = None
        return self
        
    def with_event(self, event: Dict):
        """
        Establece el evento base para construir la entidad.
        
        Args:
            event: Evento base (será transformado internamente)
            
        Returns:
            El constructor para encadenamiento de métodos
        """
        self._event = self._unflatten_json(event)
        return self
    
    def with_session_data(self, repository):
        """
        Extrae y establece los datos de sesión del evento.
        
        Args:
            repository: Repositorio para buscar tidnid si no está en el evento
            
        Returns:
            El constructor para encadenamiento de métodos
            
        Raises:
            ValueError: Si no se ha establecido el evento previamente
        """
        if not self._event:
            raise ValueError("Debe establecer el evento primero con with_event()")
            
        try:
            # Extraer session_id
            session_id = jmespath.search(
                'jsonPayload.dataObject.consumer.appConsumer.sessionId', 
                self._event
            )
            
            if not session_id:
                raise ValueError("No se encontró session_id en el evento")
                
            self.entity['session_id'] = session_id.strip()
            
            # Extraer tidnid del evento
            tidnid = jmespath.search(
                "(jsonPayload.dataObject.documento || jsonPayload.dataObject.client.documentClient) | " +
                "join('-', [tipo || type, numero || number])", 
                self._event
            )
            
            # Si no está en el evento, buscarlo en el repositorio
            if not tidnid and repository:
                try:
                    date_str = datetime.now().strftime('%Y%m%d')
                    tidnid = repository.find_tidnid(self.entity['session_id'], date_str)
                except Exception as e:
                    logger.error(f"Error al buscar tidnid: {e}")
                    
            self.entity['tidnid'] = tidnid
            return self
        except Exception as e:
            logger.error(f"Error al extraer datos de sesión: {e}")
            raise ValueError(f"Error al extraer datos de sesión: {e}")
    
    def with_service_data(self, s3_config: Dict):
        """
        Extrae y establece los datos del servicio.
        
        Args:
            s3_config: Configuración de servicios desde S3
            
        Returns:
            El constructor para encadenamiento de métodos
            
        Raises:
            ValueError: Si no se ha establecido el evento previamente
        """
        if not self._event:
            raise ValueError("Debe establecer el evento primero con with_event()")
            
        try:
            # Extraer datos básicos
            id_service = jmespath.search('jsonPayload.dataObject.messages.idService', self._event)
            consumer_id = jmespath.search('jsonPayload.dataObject.consumer.appConsumer.id', self._event)
            timestamp = jmespath.search("timestamp", self._event)
            
            if not id_service or not consumer_id:
                raise ValueError("No se encontró id_service o consumer_id en el evento")
                
            id_service = id_service.strip()
            consumer_id = consumer_id.strip()
            
            # Buscar configuración de servicio
            services = jmespath.search(f"[?id=='{consumer_id}'].services[]", s3_config)
            
            # Determinar nombre de entidad
            entity_names = jmespath.search(f"[?id_service=='{id_service}'].entity[][]", services)
            
            # Verificar si es workflow
            entity_workflow = jmespath.search("jsonPayload.dataObject.messages.transaction.transactionName", self._event)
            if entity_workflow:
                entity_names = entity_names or [entity_workflow]
                
            # Extraer variables del servicio
            paths = jmespath.search(f"[?id_service=='{id_service}'].paths[]", services)
            service_data = {}
            if paths:
                from obs_layer_sorsimple_mbaas.common.utils.jmespath import extract_from_message_selected_fields
                service_data = dict(extract_from_message_selected_fields(paths, self._event))
                
            # Actualizar entidad
            self.entity['entity_names'] = entity_names
            self.entity['data']['id_service'] = id_service
            self.entity['data']['timestamp'] = timestamp
            self.entity['data']['service'] = service_data
            
            return self
        except Exception as e:
            logger.error(f"Error al extraer datos de servicio: {e}")
            raise ValueError(f"Error al extraer datos de servicio: {e}")
    
    def with_rules(self, rule_engine = None):
        """
        Aplica reglas de negocio a la entidad.
        
        Args:
            rule_engine: Motor de reglas configurado
            
        Returns:
            El constructor para encadenamiento de métodos
        """
        try:
            if rule_engine and self._event:
                self.entity['data']['rules'] = rule_engine.process_event(self._event)
            else:
                self.entity['data']['rules'] = {}
                
            return self
        except Exception as e:
            logger.error(f"Error al aplicar reglas: {e}")
            self.entity['data']['rules'] = {}
            return self
    
    def build(self):
        """
        Construye y retorna la entidad completa.
        
        Returns:
            La entidad construida
            
        Raises:
            ValueError: Si faltan datos esenciales
        """
        # Validaciones finales
        if not self.entity['entity_names']:
            raise ValueError("La entidad debe tener al menos un nombre de entidad")
        if not self.entity['session_id']:
            raise ValueError("La entidad debe tener un session_id")
            
        return self.entity
        
    def _unflatten_json(self, event: Dict) -> Dict:
        """
        Desacopla el evento aplanado a estructura JSON con niveles.
        
        Args:
            event: Evento a procesar
            
        Returns:
            Evento con estructura jerárquica
        """
        try:
            event_flat = flatten(event, '.')
            return unflatten_list(event_flat, '.')
        except Exception as e:
            logger.error(f"Error al desaplanar JSON: {e}")
            return event
