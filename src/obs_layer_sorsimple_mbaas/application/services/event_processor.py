"""application/services/event_processor.py"""

import json
import jmespath

from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from obs_layer_sorsimple_mbaas.domain.rules.engine import RuleEngine
from obs_layer_sorsimple_mbaas.domain.entities.entity import Entity
from obs_layer_sorsimple_mbaas.common.utils.log import logger


class EventProcessor:
    """
    Procesa eventos aplicando reglas de negocio y generando entidades.
    """

    def __init__(self, s3_config: Dict[str, Any]):
        """
        Inicializa el procesador con la configuración.
        
        Args:
            s3_config: Configuración de servicios y reglas desde S3
        """
        self.s3_config = s3_config

    def _parse_record(self, record: Union[str, Dict]) -> Dict:
        """
        Parsea y valida los datos del mensaje SQS.
        
        Args:
            record: Mensaje SQS que contiene el evento
            
        Returns:
            Mensaje SQS parseado a diccionario
            
        Raises:
            ValueError: Si el mensaje no puede ser parseado
        """
        try:
            from obs_layer_sorsimple_mbaas.interfaces.models.sqs_models import Record
            
            # Validar estructura del mensaje
            if isinstance(record, str):
                validated_record = Record.model_validate_json(record)
            else:
                validated_record = Record.model_validate(record)
                
            return validated_record.model_dump()
        except Exception as e:
            logger.error(f"Error al parsear registro SQS: {e}")
            raise ValueError(f"Formato de mensaje inválido: {e}")

    def _get_record_body(self, parsed_record: Dict[str, Any]) -> Dict:
        """
        Extrae el contenido del mensaje y lo parsea a diccionario.
        
        Args:
            parsed_record: Registro SQS parseado
            
        Returns:
            Diccionario con datos del evento
            
        Raises:
            ValueError: Si el body no puede ser parseado
        """
        try:
            # Extraer campo body
            body = parsed_record.get('body', {})
            
            # Si es string, intentar parsearlo como JSON
            while isinstance(body, str):
                try:
                    body = json.loads(body)
                except json.JSONDecodeError:
                    break
                
            return body
        except Exception as e:
            logger.error(f"Error al extraer body del mensaje: {e}")
            raise ValueError(f"Error al procesar contenido del mensaje: {e}")

    def process(self, message: str) -> Dict:
        """
        Procesa un mensaje SQS para extraer el evento contenido.
        
        Args:
            message: Mensaje SQS a procesar
            
        Returns:
            Evento extraído del mensaje
            
        Raises:
            ValueError: Si ocurre un error en el procesamiento
        """
        try:
            # Parsear mensaje
            parsed_record = self._parse_record(message)
            
            # Extraer y retornar el evento
            return self._get_record_body(parsed_record)
        except Exception as e:
            logger.error(f"Error en procesamiento de mensaje: {e}")
            raise ValueError(f"Error al procesar mensaje: {e}")

    def get_rules(self, event: Dict) -> Optional[List[Dict]]:
        """
        Obtiene las reglas aplicables al evento.
        
        Args:
            event: Evento a procesar
            
        Returns:
            Lista de reglas aplicables o None si no hay reglas
        """
        try:
            # Desaplanar el evento para usar JMESPath
            from obs_layer_sorsimple_mbaas.application.builders.entity_builder import EntityBuilder
            event_unflat = EntityBuilder()._unflatten_json(event)
            
            # Extraer información necesaria
            id_service = jmespath.search('jsonPayload.dataObject.messages.idService', event_unflat)
            consumer_id = jmespath.search('jsonPayload.dataObject.consumer.appConsumer.id', event_unflat)
            
            if not id_service or not consumer_id:
                logger.warning("No se pudo extraer id_service o consumer_id del evento")
                return None
                
            # Limpiar datos
            id_service = id_service.strip()
            consumer_id = consumer_id.strip()
            
            # Buscar reglas
            rules = jmespath.search(f"[?id=='{consumer_id}'].rules[]", self.s3_config)
            
            if not rules:
                logger.info(f"No se encontraron reglas para consumer_id={consumer_id}")
                return None
                
            # Filtrar reglas por tipo de evento
            filtered_rules = jmespath.search(f"[?event_type=='{id_service}']", rules)
            
            if not filtered_rules:
                logger.info(f"No se encontraron reglas para id_service={id_service}")
                
            return filtered_rules
        except Exception as e:
            logger.error(f"Error al obtener reglas: {e}")
            return None

    def build_entity(
            self, repository, event: Dict,
            rules: Optional[List[Dict]] = None,
            extensions: Optional[Dict[Any, str]] = None
        ) -> Entity:
        """
        Construye una entidad aplicando reglas de negocio.
        
        Args:
            repository: Repositorio para acceso a datos
            event: Evento del cual construir la entidad
            rules: Reglas de negocio a aplicar
            extensions: Datos de extensión para las reglas
            
        Returns:
            Entidad construida
            
        Raises:
            ValueError: Si ocurre un error en la construcción
        """
        try:
            # Importar el builder
            from obs_layer_sorsimple_mbaas.application.builders.entity_builder import EntityBuilder
            
            # Crear motor de reglas si hay reglas
            rule_engine = None
            if rules:
                rule_engine = RuleEngine(rules, extensions)
                
            # Construir la entidad usando el builder
            entity_dict = EntityBuilder()\
                .with_event(event)\
                .with_session_data(repository)\
                .with_service_data(self.s3_config)\
                .with_rules(rule_engine)\
                .build()
                
            # Convertir a objeto de dominio
            return Entity.from_dict(entity_dict)
        except Exception as e:
            logger.error(f"Error al construir entidad: {e}")
            raise ValueError(f"Error al construir entidad: {e}")

    def save_entity(self, repository, entity: Entity) -> bool:
        """
        Guarda una entidad usando el repositorio proporcionado.
        
        Args:
            repository: Repositorio para persistencia
            entity: Entidad a guardar
            
        Returns:
            True si se guardó correctamente, False en caso contrario
        """
        try:
            # Obtener fecha actual
            date_str = datetime.now().strftime('%Y%m%d')
            
            # Guardar la entidad en cada tabla correspondiente
            for entity_name in entity.entity_names:
                # Obtener eventos actuales
                current_events = repository.find(entity_name, entity.session_id, date_str)
                
                # Agregar nuevo evento
                current_events.append(entity.data)
                
                # Guardar eventos actualizados
                success = repository.save(
                    entity_name,
                    entity.session_id,
                    entity.tidnid,
                    current_events,
                    date_str
                )
                
                if not success:
                    logger.error(f"Error al guardar entidad en {entity_name}")
                    return False
                    
            return True
        except Exception as e:
            logger.error(f"Error al guardar entidad: {e}")
            return False

    def process_and_save(self, message: str, repository) -> Optional[Entity]:
        """
        Procesa un mensaje, construye la entidad y la guarda.
        
        Este método coordina todo el flujo del procesamiento de eventos.
        
        Args:
            message: Mensaje a procesar
            repository: Repositorio para persistencia
            
        Returns:
            Entidad procesada y guardada o None si ocurrió un error
        """
        try:
            # Procesar el mensaje para obtener el evento
            event = self.process(message)
            
            # Obtener reglas aplicables
            rules = self.get_rules(event)
            
            # Construir la entidad
            entity = self.build_entity(repository, event, rules)
            
            # Guardar la entidad
            success = self.save_entity(repository, entity)
            
            if not success:
                logger.error("No se pudo guardar la entidad")
                return None
                
            return entity
        except Exception as e:
            logger.error(f"Error en el procesamiento completo: {e}")
            return None
