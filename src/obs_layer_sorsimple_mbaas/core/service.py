"""src/core/service.py"""

import json
import jmespath

from datetime import datetime
from flatten_json import flatten, unflatten_list, unflatten
from typing import Any, Dict, List, Optional, Union

from .interfaces.message_processor import MessageProcessor
from .rules import RuleEngine
from ..utils.models import Record
from ..utils.log import logger
from ..utils.jmespath import extract_from_message_selected_fields


class EventProcessor(MessageProcessor):
    """
    Filtra variables de tramaba Mbaas según reglas de negocio configuradas.
    """

    def __init__(self, s3_config: Dict[str, Any]):
        """
        Inicializa el procesador de servicios.

        Args:
            s3_config (Dict): Parametrización de reglas de negocio.
        """
        self.s3_config = s3_config
        self._event_data: Optional[Dict[str, Any]] = None

    def _parse_record(self, record: Union[str, Dict]) -> Dict:
        """
        Parsea y valida los datos del mensaje SQS.

        Args:
            record (Union[str, Dict]): Mensaje SQS que contiene el evento mbaas procesado.

        Returns:
            Dict: Mensaje SQS parseado a diccionario.
        """
        # Validar estructura del mensaje y convertir a json
        validated_record = Record.model_validate_json(record)
        parsed_record = validated_record.model_dump()
        
        return parsed_record

    def _get_record_body(self, parsed_record: Dict[str, Any]) -> Dict:
        """
        Extrae el mensaje procesado del mbaas y parsea a diccionario.
        
        Args:
            parsed_record (Dict[str, Any]): Record SQS parseado a json.

        Returns:
            Dict: Diccionario con datos procesados del evento mbaas.
        """
        # Extraigo campo body
        body = parsed_record.get('body', {})
        
        while isinstance(body, str):
            try:
                body = json.loads(body)
            except json.JSONDecodeError:
                break
            
        return body

    def process(self, message: str) -> Dict:
        """
        Procesador de datos del evento mbaas.
        
        Args:
            message (str): Mensaje a procesar.

        Returns:
            Dict: Diccionario con los datos procesados del evento mbaas.
        """
        # Parsear mensaje recibido de SQS
        parsed_record = self._parse_record(message)
        
        # Extraer body del mensaje parseado
        body = self._get_record_body(parsed_record)
        
        return body

    def get_rules(self, event: Dict) -> Optional[List[Dict]]:
        """
        Extrae las reglas de negocio que son aplicables al servicio mbaas.

        Args:
            event (Dict): Evento con datos procesados del evento mbaas.

        Returns:
            Optional[List[Dict]]: Lista de diccionarios que contien los campos relacionados a las reglas de negocio.
        """
        event = self._unflatten_json(event)
        
        return self._extract_rules(event)

    def _unflatten_json(self, event: Dict) -> Dict:
        """
        Desacopla el evento (json) aplanado a una estructura json con niveles.

        Args:
            event (Dict): Evento mbaas procesado.

        Returns:
            Dict: Diccionario que contiene la data procesada del servicio mbaas.
        """
        # Se aplana el evento una vez para generar nuevas claves en el diccionario
        # usando índices para los elementos de las listas
        event = flatten(event, '.')
        
        return unflatten_list(event, '.')

    def _extract_rules(self, event: Dict) -> Optional[List[Dict]]:
        """
        Extrae las reglas de negocio aplicables al servicio mbaas.

        Args:
            event (Dict): Evento con datos procesados servcio mbaas.

        Returns:
            Optional[List[Dict]]: Lista de diccionarios con las reglas de negocio aplicables.
        """
        id_service = jmespath.search('jsonPayload.dataObject.messages.idService', event).strip()
        consumer_id = jmespath.search('jsonPayload.dataObject.consumer.appConsumer.id', event).strip()
        rules = jmespath.search(f"[?id=='{consumer_id}'].rules[]", self.s3_config)
        
        if not rules:
            return None
        
        return jmespath.search(f"[?event_type=='{id_service}']", rules)

    def build_entity(
            self, db_service: 'DatabaseService',
            event: Dict,
            rules: Optional[List[Dict]] = None,
            extensions: Optional[Dict[Any, str]] = None
        ) -> Dict:
        """
        Construye la entidad con las reglas aplicables al servicio mbaas.

        Args:
            db_service (DatabaseService): Clase para operaciones de Base de Datos.
            event (Dict): Evento con datos procesados servcio mbaas.
            rules (Optional[List[Dict]], optional): Reglas de negocio aplicables. Defaults to None.
            extensions: (Optional[Dict[Any, str]], optional): Extensiones adicionales.

        Returns:
            Dict: Diccionario con datos de la entidad.
        """
        try:
            event_data = self._prepare_entity_data(event, db_service)
            
            if rules:
                engine = RuleEngine(rules, extensions)
                event_data['data']['rules'] = engine.process_event(event)
            else:
                event_data['data']['rules'] = {}
                
            return event_data
        
        except Exception as e:
            logger.error(f"Error al construir la entidad: {e}")
            
            raise

    def _prepare_entity_data(self, event: Dict, db_service: 'DatabaseService') -> Dict:
        """
        Extrae campos base para la construcción de la entidad.

        Args:
            event (Dict): Evento con datos procesados servcio mbaas.
            db_service (DatabaseService): Clase para operaciones de Base de Datos.

        Returns:
            Dict: Entidad con reglas de negocio aplicables y data mínima.
        """
        # Desacopla el evento para aplicar querys jmespath
        event = self._unflatten_json(event)
        
        # Extrae variables requeridas para poblar entidad
        id_service = jmespath.search('jsonPayload.dataObject.messages.idService', event).strip()
        consumer_id = jmespath.search('jsonPayload.dataObject.consumer.appConsumer.id', event).strip()
        session_id = jmespath.search('jsonPayload.dataObject.consumer.appConsumer.sessionId', event).strip()
        services = jmespath.search(f"[?id=='{consumer_id}'].services[]", self.s3_config)
        paths = jmespath.search(f"[?id_service=='{id_service}'].paths[]", services)
        entity = self._get_entity(id_service, services, event)
        timestamp = jmespath.search("timestamp", event)
        
        return {
            'entity_names': entity,
            'session_id': session_id,
            'tidnid': self._get_tidnid(event, session_id, db_service),
            'data': {
                'id_service': id_service,
                'timestamp': timestamp,
                'service': dict(extract_from_message_selected_fields(paths, event))
            }
        }

    def _get_entity(self, id_service: str, services: List[Dict], event: Dict) -> str:
        """
        Recupera el nombre de la entidad de negocio del evento procesado mbaas o workflow.

        Args:
            event (Dict): Evento procesado mbaas o workflow.

        Returns:
            str: Nombre entidad.
        """
        entity_general = jmespath.search(f"[?id_service=='{id_service}'].entity[][]", services)
        entity_workflow = jmespath.search("jsonPayload.dataObject.messages.transaction.transactionName", event)
        
        if entity_general:
            return entity_general
        elif entity_workflow:
            return [entity_workflow]
    
    def _get_tidnid(self, event: Dict, session_id: str, db_service: 'DatabaseService') -> Optional[str]:
        """
        Recupera el tidnid del evento procesado del servicio mbaas.

        Args:
            event (Dict): Evento con datos procesados servcio mbaas.
            session_id (str): Session Id (identificador único) del evento.
            db_service (DatabaseService): Instancia cliente DB.

        Returns:
            Optional[str]: tidnid del cliente.
        """
        tidnid = jmespath.search("(jsonPayload.dataObject.documento || jsonPayload.dataObject.client.documentClient) | join('-', [tipo || type, numero || number])", event)
        
        if tidnid:
            return tidnid
        else:
            # Si no se encuentra el tidnid en el evento procesado se consulta BD para extraer tidnid
            return db_service.get_events(
                sql="",
                params=(datetime.now().strftime('%Y%m%d'), session_id)
            )

    def save_entity(self, db_service: 'DatabaseService', entity: Dict) -> None:
        """
        Guarda los datos de la entidad.

        Args:
            db_service (DatabaseService): Cliente de Base de Datos.
            entity (Dict): Entidad con los datos que serán almacenados.
        """
        try:
            for entity_name in entity['entity_names']:
                current_events = db_service.get_events(
                    sql="{}".format(entity_name),
                    params=(datetime.now().strftime('%Y%m%d'), entity['session_id'])
                ) or []

                current_events.append(entity['data'])

                db_service.save_events(
                    sql="{}".format(entity_name),
                    params=(entity['session_id'], entity['tidnid'], 
                           json.dumps(current_events), datetime.now().strftime('%Y%m%d'))
                )
        except Exception as e:
            logger.error(f"Error al intentar guardar la entidad en la Base de Datos: {e}")
            raise
