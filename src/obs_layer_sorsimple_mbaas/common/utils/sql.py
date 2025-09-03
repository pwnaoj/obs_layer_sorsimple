"""common/utils/sql.py"""

import json
import jmespath

from datetime import datetime
from typing import Any, Dict, Tuple

from .log import logger


class SQLBuilder:
   
    def __init__(self, db_config: Dict, context: Dict = None):
        """
        Inicializa configuración.

        Args:
            db_config (Dict): Configuración base de datos que contiene query y params.
            context (Dict, optional): Contexto adicional con valores como entity_name.
        """
        self._db_config = db_config
        self._context = context or {}

    def _validate_config(self, event_unflat: Dict, query_type: str) -> Tuple[str, Dict]:
        """
        Valida si existe una configuración de query y params para el appConsumer.

        Args:
            event_unflat (Dict): Evento del que se extraen valores.
            query_type (str): Query a validar.

        Raises:
            ValueError: Si no existe configuración para query/params.

        Returns:
            Tuple[str, Dict]: Tupla con query y params.
        """
        app_consumer_id = jmespath.search('jsonPayload.dataObject.consumer.appConsumer.id', event_unflat)
       
        # Validar que exista una configuración
        if self._db_config is None:
            raise ValueError(f"No existe una configuración de base de datos para appConsumer='{app_consumer_id}'.")
       
        # Extraer querys configurados
        querys: Dict = self._db_config.get('querys', {})
       
        # Validar que exista la consulta solicitada
        if query_type not in querys:
            raise ValueError(f"No existe la consulta '{query_type}' para appConsumer='{app_consumer_id}'.")
       
        query_config = querys.get(query_type, {})
        query = query_config.get('query')
        params = query_config.get('params')
       
        # Validar que existan query y params
        if not query:
            raise ValueError(f"No existe la definición de query '{query_type}' para appConsumer='{app_consumer_id}'.")
       
        if not params:
            raise ValueError(f"No existen params configurados para query '{query_type}', appConsumer='{app_consumer_id}'.")

        return query, params
   
    def _process_jmespath_expression(self, expression: str, event_unflat: Dict, default=None):
        """
        Procesa expresiones JMESPath y maneja errores.
       
        Args:
            expression (str): Expresión JMESPath a evaluar
            event_unflat (Dict): Evento donde buscar los valores
            default: Valor por defecto si la expresión falla
           
        Returns:
            Any: Valor extraído o valor por defecto
        """
        try:
            if not expression:
                return default
               
            result = jmespath.search(expression, event_unflat)
            return result if result is not None else default
        except Exception as e:
            logger.warning(f"Error al procesar expresión JMESPath '{expression}': {e}")
            return default
   
    def _build_query(self, query: str, params: Dict) -> str:
        """
        Construye el query formateado.

        Args:
            query (str): Query disponible.
            params (Dict): Params disponible.

        Returns:
            str: Query formateado.
        """
        # Crear diccionario de placeholders ordenados por índice
        placeholders = {}
        for param_idx, param_config in params.items():
            placeholder = param_config.get('placeholder', '')
            placeholders[param_idx] = placeholder
       
        # Ordenar placeholders según el índice
        ordered_placeholders = [placeholders.get(str(i), f'param_{i}') for i in range(len(params))]
       
        # Reemplazar los marcadores de posición en la consulta
        query_formatted = query.format(*ordered_placeholders)
        
        # Reemplazar {entity_name} si está presente en el contexto
        if 'entity_names' in query_formatted and 'entity_name' in self._context:
            query_formatted = query_formatted.replace('entity_names', self._context['entity_name'])
        
        return query_formatted
   
    def _build_params(self, params_config: Dict, event_unflat: Dict, entity=None) -> Tuple[Any, ...]:
        """
        Construye los parámetros para la consulta SQL.
       
        Args:
            params_config (Dict): Configuración de parámetros
            event_unflat (Dict): Evento del que se extraen valores
            entity: Entidad con datos adicionales (opcional)
           
        Returns:
            Tuple[Any, ...]: Tupla ordenada de valores para la consulta SQL
        """
        # Diccionario para almacenar los valores extraídos
        param_values = {}
        
        # Procesar cada parámetro según su configuración
        for param_idx, param_config in params_config.items():
            placeholder = param_config.get('placeholder', '')
            requires = param_config.get('requires')
            value = param_config.get('value')
            
            extracted_value = None
           
            # Extraer valor según el origen
            if requires == 'datetime.now':
                # Usar el formato de fecha especificado
                format_str = value or "%Y%m%d"
                extracted_value = datetime.now().strftime(format_str)
            
            elif requires == 'obs_config':
                if placeholder == 'etapa_journey':
                    value = value.format(self._context.get('app_consumer_id'), self._context.get('id_service'))
                    extracted_value = self._process_jmespath_expression(
                        value, 
                        self._context.get('obs_config')
                    )
            
            elif requires == 'event':
                # Extraer valor del evento usando JMESPath
                extracted_value = self._process_jmespath_expression(value, event_unflat)
                   
            elif requires == 'entity':
                # Extraer valor de la entidad
                if entity:
                    if hasattr(entity, placeholder) and placeholder == 'entity_names':
                        continue
                    elif hasattr(entity, placeholder):
                        extracted_value = entity.__getattribute__(placeholder)
                        if isinstance(extracted_value, list):
                            extracted_value = extracted_value[0]
                    elif isinstance(entity, dict) and placeholder in entity:
                        extracted_value = entity[placeholder]
                    elif placeholder == 'datos_evento' and entity.data:
                        extracted_value = entity.data
           
            # Si no se pudo extraer un valor, intentar usar un valor del contexto
            if extracted_value is None and placeholder in self._context:
                extracted_value = self._context.get(placeholder)
               
            # Almacenar el valor extraído
            param_values[param_idx] = extracted_value
       
        # Ordenar los parámetros según el índice para asegurar el orden correcto
        ordered_values = []
        for i in range(len(params_config)):
            value = param_values.get(str(i))
            # Si el valor es None no almacenar
            if value is None:
                continue
            # Si el valor es un diccionario o lista, convertirlo a JSON
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
               
            ordered_values.append(value)
       
        return tuple(ordered_values)
   
    def build_query_and_params(self, event_unflat: Dict, query_type: str, entity=None, **kwargs):
        """
        Construye el query formateado y genera los params correspondientes.

        Args:
            event_unflat (Dict): Evento del que se extraen valores.
            query_type (str): Query a validar.
            entity: Entidad con datos adicionales (opcional)
            **kwargs: Valores adicionales para el contexto
           
        Returns:
            Tuple[str, Tuple]: Consulta formateada y parámetros
        """
        # Actualizar contexto con valores adicionales
        self._context.update(kwargs)
       
        # Validar configuración
        query, params = self._validate_config(event_unflat, query_type)
       
        # Construir query formateado
        query_formatted = self._build_query(query, params)
       
        # Extraer parámetros
        params_extracted = self._build_params(params, event_unflat, entity)
       
        logger.debug(f"Query: {query_formatted}")
        logger.debug(f"Params: {params_extracted}")
       
        return query_formatted, params_extracted
