"""infrastructure/query_builders/save_query_builder.py"""

from typing import Dict, Tuple

from obs_layer_sorsimple_mbaas.domain.interfaces.query_builder import QueryBuilder
from obs_layer_sorsimple_mbaas.common.value_objects.parameter_context import ParameterContext
from obs_layer_sorsimple_mbaas.application.services.parameter_extraction_service import ParameterExtractionService
from obs_layer_sorsimple_mbaas.common.utils.log import logger


class SaveQueryBuilder(QueryBuilder):
    """
    Constructor específico para consultas de tipo 'save'.
    """
    
    def __init__(self):
        self.param_service = ParameterExtractionService()
    
    def build_query(self, query_config: Dict, context: ParameterContext) -> Tuple[str, Tuple]:
        """
        Construye consulta de guardado con parámetros extraídos.
        
        Args:
            query_config: Configuración con query y params
            context: Contexto con datos necesarios
            
        Returns:
            Tupla con consulta formateada y parámetros
        """
        try:
            # Extraer template de consulta
            query_template = query_config.get('query', '')
            params_config = query_config.get('params', {})
            
            # Formatear consulta con placeholders de contexto
            formatted_query = self._format_query_from_params_config(query_template, params_config, context)
            
            # Extraer parámetros usando strategies
            parameters = self.param_service.extract_parameters(params_config, context)
            
            return formatted_query, parameters
        except Exception as e:
            logger.error(f"Error construyendo query save: {e}")
            return "", ()
    
    def _format_query_from_params_config(self, query_template: str,
                                    params_config: Dict,
                                    context: ParameterContext) -> str:
        """
        Formateo simple para el caso donde todos los placeholders son tipo 'parameter'.
        Reemplaza {0}, {1}, etc. con los nombres de los placeholders configurados.
        """
        try:
            # Crear lista ordenada de nombres de placeholders
            replacements = []
        
            for i in range(len(params_config)):
                param_config = params_config.get(str(i), {})
                placeholder_name = param_config.get('placeholder', f'param_{i}')
                replacements.append(placeholder_name)
        
            # Formatear el template con los nombres reales
            formatted_query = query_template.format(*replacements)
        
            return formatted_query
        
        except Exception as e:
            logger.error(f"Error simple formateando query: {e}")
            return query_template
    
    def _format_query_placeholders(self, query_template: str, context: ParameterContext) -> str:
        """
        Reemplaza placeholders dinámicos en el query template.
        
        Args:
            query_template: Template de consulta con {0}, {1}, etc.
            context: Contexto con datos para reemplazo
            
        Returns:
            Query con placeholders de contexto reemplazados
        """
        try:
            # Crear diccionario de reemplazos dinámicos
            replacements = []
            
            # Mapear placeholders comunes desde el contexto
            entity_name = context.get_entity_name()
            if entity_name:
                replacements.append(entity_name)
            
            # Agregar otros placeholders fijos
            replacements.extend(['sessionid', 'tidnid', 'data', 'fecha_part'])
            
            # Formatear query con los reemplazos
            return query_template.format(*replacements)
        except Exception as e:
            logger.error(f"Error formateando query: {e}")
            return query_template
