"""infrastructure/query_builders/find_query_builder.py"""

from typing import Dict, Tuple

from obs_layer_sorsimple_mbaas.domain.interfaces.query_builder import QueryBuilder
from obs_layer_sorsimple_mbaas.common.value_objects.parameter_context import ParameterContext
from obs_layer_sorsimple_mbaas.application.services.parameter_extraction_service import ParameterExtractionService
from obs_layer_sorsimple_mbaas.common.utils.log import logger


class FindQueryBuilder(QueryBuilder):
    """
    Constructor específico para consultas de tipo 'find' y 'find_tidnid'.
    """
    
    def __init__(self):
        self.param_service = ParameterExtractionService()
    
    def build_query(self, query_config: Dict, context: ParameterContext) -> Tuple[str, Tuple]:
        """
        Construye consulta de búsqueda con parámetros extraídos.
        
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
            
            # Formatear consulta con placeholders
            formatted_query = self._format_query_placeholders(query_template, context)
            
            # Extraer parámetros
            parameters = self.param_service.extract_parameters(params_config, context)
            
            return formatted_query, parameters
        except Exception as e:
            logger.error(f"Error construyendo query find: {e}")
            return "", ()
    
    def _format_query_placeholders(self, query_template: str, context: ParameterContext) -> str:
        """
        Reemplaza placeholders para consultas de búsqueda.
        
        Args:
            query_template: Template con {0}, {1}, etc.
            context: Contexto con datos
            
        Returns:
            Query formateado
        """
        try:
            # Para find queries, típicamente usan entity_name, fecha_part, sessionid
            entity_name = context.get_entity_name()
            replacements = [entity_name, 'fecha_part', 'sessionid'] if entity_name else ['fecha_part', 'sessionid']
            
            return query_template.format(*replacements)
        except Exception as e:
            logger.error(f"Error formateando find query: {e}")
            return query_template
