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
        Formatea query manejando correctamente placeholders estructurales y parámetros.
        """
        try:
            # Crear mapeo de índice -> valor/placeholder
            placeholder_map = {}
        
            # Procesar cada parámetro según su tipo (igual que FindQueryBuilder)
            for param_idx, param_config in params_config.items():
                placeholder_name = param_config.get('placeholder', '')
                param_type = param_config.get('type', 'parameter')
            
                if param_type == 'structural':
                    # CORRECCIÓN: Resolver valor del contexto para estructurales
                    resolved_value = self._resolve_structural_value(placeholder_name, context)
                    placeholder_map[param_idx] = resolved_value
                    logger.debug(f"Estructural [{param_idx}]: {placeholder_name} -> {resolved_value}")
                else:
                    # Para parameters: usar nombre del placeholder
                    placeholder_map[param_idx] = placeholder_name
                    logger.debug(f"Parámetro [{param_idx}]: manteniendo placeholder {placeholder_name}")
        
            # Crear lista ordenada de reemplazos
            ordered_replacements = []
            for i in range(len(params_config)):
                replacement = placeholder_map.get(str(i), f'placeholder_{i}')
                ordered_replacements.append(replacement)
        
            # Formatear el template
            formatted_query = query_template.format(*ordered_replacements)
            logger.debug(f"Query formateado: {formatted_query}")
        
            return formatted_query
        
        except Exception as e:
            logger.error(f"Error formateando query: {e}")
            return query_template
    
    def _resolve_structural_value(self, placeholder_name: str, context: ParameterContext) -> str:
        """
        Resuelve valores para placeholders estructurales usando el contexto.
        """
        if placeholder_name == 'entity_names':
            return context.get_entity_name() or 'default_entity'
        elif placeholder_name == 'table_name':
            return context.get_context_value('table_name', 'default_table')
        elif placeholder_name == 'schema_name':
            return context.get_context_value('schema_name', 'public')
        else:
            logger.warning(f"Resolver no definido para placeholder estructural: {placeholder_name}")
            return placeholder_name
    
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
