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
            
            # Formatear basándose en configuración de parámetros
            formatted_query = self._format_query_from_params_config(
                query_template, params_config, context
            )
            
            # Extraer parámetros usando strategies
            parameters = self.param_service.extract_parameters(params_config, context)
            
            logger.debug(f"Query template original: {query_template}")
            logger.debug(f"Query formateado: {formatted_query}")
            logger.debug(f"Configuración de parámetros: {params_config}")
            
            return formatted_query, parameters
            
        except Exception as e:
            logger.error(f"Error construyendo query find: {e}")
            return "", ()
    
    def _format_query_from_params_config(self, query_template: str, 
                                        params_config: Dict, 
                                        context: ParameterContext) -> str:
        """
        Formatea query basándose en la configuración real de parámetros.
        
        Esta es la corrección principal: en lugar de asumir qué va en cada 
        placeholder, lee la configuración y mapea correctamente.
        
        Args:
            query_template: Template con {0}, {1}, etc.
            params_config: Configuración de parámetros desde obs_config.json
            context: Contexto para valores dinámicos
            
        Returns:
            Query con placeholders correctamente reemplazados
        """
        try:
            # Crear mapeo de índice -> placeholder real
            placeholder_map = {}
            
            # Procesar cada parámetro configurado para construir el mapeo
            for param_idx, param_config in params_config.items():
                placeholder_name = param_config.get('placeholder', '')
                param_type = param_config.get('type', 'parameter')
                
                if param_type == 'structural':
                    # Resolver valor directamente
                    resolved_value = self._resolve_structural_value(placeholder_name, context)
                    placeholder_map[param_idx] = resolved_value
                    logger.debug(f"Estructural [{param_idx}]: {placeholder_name} -> {resolved_value}")
                else:
                    # Usar nombre del placeholder
                    placeholder_map[param_idx] = placeholder_name                
                    logger.debug(f"Parámetro [{param_idx}]: manteniendo placeholder {placeholder_name}")
            
            # Crear lista ordenada de replacements
            ordered_replacements = []
            for i in range(len(params_config)):
                replacement = placeholder_map.get(str(i), f'placeholder_{i}')
                ordered_replacements.append(replacement)
            
            # Formatear el template con los placeholders correctos
            formatted_query = query_template.format(*ordered_replacements)
            logger.debug(f"Query formateado: {formatted_query}")
            
            return formatted_query
            
        except Exception as e:
            logger.error(f"Error formateando query desde configuración: {e}")
            return query_template
        
    def _resolve_structural_value(self, placeholder_name: str, context: ParameterContext) -> str:
        """
        Resuelve valores para placeholders estructurales usando el contexto.

        Args:
            placeholder_name (str): _description_
            context (ParameterContext): _description_

        Returns:
            str: _description_
        """
        if placeholder_name == 'entity_names':
            return context.get_entity_name() or ' default_entity'
        elif placeholder_name == 'table_name':
            return context.get_context_value('table_name', 'default_table')
        elif placeholder_name == 'schema_name':
            return context.get_context_value('schema_name', 'public')
        else:
            logger.warning(f"Resolver no definido para placeholder estructural: {placeholder_name}")
            return placeholder_name
    
    def _resolve_dynamic_placeholder(self, placeholder_name: str, 
                                   context: ParameterContext) -> str:
        """
        Resuelve placeholders dinámicos que dependen del contexto.
        
        Args:
            placeholder_name: Nombre del placeholder desde configuración
            context: Contexto para resolución dinámica
            
        Returns:
            Placeholder resuelto (puede ser dinámico o el mismo nombre)
        """
        # Casos especiales donde el placeholder depende del contexto
        dynamic_resolvers = {
            'entity_names': lambda: context.get_entity_name() or 'entity_names'
        }
        
        resolver = dynamic_resolvers.get(placeholder_name)
        if resolver:
            resolved = resolver()
            logger.debug(f"Placeholder dinámico '{placeholder_name}' resuelto a: '{resolved}'")
            return resolved
        
        # Para la mayoría de casos, usar el placeholder name directamente
        return placeholder_name
