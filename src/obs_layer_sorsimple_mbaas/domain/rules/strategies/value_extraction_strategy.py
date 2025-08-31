"""domain/rules/strategies/value_extraction_strategy.py"""

import jmespath

from flatten_json import unflatten
from typing import Any, Dict, Optional

from .base_strategy import ActionStrategy
from obs_layer_sorsimple_mbaas.common.utils.log import logger


class ValueExtractionStrategy(ActionStrategy):
    """
    Estrategia para extraer valores usando consultas JMESPath.
    """
    
    def execute(self, action: Dict[str, Any], event: Dict[str, Any], 
                field: str, extensions: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Extrae valores usando consultas JMESPath.
        
        Args:
            action: Configuración con la consulta JMESPath
            event: Evento del que extraer datos
            field: Campo donde se almacenará el resultado
            extensions: Datos de extensión opcionales para consultas
            
        Returns:
            Diccionario con el resultado extraído
        """
        try:
            # Obtener la consulta
            query = action['query']
            
            # Determinar si se requieren extensiones
            require_ext = action.get('require_ext', 'false').lower() == 'true'
            
            # Desaplanar el evento para estructura jerárquica
            event_unflat = unflatten(event, '.')
            
            if require_ext and extensions:
                # Extraer valor del campo de condición
                conditions = action.get('conditions', [])
                condition_field = jmespath.search('[].field | [0]', conditions) if conditions else None
                value = jmespath.search(condition_field, event_unflat) if condition_field else None
                
                # Buscar en extensiones
                name_ext = action.get('name_ext')
                ext_data = extensions.get(name_ext, []) if name_ext else []
                
                # Formatear consulta si hay valor
                formatted_query = query.format(value) if value is not None else query
                result = jmespath.search(formatted_query, ext_data)
            else:
                # Búsqueda directa en el evento
                result = jmespath.search(query, event_unflat)
                
            return {field: result} if result is not None else {}
        except Exception as e:
            logger.error(f"Error al extraer valor: {e}")
            return {}
