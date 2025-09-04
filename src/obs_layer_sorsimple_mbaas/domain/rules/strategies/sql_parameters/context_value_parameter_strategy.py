"""domain/rules/strategies/sql_parameters/context_value_parameter_strategy.py"""

from typing import Any, Dict, Optional

from obs_layer_sorsimple_mbaas.domain.rules.strategies.base_strategy import ActionStrategy
from obs_layer_sorsimple_mbaas.common.utils.log import logger


class ContextValueParameterStrategy(ActionStrategy):
    """Estrategia para extraer valores del contexto personalizado."""
    
    def execute(self, action: Dict[str, Any], event: Dict[str, Any], 
                field: str, extensions: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        try:
            context_key = action.get('value', '')
            if not context_key:
                return {field: None}
            
            full_context = extensions.get('full_context') if extensions else None
            
            if context_key == 'entity_name' and full_context:
                entity_name = full_context.get_entity_name()
                logger.debug(f"Entity name del contexto: {entity_name}")
                return {field: entity_name}
            
            # Buscar en el contexto personalizado
            custom_context = extensions.get('custom_context', {}) if extensions else {}
            value = custom_context.get(context_key)
            
            return {field: value}
        except Exception as e:
            logger.error(f"Error en ContextValueParameterStrategy: {e}")
            return {field: None}
        