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
                return {}
                
            # Buscar en el contexto personalizado
            custom_context = extensions.get('custom_context', {}) if extensions else {}
            value = custom_context.get(context_key)
            
            return {field: value} if value is not None else {}
        except Exception as e:
            logger.error(f"Error en ContextValueParameterStrategy: {e}")
            return {}
        