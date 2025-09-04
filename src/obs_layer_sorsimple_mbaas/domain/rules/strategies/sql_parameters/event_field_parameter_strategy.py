"""domain/rules/strategies/sql_parameters/event_field_parameter_strategy.py"""

import jmespath
from typing import Any, Dict, Optional

from obs_layer_sorsimple_mbaas.domain.rules.strategies.base_strategy import ActionStrategy
from obs_layer_sorsimple_mbaas.common.utils.log import logger


class EventFieldParameterStrategy(ActionStrategy):
    """Estrategia para extraer valores del evento usando JMESPath."""
    
    def execute(self, action: Dict[str, Any], event: Dict[str, Any], 
                field: str, extensions: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        try:
            jmespath_query = action.get('value', '')
            if not jmespath_query:
                return {}
                
            value = jmespath.search(jmespath_query, event)
            return {field: value} if value is not None else {}
        except Exception as e:
            logger.error(f"Error en EventFieldParameterStrategy: {e}")
            return {}
