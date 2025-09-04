"""domain/rules/strategies/sql_parameters/datetime_parameter_strategy.py"""

from typing import Any, Dict, Optional
from datetime import datetime

from obs_layer_sorsimple_mbaas.domain.rules.strategies.base_strategy import ActionStrategy
from obs_layer_sorsimple_mbaas.common.utils.log import logger


class DateTimeParameterStrategy(ActionStrategy):
    """Estrategia para extraer valores de fecha/hora."""
    
    def execute(self, action: Dict[str, Any], event: Dict[str, Any], 
                field: str, extensions: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        try:
            format_str = action.get('value', '%Y%m%d')
            date_value = datetime.now().strftime(format_str)
            return {field: date_value}
        except Exception as e:
            logger.error(f"Error en DateTimeParameterStrategy: {e}")
            return {}
