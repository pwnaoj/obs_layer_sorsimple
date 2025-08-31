"""domain/rules/strategies/set_value_strategy.py"""

from typing import Any, Dict, Optional

from .base_strategy import ActionStrategy
from obs_layer_sorsimple_mbaas.common.utils.log import logger


class SetFixedValueStrategy(ActionStrategy):
    """
    Estrategia para establecer un valor fijo predefinido.
    """
    
    def execute(self, action: Dict[str, Any], event: Dict[str, Any], 
                field: str, extensions: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Establece un valor fijo predefinido.
        
        Args:
            action: Configuración con el valor fijo
            event: No utilizado en esta estrategia
            field: Campo donde se almacenará el resultado
            extensions: No utilizado en esta estrategia
            
        Returns:
            Diccionario con el valor fijo establecido
        """
        try:
            # Obtener el valor fijo
            fixed_value = action['value']
            
            # Retornar el valor fijo
            return {field: fixed_value}
        except Exception as e:
            logger.error(f"Error al establecer valor fijo: {e}")
            return {}
