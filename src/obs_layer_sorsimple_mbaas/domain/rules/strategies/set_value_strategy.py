"""domain/rules/strategies/set_value_strategy.py"""

from typing import Any, Dict, Optional

from .base_strategy import ActionStrategy
from obs_layer_sorsimple_mbaas.common.utils.log import logger


class SetValueStrategy(ActionStrategy):
    """
    Estrategia para establecer un valor obtenido del evento.
    """
    
    def execute(self, action: Dict[str, Any], event: Dict[str, Any], 
                field: str, extensions: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Establece un valor obtenido del evento.
        
        Args:
            action: Configuración con el valor a obtener
            event: Evento del que extraer el valor
            field: Campo donde se almacenará el resultado
            extensions: No utilizado en esta estrategia
            
        Returns:
            Diccionario con el valor establecido
        """
        try:
            import jmespath
            # Obtener la clave de origen
            source_field = action['value']
            
            # Extraer el valor del evento
            value = event.get(source_field) or jmespath.search(source_field, event)
            
            # Solo retornar si se encontró un valor
            if value is not None:
                return {field: value}
            return {}
        except Exception as e:
            logger.error(f"Error al establecer valor: {e}")
            return {}
