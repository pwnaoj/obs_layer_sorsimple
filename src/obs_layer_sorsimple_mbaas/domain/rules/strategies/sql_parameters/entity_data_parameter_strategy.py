"""domain/rules/strategies/sql_parameters/entity_data_parameter_strategy.py"""

import json

from typing import Any, Dict, Optional

from obs_layer_sorsimple_mbaas.domain.rules.strategies.base_strategy import ActionStrategy
from obs_layer_sorsimple_mbaas.common.utils.log import logger


class EntityDataParameterStrategy(ActionStrategy):
    """Estrategia para extraer datos de la entidad."""
    
    def execute(self, action: Dict[str, Any], event: Dict[str, Any], 
                field: str, extensions: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        try:
            # Obtener entity del contexto (se pasará via extensions)
            entity = extensions.get('entity') if extensions else None
            
            if not entity:
                return {}
            
            # Si no hay valor específico, retornar toda la data de la entidad
            if not action.get('value'):
                entity_data = entity.data if hasattr(entity, 'data') else {}
                return {field: json.dumps(entity_data) if isinstance(entity_data, dict) else entity_data}
            
            # Si hay valor específico, extraerlo de la entidad
            field_name = action.get('value', '')
            entity_data = entity.data if hasattr(entity, 'data') else {}
            value = entity_data.get(field_name)
            
            return {field: value} if value is not None else {}
        except Exception as e:
            logger.error(f"Error en EntityDataParameterStrategy: {e}")
            return {}
