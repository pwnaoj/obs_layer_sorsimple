"""common/factories/strategy_factory.py"""

from typing import Optional, Dict, Type

from obs_layer_sorsimple_mbaas.common.utils.log import logger
from obs_layer_sorsimple_mbaas.common.exceptions.domain_exceptions import ConfigurationError


class StrategyFactory:
    """
    Fábrica para crear estrategias de acción siguiendo el patrón Factory Method.
    """
    
    _strategies = {}
    
    @classmethod
    def register(cls, action_type: str, strategy_class: Type):
        """
        Registra un tipo de estrategia en la fábrica.
        
        Args:
            action_type: Tipo de acción
            strategy_class: Clase de la estrategia
        """
        cls._strategies[action_type.lower()] = strategy_class
        
    @classmethod
    def create_strategy(cls, action_type: str):
        """
        Crea una estrategia basada en el tipo de acción.
        
        Args:
            action_type: Tipo de acción a ejecutar
            
        Returns:
            Instancia de la estrategia creada o None si no se encuentra
        """
        # Importaciones tardías para evitar dependencias circulares
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.value_extraction_strategy import ValueExtractionStrategy
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.set_value_strategy import SetValueStrategy
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.set_fixed_value_strategy import SetFixedValueStrategy
        
        # Importar nuevas strategies para parámetros SQL
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.sql_parameters.datetime_parameter_strategy import DateTimeParameterStrategy
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.sql_parameters.event_field_parameter_strategy import EventFieldParameterStrategy
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.sql_parameters.entity_data_parameter_strategy import EntityDataParameterStrategy
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.sql_parameters.context_value_parameter_strategy import ContextValueParameterStrategy
        
        # Registrar estrategias por defecto si no existen
        if not cls._strategies:
            cls._strategies = {
                # Strategies originales para BusinessRule
                'extract_value': ValueExtractionStrategy,
                'set_value': SetValueStrategy,
                'set_fixed_value': SetFixedValueStrategy,
                
                # Nuevas strategies para parámetros SQL
                'datetime.now': DateTimeParameterStrategy,
                'event': EventFieldParameterStrategy,
                'entity': EntityDataParameterStrategy,
                'context': ContextValueParameterStrategy,
            }
            
        # Normalizar el tipo de acción
        normalized_type = action_type.lower() if action_type else ""
        
        # Crear y retornar la estrategia
        strategy_class = cls._strategies.get(normalized_type)
        if not strategy_class:
            logger.warning(f"Tipo de acción no soportado: {action_type}")
            return None
            
        try:
            return strategy_class()
        except Exception as e:
            error_msg = f"Error al crear estrategia {action_type}: {e}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg, {"original_error": str(e)})
    
    @classmethod
    def get_registered_strategies(cls) -> Dict[str, Type]:
        """
        Retorna todas las strategies registradas.
        
        Returns:
            Diccionario con tipos de acción y sus clases
        """
        return cls._strategies.copy()
    
    @classmethod
    def is_strategy_registered(cls, action_type: str) -> bool:
        """
        Verifica si una strategy está registrada.
        
        Args:
            action_type: Tipo de acción a verificar
            
        Returns:
            True si está registrada, False caso contrario
        """
        return action_type.lower() in cls._strategies
