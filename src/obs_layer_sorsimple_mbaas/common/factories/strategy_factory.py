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
            
        Raises:
            ConfigurationError: Si el tipo de estrategia no está registrado
        """
        # Importación tardía para evitar dependencias circulares
        # from obs_layer_sorsimple_mbaas.domain.rules.strategies.time_calculation_strategy import TimeCalculationStrategy
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.value_extraction_strategy import ValueExtractionStrategy
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.set_value_strategy import SetValueStrategy
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.set_fixed_value_strategy import SetFixedValueStrategy
        # from obs_layer_sorsimple_mbaas.domain.rules.strategies.sum_numeric_string_strategy import SumNumericStringStrategy
        
        # Registrar estrategias por defecto si no existen
        if not cls._strategies:
            cls._strategies = {
                # 'time_difference': TimeCalculationStrategy,
                'extract_value': ValueExtractionStrategy,
                'set_value': SetValueStrategy,
                'set_fixed_value': SetFixedValueStrategy,
                # 'sum_numeric_string': SumNumericStringStrategy
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
