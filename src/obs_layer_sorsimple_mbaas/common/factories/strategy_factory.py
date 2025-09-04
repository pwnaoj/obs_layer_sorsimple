"""common/factories/strategy_factory.py"""

from typing import Optional, Dict, Type

from obs_layer_sorsimple_mbaas.common.utils.log import logger
from obs_layer_sorsimple_mbaas.common.exceptions.domain_exceptions import ConfigurationError


class StrategyFactory:
    """
    Fábrica para crear estrategias de acción siguiendo el patrón Factory Method.
    CORREGIDO: Registra todas las strategies al inicio para evitar conflictos.
    """
    
    _strategies = {}
    _initialized = False
    
    @classmethod
    def _initialize_default_strategies(cls):
        """
        Inicializa todas las strategies disponibles de una sola vez.
        Se ejecuta solo una vez para evitar conflictos.
        """
        if cls._initialized:
            return
            
        # Importaciones tardías para evitar dependencias circulares
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.value_extraction_strategy import ValueExtractionStrategy
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.set_value_strategy import SetValueStrategy
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.set_fixed_value_strategy import SetFixedValueStrategy
        
        # Importar strategies para parámetros SQL
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.sql_parameters.datetime_parameter_strategy import DateTimeParameterStrategy
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.sql_parameters.event_field_parameter_strategy import EventFieldParameterStrategy
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.sql_parameters.entity_data_parameter_strategy import EntityDataParameterStrategy
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.sql_parameters.context_value_parameter_strategy import ContextValueParameterStrategy
        
        # Registrar TODAS las strategies disponibles
        default_strategies = {
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
        
        # Solo agregar strategies que no existan ya
        for action_type, strategy_class in default_strategies.items():
            if action_type.lower() not in cls._strategies:
                cls._strategies[action_type.lower()] = strategy_class
        
        cls._initialized = True
        
        logger.debug(f"StrategyFactory inicializado con {len(cls._strategies)} strategies")
    
    @classmethod
    def register(cls, action_type: str, strategy_class: Type):
        """
        Registra un tipo de estrategia en la fábrica.
        
        Args:
            action_type: Tipo de acción
            strategy_class: Clase de la estrategia
        """
        # Asegurar inicialización antes de registrar
        cls._initialize_default_strategies()
        
        cls._strategies[action_type.lower()] = strategy_class
        logger.debug(f"Strategy '{action_type}' registrada exitosamente")
        
    @classmethod
    def create_strategy(cls, action_type: str):
        """
        Crea una estrategia basada en el tipo de acción.
        
        Args:
            action_type: Tipo de acción a ejecutar
            
        Returns:
            Instancia de la estrategia creada o None si no se encuentra
        """
        # Asegurar que las strategies por defecto estén registradas
        cls._initialize_default_strategies()
        
        # Normalizar el tipo de acción
        normalized_type = action_type.lower() if action_type else ""
        
        # Crear y retornar la estrategia
        strategy_class = cls._strategies.get(normalized_type)
        if not strategy_class:
            logger.warning(f"Tipo de acción no soportado: {action_type}")
            logger.debug(f"Strategies disponibles: {list(cls._strategies.keys())}")
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
        cls._initialize_default_strategies()
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
        cls._initialize_default_strategies()
        return action_type.lower() in cls._strategies
        
    @classmethod
    def reset_strategies(cls):
        """
        Resetea todas las strategies registradas. Útil para testing.
        """
        cls._strategies.clear()
        cls._initialized = False
