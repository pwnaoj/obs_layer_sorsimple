"""application/services/parameter_extraction_service.py"""

from typing import Dict, Tuple, Any
from obs_layer_sorsimple_mbaas.common.factories.strategy_factory import StrategyFactory
from obs_layer_sorsimple_mbaas.common.value_objects.parameter_context import ParameterContext
from obs_layer_sorsimple_mbaas.common.utils.log import logger


class ParameterExtractionService:
    """
    Servicio para extraer parámetros SQL usando strategies dinámicas.
    """
    
    def __init__(self):
        self.strategy_factory = StrategyFactory()
        self._register_sql_strategies()
    
    def _register_sql_strategies(self):
        """Registra las strategies específicas para parámetros SQL."""
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.sql_parameters.datetime_parameter_strategy import DateTimeParameterStrategy
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.sql_parameters.event_field_parameter_strategy import EventFieldParameterStrategy
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.sql_parameters.entity_data_parameter_strategy import EntityDataParameterStrategy
        from obs_layer_sorsimple_mbaas.domain.rules.strategies.sql_parameters.context_value_parameter_strategy import ContextValueParameterStrategy
        
        # Registrar strategies SQL si no existen
        StrategyFactory.register('datetime.now', DateTimeParameterStrategy)
        StrategyFactory.register('event', EventFieldParameterStrategy)
        StrategyFactory.register('entity', EntityDataParameterStrategy)
        StrategyFactory.register('context', ContextValueParameterStrategy)
    
    def extract_parameters(self, params_config: Dict, context: ParameterContext) -> Tuple[Any, ...]:
        """
        Extrae parámetros usando las strategies configuradas.
        
        Args:
            params_config: Configuración de parámetros desde config.db
            context: Contexto con evento, entidad y configuración
            
        Returns:
            Tupla ordenada con valores extraídos para la consulta SQL
        """
        extracted_values = {}
        
        # Procesar cada parámetro configurado
        for param_idx, param_config in params_config.items():
            try:
                value = self._extract_single_parameter(param_config, context)
                if value is not None:
                    extracted_values[param_idx] = value
            except Exception as e:
                logger.error(f"Error extrayendo parámetro {param_idx}: {e}")
                continue
        
        # Ordenar parámetros según índice numérico
        return self._order_parameters(extracted_values)
    
    def _extract_single_parameter(self, param_config: Dict, context: ParameterContext) -> Any:
        """
        Extrae un parámetro individual usando la strategy apropiada.
        
        Args:
            param_config: Configuración del parámetro
            context: Contexto de extracción
            
        Returns:
            Valor extraído o None si no se pudo extraer
        """
        requires = param_config.get('requires', '')
        placeholder = param_config.get('placeholder', '')
        
        # Crear strategy para el tipo de extracción
        strategy = self.strategy_factory.create_strategy(requires)
        if not strategy:
            logger.warning(f"Strategy no encontrada para requires: {requires}")
            return None
        
        # Preparar extensiones para la strategy
        extensions = {
            'entity': context.entity,
            'custom_context': context.custom_context
        }
        
        # Ejecutar extracción
        result = strategy.execute(param_config, context.event, placeholder, extensions)
        
        # Retornar el valor extraído
        return result.get(placeholder) if result else None
    
    def _order_parameters(self, extracted_values: Dict) -> Tuple[Any, ...]:
        """
        Ordena los parámetros según su índice numérico.
        
        Args:
            extracted_values: Diccionario con valores extraídos
            
        Returns:
            Tupla ordenada con valores para SQL
        """
        ordered_params = []
        
        # Ordenar por índice numérico
        for i in range(len(extracted_values)):
            value = extracted_values.get(str(i))
            if value is not None:
                # Convertir dict/list a JSON string para SQL
                if isinstance(value, (dict, list)):
                    import json
                    value = json.dumps(value)
                ordered_params.append(value)
        
        return tuple(ordered_params)
