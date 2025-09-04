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
        """
        Inicializa el servicio. StrategyFactory maneja todos los registros automáticamente.
        """
        self.strategy_factory = StrategyFactory()
    
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
            param_type = param_config.get('type', 'parameter')
            
            if param_type == 'structural':
                logger.debug(f"Saltando parámetro estructural en índice {param_idx}")
                continue
            
            try:
                value = self._extract_single_parameter(param_config, context)
                extracted_values[param_idx] = value
                logger.debug(f"Parámetro {param_idx} extraído: {value}")
            except Exception as e:
                logger.error(f"Error extrayendo parámetro {param_idx}: {e}")
                extracted_values[param_idx] = None
        
        # Ordenar parámetros según índice numérico
        ordered_params = self._order_parameters(extracted_values)
        logger.debug(f"Parámetros finales ordenados: {ordered_params}")
        
        return ordered_params
    
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
        
        logger.debug(f"Extrayendo parámetro - requires: {requires}, placeholder: {placeholder}")
        
        # Crear strategy para el tipo de extracción
        strategy = self.strategy_factory.create_strategy(requires)
        if not strategy:
            logger.warning(f"Strategy no encontrada para requires: {requires}")
            return None
        
        # Preparar extensiones para la strategy
        extensions = {
            'entity': context.entity,
            'custom_context': context.custom_context,
            'full_context': context
        }
        
        # Ejecutar extracción
        result = strategy.execute(param_config, context.event, placeholder, extensions)
        
        # Retornar el valor extraído
        extracted_value = result.get(placeholder) if result else None
        logger.debug(f"Valor extraído para {placeholder}: {extracted_value}")
        
        return extracted_value
    
    def _order_parameters(self, extracted_values: Dict) -> Tuple[Any, ...]:
        """
        Ordena los parámetros según su índice numérico.
        
        Args:
            extracted_values: Diccionario con valores extraídos
            
        Returns:
            Tupla ordenada con valores para SQL
        """
        ordered_params = []
        
        # Obtener todos los índices dsponibles y ordenarlos
        available_indices = sorted([int(k) for k in extracted_values.keys()])
        
        # Ordenar por índice numérico (0, 1, 2, ...)
        for index in available_indices:
            value = extracted_values.get(str(index))
            
            if value is not None and isinstance(value, (dict, list)):
                # Convertir dict/list a JSON string para SQL
                import json
                value = json.dumps(value)
            
            # Incluir el valor (puede ser None)
            ordered_params.append(value)
        
        return tuple(ordered_params)
