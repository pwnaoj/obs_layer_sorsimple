"""domain/rules/engine.py"""

import jmespath

from datetime import datetime, timezone
from flatten_json import unflatten
from typing import Any, Dict, List, Optional, Union

from obs_layer_sorsimple_mbaas.common.utils.log import logger


class BusinessRule:
    """
    Regla de negocio que evalúa condiciones y ejecuta acciones.
    """

    def __init__(self, rule_config: Dict, extensions: Optional[Dict[str, Any]] = None):
        """
        Inicializa una regla con su configuración.
        
        Args:
            rule_config: Configuración de la regla
            extensions: Datos de extensión opcionales
        """
        self.config = rule_config
        self.desc = self.config['description']
        self.extensions = extensions
        self.conditions = rule_config.get('conditions', [])
        self.actions = rule_config.get('actions', [])

    def is_applicable(self, event: Dict, current_time: datetime) -> bool:
        """
        Verifica si la regla es aplicable al evento en el momento actual.
        
        Args:
            event: Evento a evaluar
            current_time: Momento actual para validar periodos de vigencia
            
        Returns:
            True si la regla es aplicable, False en caso contrario
        """
        if not self._check_validity_period(current_time):
            return False
        return self._evaluate_conditions(event)

    def _check_validity_period(self, current_time: datetime) -> bool:
        """
        Valida que la regla esté dentro de su periodo de vigencia.
        
        Args:
            current_time: Momento actual
            
        Returns:
            True si la regla está vigente, False en caso contrario
        """
        validity = self.config.get('validity_period', {})
        if not validity:
            return True

        start = validity.get('start_date')
        end = validity.get('end_date')

        if start and current_time < datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc):
            return False
        if end and current_time > datetime.strptime(end, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc):
            return False
        return True

    def _evaluate_conditions(self, event: Dict) -> bool:
        """
        Evalúa todas las condiciones de la regla.
        
        Args:
            event: Evento a evaluar
            
        Returns:
            True si todas las condiciones se cumplen, False si alguna falla
        """
        return all(self._check_condition(condition, event) for condition in self.conditions)

    def _check_condition(self, condition: Dict, event: Dict) -> bool:
        """
        Evalúa una condición individual.
        
        Args:
            condition: Condición a evaluar
            event: Evento a evaluar
            
        Returns:
            True si la condición se cumple, False en caso contrario
        """
        try:
            operator = condition['operator']
            field = condition['field']
            expected_value = condition.get('value')
            
            # Usar JMESPath para obtener el valor del campo
            actual_value = jmespath.search(field, event)
            
            # Definir operadores que funcionan con estructuras anidadas
            operators = {
                'exists': lambda: actual_value is not None,
                'matches_query': lambda: bool(actual_value),
                'equals': lambda: actual_value == expected_value,
                'not_equals': lambda: actual_value != expected_value,
                'in': lambda: actual_value in expected_value if expected_value else False,
                'contains': lambda: expected_value in actual_value if actual_value else False,
                'greater_than': lambda: actual_value > expected_value if actual_value is not None else False,
                'less_than': lambda: actual_value < expected_value if actual_value is not None else False
            }
            
            # Ejecutar el operador correspondiente
            operation = operators.get(operator, lambda: True)
            result = operation()
            
            return result
        except Exception as e:
            logger.error(f"Error al evaluar condición: {e}")
            return False

    def apply(self, event: Dict) -> Dict:
        """
        Aplica las acciones de la regla al evento.
        
        Args:
            event: Evento al que aplicar las acciones
            
        Returns:
            Diccionario con los resultados de las acciones
        """
        results = {}
        for action in self.actions:
            result = self._execute_action(action, event)
            
            if result:            
                results.update(result)
        
        return results

    def _execute_action(self, action: Dict, event: Dict) -> Optional[Dict]:
        """
        Ejecuta una acción individual.
        
        Args:
            action: Acción a ejecutar
            event: Evento sobre el que ejecutar la acción
            
        Returns:
            Resultado de la acción o None si no se pudo ejecutar
        """
        try:
            # Identificar tipo de acción
            action_type = action.get('calculate') or action.get('action')
            field = action['field']
            
            # Importar la fábrica de estrategias
            from obs_layer_sorsimple_mbaas.common.factories.strategy_factory import StrategyFactory
            
            # Crear estrategia usando la fábrica
            strategy = StrategyFactory.create_strategy(action_type)
            if not strategy:
                logger.warning(f"Tipo de acción no soportada: {action_type}")
                return None
                
            # Ejecutar estrategia
            return strategy.execute(action, event, field, self.extensions)
        except Exception as e:
            logger.error(f"Error al ejecutar acción: {e}")
            return None


class RuleEngine:
    """
    Motor para procesar reglas de negocio.
    """

    def __init__(self, rules_config: List[Dict], extensions: Optional[Dict[Any, str]] = None):
        """
        Inicializa el motor con configuración de reglas.
        
        Args:
            rules_config: Lista de configuraciones de reglas
            extensions: Datos de extensión opcionales
        """
        self.rules = [BusinessRule(rule, extensions) for rule in rules_config]

    def process_event(self, event: Dict, current_time: Optional[datetime] = None) -> Dict:
        """
        Procesa un evento aplicando las reglas configuradas.
        
        Args:
            event: Evento a procesar
            current_time: Momento actual (por defecto, ahora)
            
        Returns:
            Diccionario con los resultados de las reglas aplicadas
        """
        current_time = current_time or datetime.now(timezone.utc)
        results = {}

        # Ordenar reglas por prioridad (mayor primero)
        sorted_rules = sorted(
            self.rules, 
            key=lambda r: r.config.get('priority', 0), 
            reverse=True
        )

        # Aplicar reglas en orden
        for rule in sorted_rules:            
            if rule.is_applicable(event, current_time):
                rule_results = rule.apply(event)
                results.update(rule_results)

        return results
