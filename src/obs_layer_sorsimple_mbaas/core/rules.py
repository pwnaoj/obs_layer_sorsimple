"""src/core/rules.py"""

import jmespath

from datetime import datetime, timezone
from flatten_json import unflatten
from typing import Any, Dict, List, Optional, Union


class BusinessRule:
    """Business rule processor."""

    def __init__(self, rule_config: Dict, extensions: Optional[Dict[str, Any]] = None):
        """Initialize rule with configuration."""
        self.config = rule_config
        self.extensions = extensions
        self.conditions = rule_config.get('conditions', [])
        self.actions = rule_config.get('actions', [])

    def is_applicable(self, event: Dict, current_time: datetime) -> bool:
        """Check if rule is currently applicable."""
        if not self._check_validity_period(current_time):
            return False
        return self._evaluate_conditions(event)

    def _check_validity_period(self, current_time: datetime) -> bool:
        """Validate rule's time period."""
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
        """Evaluate all rule conditions."""
        return all(self._check_condition(condition, event) for condition in self.conditions)

    def _check_condition(self, condition: Dict, event: Dict) -> bool:
        """Evaluate a single condition."""
        operator = condition['operator']
        field = condition['field']
        value = condition.get('value')

        operators = {
            'exists': lambda: field in event,
            'matches_query': lambda: bool(jmespath.search(field, unflatten(event, '.'))),
            'equals': lambda: event.get(field) == value,
            'not_equals': lambda: event.get(field) != value,
            'in': lambda: event.get(field) in value
        }

        return operators.get(operator, lambda: True)()

    def apply(self, event: Dict) -> Dict:
        """Apply rule actions to event."""
        results = {}
        for action in self.actions:
            result = self._execute_action(action, event)
            if result:
                results.update(result)
        return results

    def _execute_action(self, action: Dict, event: Dict) -> Optional[Dict]:
        """Execute a single action."""
        action_type = action.get('calculate') or action.get('action')
        field = action['field']

        actions = {
            'time_difference': lambda: self._calculate_time_diff(action, event, field),
            'set_value': lambda: {field: event.get(action['value'])},
            'set_fixed_value': lambda: {field: action['value']},
            'extract_value': lambda: self._extract_value(action['query'], event, field),
            'sum_numeric_string': lambda: self._sum_numeric_string(action, event, field)
        }

        return actions.get(action_type, lambda: None)()

    def _calculate_time_diff(self, action: Dict, event: Dict, field: str) -> Optional[Dict]:
        """Calculate time difference between two timestamps."""
        params = action['params']
        start = event.get(params['start_time'].strip('$'))
        end = event.get(params['end_time'].strip('$'))

        if not (start and end):
            return None

        fmt = '%Y-%m-%dT%H:%M:%SZ'
        delta = (datetime.strptime(end, fmt).replace(tzinfo=timezone.utc) - 
                datetime.strptime(start, fmt).replace(tzinfo=timezone.utc))
        return {field: delta.total_seconds()}
    
    def _extract_value(self, jmespath_query: str, event: Dict, field: str):
        """Execute jmespath query."""
        require_ext = jmespath.search('[].require_ext | [0]', self.conditions)
        name_ext = jmespath.search('[].name_ext | [0]', self.actions) if require_ext.lower() == 'true' else None
        condition_field = jmespath.search('[].field | [0]', self.conditions)
        value = jmespath.search(condition_field, unflatten(event, '.'))
        result = jmespath.search(jmespath_query.format(value), self.extensions.get(name_ext, []) if name_ext else [])
        
        return {field: result}

class RuleEngine:
    """Engine for processing business rules."""

    def __init__(self, rules_config: List[Dict], extenstions: Optional[Dict[Any, str]] = None):
        """Initialize engine with rules configuration."""
        self.rules = [BusinessRule(rule, extenstions) for rule in rules_config]

    def process_event(self, event: Dict, current_time: Optional[datetime] = None) -> Dict:
        """Process event through applicable rules."""
        current_time = current_time or datetime.now(timezone.utc)
        results = {}

        for rule in sorted(self.rules, key=lambda r: r.config.get('priority', 0), reverse=True):
            if rule.is_applicable(event, current_time):
                results.update(rule.apply(event))

        return results
