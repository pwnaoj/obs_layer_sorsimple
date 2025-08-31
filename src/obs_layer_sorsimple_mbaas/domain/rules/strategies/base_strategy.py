"""domain/rules/strategies/base_strategy.py"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class ActionStrategy(ABC):
    """
    Interfaz base para estrategias de acciones de reglas de negocio.
    
    Define el contrato que todas las estrategias concretas deben implementar.
    Aplica el patrón Strategy para hacer intercambiables diferentes algoritmos
    de procesamiento de acciones.
    """
    
    @abstractmethod
    def execute(self, action: Dict[str, Any], event: Dict[str, Any], 
                field: str, extensions: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Ejecuta la acción específica.
        
        Args:
            action: Configuración de la acción a ejecutar
            event: Evento sobre el que se aplica la acción
            field: Campo donde se almacenará el resultado
            extensions: Datos de extensión opcionales
            
        Returns:
            Diccionario con el resultado de la acción
        """
        pass
