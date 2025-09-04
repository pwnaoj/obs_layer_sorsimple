"""domain/interfaces/query_builder.py"""

from abc import ABC, abstractmethod
from typing import Dict, Tuple

from obs_layer_sorsimple_mbaas.common.value_objects.parameter_context import ParameterContext


class QueryBuilder(ABC):
    """
    Interfaz base para constructores de consultas SQL.
    """
    
    @abstractmethod
    def build_query(self, query_config: Dict, context: ParameterContext) -> Tuple[str, Tuple]:
        """
        Construye una consulta SQL con sus parámetros.
        
        Args:
            query_config: Configuración de la consulta (query + params)
            context: Contexto con datos para extracción
            
        Returns:
            Tupla con (query_formateado, parametros_ordenados)
        """
        pass
