"""common/factories/query_builder_factory.py"""

from typing import Optional, Type

from obs_layer_sorsimple_mbaas.domain.interfaces.query_builder import QueryBuilder
from obs_layer_sorsimple_mbaas.common.utils.log import logger
from obs_layer_sorsimple_mbaas.common.exceptions.domain_exceptions import ConfigurationError


class QueryBuilderFactory:
    """
    Fábrica para crear query builders específicos según el tipo de consulta.
    """
    
    _builders = {}
    
    @classmethod
    def register(cls, query_type: str, builder_class: Type[QueryBuilder]):
        """
        Registra un tipo de builder en la fábrica.
        
        Args:
            query_type: Tipo de consulta (save, find, find_tidnid)
            builder_class: Clase del builder
        """
        cls._builders[query_type.lower()] = builder_class
    
    @classmethod
    def create_builder(cls, query_type: str) -> QueryBuilder:
        """
        Crea un builder basado en el tipo de consulta.
        
        Args:
            query_type: Tipo de consulta a construir
            
        Returns:
            Instancia del builder apropiado
            
        Raises:
            ConfigurationError: Si el tipo de builder no está registrado
        """
        # Importación tardía para evitar dependencias circulares
        from obs_layer_sorsimple_mbaas.infrastructure.query_builders.save_query_builder import SaveQueryBuilder
        from obs_layer_sorsimple_mbaas.infrastructure.query_builders.find_query_builder import FindQueryBuilder
        
        # Registrar builders por defecto si no existen
        if not cls._builders:
            cls._builders = {
                'save': SaveQueryBuilder,
                'find': FindQueryBuilder,
                'find_tidnid': FindQueryBuilder,  # Reutiliza FindQueryBuilder
            }
        
        # Crear y retornar el builder
        builder_class = cls._builders.get(query_type.lower())
        if not builder_class:
            error_msg = f"Tipo de query builder no soportado: {query_type}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg)
        
        try:
            return builder_class()
        except Exception as e:
            error_msg = f"Error al crear query builder {query_type}: {e}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg, {"original_error": str(e)})
