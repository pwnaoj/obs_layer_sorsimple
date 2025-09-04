"""application/services/sql_query_service.py"""

from typing import Dict, Tuple

from obs_layer_sorsimple_mbaas.common.value_objects.parameter_context import ParameterContext
from obs_layer_sorsimple_mbaas.common.factories.query_builder_factory import QueryBuilderFactory
from obs_layer_sorsimple_mbaas.common.utils.log import logger
from obs_layer_sorsimple_mbaas.common.exceptions.domain_exceptions import ConfigurationError


class SQLQueryService:
    """
    Servicio orquestador para construcción de consultas SQL dinámicas.
    Reemplaza la funcionalidad de SQLBuilder con arquitectura desacoplada.
    """
    
    def __init__(self, db_config: Dict):
        """
        Inicializa el servicio con configuración de base de datos.
        
        Args:
            db_config: Configuración de consultas y parámetros desde S3
        """
        self.db_config = db_config
        self.query_factory = QueryBuilderFactory()
    
    def build_query_and_params(self, query_type: str, context: ParameterContext) -> Tuple[str, Tuple]:
        """
        Construye consulta SQL completa con parámetros extraídos.
        
        Args:
            query_type: Tipo de consulta (save, find, find_tidnid)
            context: Contexto con evento, entidad y configuración
            
        Returns:
            Tupla con (consulta_sql, parametros_tupla)
            
        Raises:
            ConfigurationError: Si la configuración es inválida
        """
        try:
            # Validar configuración
            self._validate_query_config(query_type, context)
            
            # Obtener configuración específica del query
            query_config = self.db_config['querys'][query_type]
            
            # Crear builder apropiado
            builder = self.query_factory.create_builder(query_type)
            
            # Construir consulta y parámetros
            query, params = builder.build_query(query_config, context)
            
            # Log para debugging
            logger.debug(f"Query construido - Tipo: {query_type}")
            logger.debug(f"SQL: {query}")
            logger.debug(f"Params: {params}")
            
            return query, params
            
        except Exception as e:
            error_msg = f"Error construyendo consulta {query_type}: {e}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg)
    
    def _validate_query_config(self, query_type: str, context: ParameterContext):
        """
        Valida que existe configuración válida para el tipo de consulta.
        
        Args:
            query_type: Tipo de consulta a validar
            context: Contexto para extracción de app_consumer_id
            
        Raises:
            ConfigurationError: Si la configuración es inválida
        """
        app_consumer_id = context.get_app_consumer_id()
        
        # Validar que existe configuración de DB
        if not self.db_config:
            raise ConfigurationError(
                f"No existe configuración de DB para appConsumer='{app_consumer_id}'"
            )
        
        # Validar que existe el query solicitado
        querys = self.db_config.get('querys', {})
        if query_type not in querys:
            raise ConfigurationError(
                f"No existe consulta '{query_type}' para appConsumer='{app_consumer_id}'"
            )
        
        # Validar estructura del query
        query_config = querys[query_type]
        if not query_config.get('query'):
            raise ConfigurationError(
                f"Query '{query_type}' no tiene definición SQL para appConsumer='{app_consumer_id}'"
            )
        
        if not query_config.get('params'):
            raise ConfigurationError(
                f"Query '{query_type}' no tiene parámetros configurados para appConsumer='{app_consumer_id}'"
            )
    
    def get_available_query_types(self) -> list:
        """
        Retorna los tipos de consultas disponibles en la configuración.
        
        Returns:
            Lista de tipos de consultas disponibles
        """
        return list(self.db_config.get('querys', {}).keys())
    
    def has_query_type(self, query_type: str) -> bool:
        """
        Verifica si existe configuración para el tipo de consulta.
        
        Args:
            query_type: Tipo de consulta a verificar
            
        Returns:
            True si existe configuración, False caso contrario
        """
        return query_type in self.db_config.get('querys', {})
