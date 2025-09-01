"""common/factories/repository_factory.py"""

from typing import Optional, Type

from obs_layer_sorsimple_mbaas.common.utils.log import logger
from obs_layer_sorsimple_mbaas.common.exceptions.domain_exceptions import ConfigurationError


class RepositoryFactory:
    """
    Fábrica para crear repositorios siguiendo el patrón Factory Method.
    """
    
    _repositories = {}
    
    @classmethod
    def register(cls, repository_type: str, repository_class: Type):
        """
        Registra un tipo de repositorio en la fábrica.
        
        Args:
            repository_type: Tipo de repositorio a registrar
            repository_class: Clase del repositorio
        """
        cls._repositories[repository_type.lower()] = repository_class
        
    @classmethod
    def create_repository(cls, db_service, repository_type: str = 'entity'):
        """
        Crea un repositorio del tipo especificado.
        
        Args:
            db_service: Servicio de base de datos a usar
            repository_type: Tipo de repositorio a crear
            
        Returns:
            Instancia del repositorio creado
            
        Raises:
            ConfigurationError: Si el tipo de repositorio no está registrado
        """
        # Importación tardía para evitar dependencias circulares
        from ...infrastructure.repositories.entity_repository import EntityRepository
        
        # Registrar repositorios por defecto si no existen
        if not cls._repositories:
            cls._repositories = {
                'entity': EntityRepository,
            }
            
        # Crear y retornar el repositorio
        repository_class = cls._repositories.get(repository_type.lower())
        if not repository_class:
            error_msg = f"Tipo de repositorio no soportado: {repository_type}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg)
            
        try:
            return repository_class(db_service)
        except Exception as e:
            error_msg = f"Error al crear repositorio {repository_type}: {e}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg, {"original_error": str(e)})
