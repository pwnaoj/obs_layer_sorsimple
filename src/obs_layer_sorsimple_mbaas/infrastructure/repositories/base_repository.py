"""infrastructure/repositories/base_repository.py"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from obs_layer_sorsimple_mbaas.common.utils.log import logger
from obs_layer_sorsimple_mbaas.common.exceptions.domain_exceptions import RepositoryError


class BaseRepository(ABC):
    """
    Clase base abstracta para todos los repositorios.
    
    Define la interfaz que deben implementar todos los repositorios
    siguiendo el patrón Repository.
    """
    
    @abstractmethod
    def find(self, *args, **kwargs) -> Any:
        """
        Busca y recupera datos del almacenamiento.
        
        Args:
            *args: Argumentos posicionales para la búsqueda
            **kwargs: Argumentos de palabra clave para la búsqueda
            
        Returns:
            Datos recuperados
            
        Raises:
            RepositoryError: Si ocurre un error en la operación
        """
        pass
    
    @abstractmethod
    def save(self, *args, **kwargs) -> bool:
        """
        Guarda datos en el almacenamiento.
        
        Args:
            *args: Argumentos posicionales para el guardado
            **kwargs: Argumentos de palabra clave para el guardado
            
        Returns:
            True si la operación fue exitosa, False en caso contrario
            
        Raises:
            RepositoryError: Si ocurre un error en la operación
        """
        pass
    
    def _handle_error(self, operation: str, error: Exception, **context) -> None:
        """
        Maneja errores de repositorio de manera consistente.
        
        Args:
            operation: Nombre de la operación que falló
            error: Excepción original
            **context: Contexto adicional de la operación
            
        Raises:
            RepositoryError: Excepción enriquecida con contexto
        """
        error_msg = f"Error en operación de repositorio '{operation}': {error}"
        logger.error(error_msg)
        
        # Agregar información de contexto
        details = {
            "original_error": str(error),
            "error_type": error.__class__.__name__
        }
        details.update(context)
        
        raise RepositoryError(error_msg, details)
