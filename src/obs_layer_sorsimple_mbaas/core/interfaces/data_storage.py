"""core/interfaces/data_storage.py"""

from abc import ABC, abstractmethod
from typing import Dict


class DatabaseService(ABC):
    """Clase base para operaciones de Base de Datos."""
    
    @abstractmethod
    def get_events(self, **kwargs) -> Dict:
        """Obtener eventos de la Base de Datos."""
        pass

    @abstractmethod
    def save_events(self, **kwargs) -> None:
        """Guardar eventos en la Base de Datos."""
        pass

    @abstractmethod
    def delete_events(self, **kwargs) -> None:
        """Eliminar eventos en la Base de Datos."""
        pass
