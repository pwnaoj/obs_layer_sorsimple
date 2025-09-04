"""core/interfaces/message_processor.py"""

from abc import ABC, abstractmethod
from typing import Dict, Optional


class MessageProcessor(ABC):
    """Interface base para procesadores de mensajes."""
    
    @abstractmethod
    def process(self) -> Dict:
        """Procesamiento de mensajes."""
        pass
