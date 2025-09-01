"""common/exceptions/domain_exceptions.py"""

from typing import Any, Dict, Optional


class BaseException(Exception):
    """
    Excepción base para todas las excepciones de la aplicación.
    """
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """
        Inicializa una nueva excepción.
        
        Args:
            message: Mensaje descriptivo del error
            details: Detalles adicionales sobre el error
        """
        self.message = message
        self.details = details or {}
        super().__init__(self.message)
        
    def __str__(self) -> str:
        """
        Representación en string de la excepción.
        
        Returns:
            Mensaje descriptivo con detalles si los hay
        """
        if self.details:
            return f"{self.message} - Detalles: {self.details}"
        return self.message


class ValidationError(BaseException):
    """
    Excepción para errores de validación de datos.
    """
    pass


class ConfigurationError(BaseException):
    """
    Excepción para errores de configuración.
    """
    pass


class RepositoryError(BaseException):
    """
    Excepción para errores de acceso a repositorios.
    """
    pass


class ProcessingError(BaseException):
    """
    Excepción para errores durante el procesamiento de eventos.
    """
    pass


class EntityBuilderError(BaseException):
    """
    Excepción para errores durante la construcción de entidades.
    """
    pass


class RuleEngineError(BaseException):
    """
    Excepción para errores en el motor de reglas.
    """
    pass


class StrategyExecutionError(BaseException):
    """
    Excepción para errores en la ejecución de estrategias.
    """
    pass


class ConsumerNotFoundError(BaseException):
    """
    Excepción para cuando no se encuentra un consumidor.
    """
    def __init__(self, consumer_id: str, message: Optional[str] = None):
        """
        Inicializa una nueva excepción.
        
        Args:
            consumer_id: ID del consumidor no encontrado
            message: Mensaje descriptivo opcional
        """
        details = {"consumer_id": consumer_id}
        super().__init__(
            message or f"Consumidor '{consumer_id}' no encontrado",
            details
        )


class ServiceNotFoundError(BaseException):
    """
    Excepción para cuando no se encuentra un servicio.
    """
    def __init__(self, service_id: str, consumer_id: str, message: Optional[str] = None):
        """
        Inicializa una nueva excepción.
        
        Args:
            service_id: ID del servicio no encontrado
            consumer_id: ID del consumidor
            message: Mensaje descriptivo opcional
        """
        details = {"service_id": service_id, "consumer_id": consumer_id}
        super().__init__(
            message or f"Servicio '{service_id}' no encontrado para el consumidor '{consumer_id}'",
            details
        )


class MissingDataError(BaseException):
    """
    Excepción para cuando faltan datos requeridos.
    """
    def __init__(self, missing_fields: list, message: Optional[str] = None):
        """
        Inicializa una nueva excepción.
        
        Args:
            missing_fields: Lista de campos faltantes
            message: Mensaje descriptivo opcional
        """
        details = {"missing_fields": missing_fields}
        super().__init__(
            message or f"Faltan campos requeridos: {', '.join(missing_fields)}",
            details
        )
