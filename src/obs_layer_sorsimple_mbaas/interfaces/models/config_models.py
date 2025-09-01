"""interfaces/models/config_models.py"""

from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any, Union


class Path(BaseModel):
    """Ruta JMESPath para extraer datos."""
    path: str
    enabled: str = "true"


class Entity(BaseModel):
    """Entidad asociada a un servicio."""
    name: str
    table: Optional[str] = None


class ServiceConfig(BaseModel):
    """Configuración de un servicio."""
    id_service: str
    paths: List[List[str]] = Field(default_factory=list)
    entity: List[str] = Field(default_factory=list)


class ActionParam(BaseModel):
    """Parámetro para una acción de regla."""
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    field_name: Optional[str] = None


class Condition(BaseModel):
    """Condición para una regla."""
    operator: str
    field: str
    value: Optional[Any] = None
    require_ext: Optional[str] = None
    name_ext: Optional[str] = None


class Action(BaseModel):
    """Acción para una regla."""
    field: str
    action: Optional[str] = None
    calculate: Optional[str] = None
    value: Optional[Any] = None
    query: Optional[str] = None
    fields: Optional[List[str]] = None
    params: Optional[ActionParam] = None
    conditions: Optional[List[Condition]] = None
    require_ext: Optional[str] = None
    name_ext: Optional[str] = None


class ValidityPeriod(BaseModel):
    """Período de validez para una regla."""
    start_date: Optional[str] = None
    end_date: Optional[str] = None


class Rule(BaseModel):
    """Regla de negocio."""
    id_rule: str
    event_type: str
    priority: int = 0
    conditions: List[Condition] = Field(default_factory=list)
    actions: List[Action] = Field(default_factory=list)
    validity_period: Optional[ValidityPeriod] = None


class ConsumerConfig(BaseModel):
    """Configuración completa de un consumidor."""
    id: str
    services: List[ServiceConfig] = Field(default_factory=list)
    rules: List[Rule] = Field(default_factory=list)


class SystemConfig(BaseModel):
    """Configuración completa del sistema."""
    consumers: List[ConsumerConfig] = Field(default_factory=list)
    extensions: Dict[str, Any] = Field(default_factory=dict)
    version: str
