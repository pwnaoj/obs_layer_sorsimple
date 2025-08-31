"""interfaces/models/sqs_models.py"""

from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any


class Attributes(BaseModel):
    """Atributos de un mensaje SQS."""
    ApproximateReceiveCount: str
    SentTimestamp: str
    SenderId: str
    ApproximateFirstReceiveTimestamp: str
    
    # Atributos opcionales (pueden no estar presentes)
    AWSTraceHeader: Optional[str] = None
    SequenceNumber: Optional[str] = None
    MessageGroupId: Optional[str] = None
    MessageDeduplicationId: Optional[str] = None


class MessageAttribute(BaseModel):
    """Estructura de un atributo de mensaje."""
    stringValue: Optional[str] = None
    binaryValue: Optional[str] = None
    stringListValues: List[str] = Field(default_factory=list)
    binaryListValues: List[str] = Field(default_factory=list)
    dataType: str


class Record(BaseModel):
    """Registro individual de un mensaje SQS."""
    messageId: str
    receiptHandle: str
    body: str
    attributes: Attributes
    messageAttributes: Dict[str, MessageAttribute] = Field(default_factory=dict)
    md5OfBody: str
    eventSource: str
    eventSourceARN: str
    awsRegion: str


class SQSEvent(BaseModel):
    """Evento completo de SQS que contiene múltiples registros."""
    Records: List[Record]


class EventResponse(BaseModel):
    """Respuesta a un evento SQS procesado."""
    messageId: str
    status: str
    sessionId: Optional[str] = None
    entities: Optional[List[str]] = None
    error: Optional[str] = None


class LambdaResponse(BaseModel):
    """Respuesta completa de la función Lambda."""
    statusCode: int
    body: Dict[str, Any]
