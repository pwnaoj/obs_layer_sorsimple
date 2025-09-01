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
    
    @staticmethod
    def sqs_record_to_dict(record):
        """
        Convierte un objeto SQSRecord de AWS Lambda Powertools a un diccionario
        para validación con Pydantic.
        """
        # Extraer atributos con manejo de posibles excepciones
        try:
            attributes = {k: v for k, v in record.attributes.items()} if hasattr(record, 'attributes') else {}
        except Exception:
            attributes = {}
    
        try:
            message_attributes = record.message_attributes if hasattr(record, 'message_attributes') else {}
        except Exception:
            message_attributes = {}
    
        # Crear diccionario con los campos necesarios
        return {
            "messageId": getattr(record, 'message_id', ''),
            "receiptHandle": getattr(record, 'receipt_handle', ''),
            "body": getattr(record, 'body', str(record)),
            "attributes": attributes,
            "messageAttributes": message_attributes,
            "md5OfBody": getattr(record, 'md5_of_body', ''),
            "eventSource": getattr(record, 'event_source', ''),
            "eventSourceARN": getattr(record, 'event_source_arn', ''),
            "awsRegion": getattr(record, 'aws_region', '')
        }
    
    @classmethod
    def from_powertools_record(cls, record):
        """
        Crea una instancia de Record a partir de un objeto SQSEvent de Powertools.
        """
        record_dict = cls.sqs_record_to_dict(record)
        return cls.model_validate(record_dict)

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
