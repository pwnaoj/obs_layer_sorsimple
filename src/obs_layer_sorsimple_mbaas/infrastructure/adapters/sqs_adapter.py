"""infrastructure/adapters/sqs_adapter.py"""

import boto3
import json

from botocore.exceptions import ClientError
from typing import Any, Dict, List, Optional, Union

from obs_layer_sorsimple_mbaas.common.utils.log import logger
from obs_layer_sorsimple_mbaas.common.exceptions.domain_exceptions import BaseException


class SQSAdapterError(BaseException):
    """
    Excepción específica para errores del adaptador SQS.
    """
    pass


class SQSAdapter:
    """
    Adaptador para interactuar con Amazon SQS.
    
    Implementa el patrón Adapter para proporcionar una interfaz
    consistente sobre el servicio SQS de AWS.
    """
    
    def __init__(self, region_name: Optional[str] = None):
        """
        Inicializa el adaptador SQS.
        
        Args:
            region_name: Región de AWS (por defecto, usa configuración del entorno)
        """
        self.region_name = region_name
        self._client = None
        
    @property
    def client(self):
        """
        Obtiene el cliente SQS, creándolo si no existe.
        
        Returns:
            Cliente boto3 para SQS
        """
        if self._client is None:
            self._client = boto3.client('sqs', region_name=self.region_name)
        return self._client
    
    def receive_message(self, queue_url: str, max_messages: int = 1, 
                        wait_time: int = 0, visibility_timeout: int = 30) -> List[Dict]:
        """
        Recibe mensajes de una cola SQS.
        
        Args:
            queue_url: URL de la cola SQS
            max_messages: Número máximo de mensajes a recibir (1-10)
            wait_time: Tiempo de espera en segundos (0-20)
            visibility_timeout: Tiempo de visibilidad en segundos
            
        Returns:
            Lista de mensajes recibidos
            
        Raises:
            SQSAdapterError: Si ocurre un error en la operación
        """
        try:
            response = self.client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time,
                VisibilityTimeout=visibility_timeout,
                AttributeNames=['All'],
                MessageAttributeNames=['All']
            )
            
            return response.get('Messages', [])
        except ClientError as e:
            error_msg = f"Error al recibir mensajes de SQS: {e}"
            logger.error(error_msg)
            raise SQSAdapterError(error_msg, {"queue_url": queue_url})
    
    def send_message(self, queue_url: str, message_body: Union[str, Dict], 
                     message_group_id: Optional[str] = None,
                     message_deduplication_id: Optional[str] = None) -> str:
        """
        Envía un mensaje a una cola SQS.
        
        Args:
            queue_url: URL de la cola SQS
            message_body: Cuerpo del mensaje (string o diccionario)
            message_group_id: ID de grupo para colas FIFO
            message_deduplication_id: ID de deduplicación para colas FIFO
            
        Returns:
            ID del mensaje enviado
            
        Raises:
            SQSAdapterError: Si ocurre un error en la operación
        """
        try:
            # Convertir a string si es diccionario
            if isinstance(message_body, dict):
                message_body = json.dumps(message_body)
                
            # Preparar parámetros
            params = {
                'QueueUrl': queue_url,
                'MessageBody': message_body
            }
            
            # Agregar parámetros FIFO si es necesario
            if message_group_id:
                params['MessageGroupId'] = message_group_id
            if message_deduplication_id:
                params['MessageDeduplicationId'] = message_deduplication_id
                
            # Enviar mensaje
            response = self.client.send_message(**params)
            
            return response['MessageId']
        except ClientError as e:
            error_msg = f"Error al enviar mensaje a SQS: {e}"
            logger.error(error_msg)
            raise SQSAdapterError(error_msg, {"queue_url": queue_url})
    
    def delete_message(self, queue_url: str, receipt_handle: str) -> bool:
        """
        Elimina un mensaje de una cola SQS.
        
        Args:
            queue_url: URL de la cola SQS
            receipt_handle: Identificador de recibo del mensaje
            
        Returns:
            True si se eliminó correctamente
            
        Raises:
            SQSAdapterError: Si ocurre un error en la operación
        """
        try:
            self.client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            return True
        except ClientError as e:
            error_msg = f"Error al eliminar mensaje de SQS: {e}"
            logger.error(error_msg)
            raise SQSAdapterError(error_msg, {
                "queue_url": queue_url, 
                "receipt_handle": receipt_handle
            })
