"""interfaces/handlers/event_handler.py"""

import json
import os

from typing import Any, Dict

from obs_layer_sorsimple_mbaas.application.services.event_processor import EventProcessor
from obs_layer_sorsimple_mbaas.application.services.config_service import ConfigService
from obs_layer_sorsimple_mbaas.infrastructure.adapters.s3_config_adapter import S3ConfigAdapter
from obs_layer_sorsimple_mbaas.infrastructure.persistence.database_service import DatabaseService
from obs_layer_sorsimple_mbaas.common.factories.repository_factory import RepositoryFactory
from obs_layer_sorsimple_mbaas.common.utils.log import logger


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Manejador principal para la función Lambda.
    
    Este es el punto de entrada para procesar mensajes SQS en AWS Lambda.
    Coordina todo el flujo de procesamiento utilizando las distintas capas
    de la aplicación.
    
    Args:
        event: Evento Lambda que contiene el mensaje SQS
        context: Contexto de ejecución de Lambda
        
    Returns:
        Respuesta a AWS Lambda
    """
    try:
        logger.info("Iniciando procesamiento de evento")
        
        # Cargar configuración
        logger.info("Cargando configuración desde S3")
        config_adapter = S3ConfigAdapter(
            bucket_name=os.environ.get('BUCKET_NAME'),
            object_name=os.environ.get('OBJECT_NAME')
        )
        config_service = ConfigService(config_adapter)
        s3_config = config_service.get_config()
        
        # Crear servicios
        logger.info("Inicializando servicios")
        db_service = DatabaseService()
        repository = RepositoryFactory.create_repository(db_service)
        processor = EventProcessor(s3_config)
        
        # Procesar cada registro SQS
        logger.info(f"Procesando {len(event.get('Records', []))} registros")
        results = []
        
        for record in event.get('Records', []):
            # El cuerpo del mensaje SQS
            message = record.get('body', '{}')
            
            # Procesar y guardar la entidad
            logger.info(f"Procesando mensaje con ID: {record.get('messageId')}")
            entity = processor.process_and_save(message, repository)
            
            if entity:
                # Agregar resultado exitoso
                results.append({
                    'messageId': record.get('messageId'),
                    'status': 'success',
                    'sessionId': entity.session_id,
                    'entities': entity.entity_names
                })
            else:
                # Agregar resultado fallido
                results.append({
                    'messageId': record.get('messageId'),
                    'status': 'error',
                    'error': 'Failed to process message'
                })
        
        # Cerrar conexión a la base de datos
        db_service.close()
        
        # Retornar respuesta
        logger.info("Procesamiento completado con éxito")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Event processed successfully',
                'results': results
            })
        }
        
    except Exception as e:
        logger.error(f"Error en lambda_handler: {e}")
        
        # Retornar error
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error processing event',
                'error': str(e)
            })
        }
