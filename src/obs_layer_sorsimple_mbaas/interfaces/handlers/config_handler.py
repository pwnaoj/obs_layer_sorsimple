"""interfaces/handlers/config_handler.py"""

import json

from typing import Any, Dict

from obs_layer_sorsimple_mbaas.application.services.config_service import ConfigService
from obs_layer_sorsimple_mbaas.infrastructure.adapters.s3_config_adapter import S3ConfigAdapter
from obs_layer_sorsimple_mbaas.common.utils.log import logger


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Manejador para gestionar la configuración del sistema.
    
    Permite obtener y refrescar la configuración almacenada en S3.
    
    Args:
        event: Evento Lambda con el comando a ejecutar
        context: Contexto de ejecución de Lambda
        
    Returns:
        Respuesta con el resultado de la operación
    """
    try:
        # Inicializar servicios
        config_adapter = S3ConfigAdapter()
        config_service = ConfigService(config_adapter)
        
        # Obtener comando a ejecutar
        command = event.get('command', 'get_config')
        
        # Ejecutar comando
        if command == 'get_config':
            # Obtener configuración actual
            config = config_service.get_config()
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Configuration retrieved successfully',
                    'config': config
                })
            }
            
        elif command == 'refresh_config':
            # Forzar recarga de la configuración
            config = config_service.get_config(force_refresh=True)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Configuration refreshed successfully',
                    'config': config
                })
            }
            
        elif command == 'get_service_config':
            # Obtener configuración de un servicio específico
            consumer_id = event.get('consumer_id')
            service_id = event.get('service_id')
            
            if not consumer_id or not service_id:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'message': 'Missing required parameters',
                        'error': 'Both consumer_id and service_id are required'
                    })
                }
                
            service_config = config_service.get_service_config(consumer_id, service_id)
            
            if service_config:
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'Service configuration retrieved successfully',
                        'config': service_config
                    })
                }
            else:
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        'message': 'Service configuration not found',
                        'error': f"No configuration found for consumer_id={consumer_id}, service_id={service_id}"
                    })
                }
                
        else:
            # Comando desconocido
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Invalid command',
                    'error': f"Unknown command: {command}"
                })
            }
            
    except Exception as e:
        logger.error(f"Error en config_handler: {e}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error processing request',
                'error': str(e)
            })
        }
