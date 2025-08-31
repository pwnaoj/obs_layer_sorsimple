"""interfaces/handlers/health_handler.py"""

import json

from typing import Any, Dict

from obs_layer_sorsimple_mbaas.infrastructure.persistence.database_service import DatabaseService
from obs_layer_sorsimple_mbaas.infrastructure.adapters.s3_config_adapter import S3ConfigAdapter
from obs_layer_sorsimple_mbaas.common.utils.log import logger


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Manejador para verificar la salud del sistema.
    
    Este handler comprueba la conectividad con los servicios esenciales
    como la base de datos y S3.
    
    Args:
        event: Evento Lambda
        context: Contexto de ejecución de Lambda
        
    Returns:
        Respuesta de estado de salud
    """
    health_status = {
        'status': 'ok',
        'services': {}
    }
    
    # Verificar conexión a la base de datos
    try:
        db_service = DatabaseService()
        db_connection = db_service._get_connection()
        cursor = db_connection.cursor()
        cursor.execute('SELECT 1')
        cursor.close()
        db_service.close()
        
        health_status['services']['database'] = {
            'status': 'ok',
            'message': 'Database connection successful'
        }
    except Exception as e:
        logger.error(f"Error de conexión a la base de datos: {e}")
        health_status['status'] = 'error'
        health_status['services']['database'] = {
            'status': 'error',
            'message': str(e)
        }
    
    # Verificar acceso a S3
    try:
        s3_adapter = S3ConfigAdapter()
        config = s3_adapter.get_config()
        
        health_status['services']['s3'] = {
            'status': 'ok',
            'message': 'S3 configuration accessible',
            'config_size': len(str(config))
        }
    except Exception as e:
        logger.error(f"Error de acceso a S3: {e}")
        health_status['status'] = 'error'
        health_status['services']['s3'] = {
            'status': 'error',
            'message': str(e)
        }
    
    # Determinar código de estado HTTP
    status_code = 200 if health_status['status'] == 'ok' else 500
    
    return {
        'statusCode': status_code,
        'body': json.dumps(health_status)
    }
