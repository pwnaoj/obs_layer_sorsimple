"""infrastructure/adapters/s3_config_adapter.py"""

import boto3
import json
import os

from botocore.exceptions import ClientError
from typing import Dict, Any, Optional

from obs_layer_sorsimple_mbaas.common.utils.log import logger
from obs_layer_sorsimple_mbaas.common.exceptions.domain_exceptions import ConfigurationError


class S3ConfigAdapter:
    """
    Adaptador para acceder a configuraciones almacenadas en S3.
    
    Implementa el patrón Adapter para proporcionar una interfaz
    consistente sobre el servicio S3 de AWS para la gestión de configuraciones.
    """
    
    def __init__(self, bucket_name: Optional[str] = None, 
                object_name: Optional[str] = None,
                region_name: Optional[str] = None):
        """
        Inicializa el adaptador S3.
        
        Args:
            bucket_name: Nombre del bucket S3 (por defecto, usa variable de entorno)
            object_name: Nombre del objeto/archivo (por defecto, usa variable de entorno)
            region_name: Región de AWS (por defecto, usa configuración del entorno)
        """
        self.bucket_name = bucket_name or os.environ.get('BUCKET_NAME')
        self.object_name = object_name or os.environ.get('OBJECT_NAME')
        self.region_name = region_name
        self._client = None
        self._config_cache = None
        
    @property
    def client(self):
        """
        Obtiene el cliente S3, creándolo si no existe.
        
        Returns:
            Cliente boto3 para S3
        """
        if self._client is None:
            self._client = boto3.client('s3', region_name=self.region_name)
        return self._client
    
    def get_config(self, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Obtiene la configuración desde S3.
        
        Args:
            force_refresh: Si es True, fuerza una recarga desde S3
            
        Returns:
            Configuración como diccionario
            
        Raises:
            ConfigurationError: Si ocurre un error en la operación
        """
        # Si hay caché y no se fuerza recarga, usar caché
        if self._config_cache is not None and not force_refresh:
            return self._config_cache
            
        try:
            # Validar parámetros
            if not self.bucket_name or not self.object_name:
                raise ConfigurationError(
                    "Configuración de S3 incompleta", 
                    {"bucket": self.bucket_name, "object": self.object_name}
                )
                
            # Obtener objeto de S3
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=self.object_name
            )
            
            # Leer y parsear contenido
            file_content = response['Body'].read().decode('utf-8')
            config = json.loads(file_content)
            
            # Actualizar caché
            self._config_cache = config
            
            return config
        except ClientError as e:
            error_msg = f"Error al obtener configuración de S3: {e}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg, {
                "bucket": self.bucket_name, 
                "object": self.object_name
            })
        except json.JSONDecodeError as e:
            error_msg = f"Error al decodificar configuración JSON: {e}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg, {
                "bucket": self.bucket_name, 
                "object": self.object_name
            })
