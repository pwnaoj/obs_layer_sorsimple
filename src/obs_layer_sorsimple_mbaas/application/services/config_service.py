"""application/services/config_services.py"""

from typing import Any, Dict, Optional

from obs_layer_sorsimple_mbaas.common.utils.log import logger


class ConfigService:
    """
    Servicio para acceder y gestionar la configuración de la aplicación.
    """
    
    def __init__(self, config_adapter):
        """
        Inicializa el servicio con un adaptador de configuración.
        
        Args:
            config_adapter: Adaptador para obtener la configuración
        """
        self.config_adapter = config_adapter
        self._config_cache = None
        
    def get_config(self, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Obtiene la configuración actual.
        
        Args:
            force_refresh: Si es True, fuerza una recarga desde la fuente
            
        Returns:
            Configuración actual
            
        Raises:
            ValueError: Si no se puede obtener la configuración
        """
        try:
            # Si se solicita recarga o no hay caché, obtener de la fuente
            if force_refresh or self._config_cache is None:
                self._config_cache = self.config_adapter.get_config()
                
            return self._config_cache
        except Exception as e:
            logger.error(f"Error al obtener configuración: {e}")
            raise ValueError(f"No se pudo obtener la configuración: {e}")
            
    def get_service_config(self, consumer_id: str, service_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene la configuración específica para un servicio.
        
        Args:
            consumer_id: ID del consumidor de la aplicación
            service_id: ID del servicio
            
        Returns:
            Configuración del servicio o None si no se encuentra
        """
        try:
            config = self.get_config()
            
            # Buscar configuración del consumidor
            consumer_config = next((c for c in config if c.get('id') == consumer_id), None)
            if not consumer_config:
                logger.warning(f"No se encontró configuración para consumer_id={consumer_id}")
                return None
                
            # Buscar configuración del servicio
            service_config = next(
                (s for s in consumer_config.get('services', []) if s.get('id_service') == service_id), 
                None
            )
            
            return service_config
        except Exception as e:
            logger.error(f"Error al obtener configuración de servicio: {e}")
            return None
            
    def get_rules_for_service(self, consumer_id: str, service_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene las reglas aplicables a un servicio.
        
        Args:
            consumer_id: ID del consumidor de la aplicación
            service_id: ID del servicio
            
        Returns:
            Reglas aplicables o None si no se encuentran
        """
        try:
            config = self.get_config()
            
            # Buscar configuración del consumidor
            consumer_config = next((c for c in config if c.get('id') == consumer_id), None)
            if not consumer_config:
                logger.warning(f"No se encontró configuración para consumer_id={consumer_id}")
                return None
                
            # Buscar reglas aplicables
            rules = [r for r in consumer_config.get('rules', []) if r.get('event_type') == service_id]
            
            return rules or None
        except Exception as e:
            logger.error(f"Error al obtener reglas de servicio: {e}")
            return None
