"""common/utils/log.py"""

import logging
import os

from typing import Optional


# Configuración del logger
def setup_logger(log_level: Optional[str] = None) -> logging.Logger:
    """
    Configura y devuelve un logger para la aplicación.
    
    Args:
        log_level: Nivel de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
    Returns:
        Logger configurado
    """
    # Determinar nivel de log
    level = log_level or os.environ.get('LOG_LEVEL', 'INFO')
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Crear logger
    logger = logging.getLogger("obs_layer_sorsimple_mbaas")
    logger.setLevel(numeric_level)
    
    # Evitar duplicación de handlers
    if not logger.handlers:
        # Crear handler para consola
        console_handler = logging.StreamHandler()
        console_handler.setLevel(numeric_level)
        
        # Definir formato
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        
        # Agregar handler al logger
        logger.addHandler(console_handler)
    
    return logger


# Crear logger global
logger = setup_logger()
