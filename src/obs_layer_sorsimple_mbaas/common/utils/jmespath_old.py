"""
Utilidades para manipulación de JMESPath y JSON.
"""
import jmespath
from typing import Any, Dict, Generator, List, Optional, Tuple

from obs_layer_sorsimple_mbaas.common.utils.log import logger


def extract_from_message_selected_fields(
    paths: List[List], 
    event: Dict
) -> Generator[Tuple[str, Any], None, None]:
    """
    Extrae campos seleccionados de un evento usando JMESPath.
    
    Args:
        paths: Lista de pares [ruta, habilitado]
        event: Evento del que extraer los campos
        
    Yields:
        Tupla (nombre_campo, valor)
        
    Raises:
        TypeError: Si hay un error de tipo en JMESPath
        ValueError: Si hay un error de sintaxis en JMESPath
        RuntimeError: Si hay otro error durante la extracción
    """
    if not paths or not event:
        return
        
    try:
        for path, enabled in paths:
            # Verificar si está habilitado
            if enabled.lower() != "true":
                continue
                
            # Extraer valor usando JMESPath
            result = jmespath.search(path, event)
            
            # Extraer nombre del campo (última parte de la ruta)
            field_name = path.split('.')[-1]
            
            # Solo devolver si se encontró un valor
            if result is not None:
                yield (field_name, result)
    except jmespath.exceptions.JMESPathTypeError as e:
        logger.error(f"Error de tipo en JMESPath: {e}")
        raise TypeError(f"Error de tipo en JMESPath: {e}")
    except jmespath.exceptions.ParseError as e:
        logger.error(f"Error de sintaxis en JMESPath: {e}")
        raise ValueError(f"Error de sintaxis en JMESPath: {e}")
    except Exception as e:
        logger.error(f"Error al extraer campos: {e}")
        raise RuntimeError(f"Error al extraer campos: {e}")


def safe_jmespath_search(
    path: str, 
    data: Dict, 
    default: Any = None
) -> Any:
    """
    Realiza una búsqueda JMESPath de manera segura.
    
    Args:
        path: Ruta JMESPath a buscar
        data: Datos en los que buscar
        default: Valor por defecto si no se encuentra o hay error
        
    Returns:
        Resultado de la búsqueda o valor por defecto
    """
    try:
        result = jmespath.search(path, data)
        return result if result is not None else default
    except Exception as e:
        logger.warning(f"Error en búsqueda JMESPath '{path}': {e}")
        return default
