"""common/utils/json_utils.py"""

import json

from typing import Any, Dict, List, Optional, Union
from flatten_json import flatten, unflatten, unflatten_list

from obs_layer_sorsimple_mbaas.common.utils.log import logger


def to_json(obj: Any) -> str:
    """
    Convierte un objeto a string JSON.
    
    Args:
        obj: Objeto a convertir
        
    Returns:
        String JSON
        
    Raises:
        ValueError: Si no se puede convertir
    """
    try:
        return json.dumps(obj, default=str)
    except Exception as e:
        logger.error(f"Error al convertir a JSON: {e}")
        raise ValueError(f"No se pudo convertir a JSON: {e}")


def from_json(json_str: str) -> Any:
    """
    Convierte un string JSON a objeto Python.
    
    Args:
        json_str: String JSON a convertir
        
    Returns:
        Objeto Python
        
    Raises:
        ValueError: Si no se puede convertir
    """
    try:
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        logger.error(f"Error al decodificar JSON: {e}")
        raise ValueError(f"JSON inválido: {e}")


def flatten_json_obj(data: Dict, delimiter: str = '.') -> Dict:
    """
    Aplana un objeto JSON anidado.
    
    Args:
        data: Datos a aplanar
        delimiter: Delimitador para las claves
        
    Returns:
        Objeto aplanado
        
    Raises:
        ValueError: Si no se puede aplanar
    """
    try:
        return flatten(data, delimiter)
    except Exception as e:
        logger.error(f"Error al aplanar JSON: {e}")
        raise ValueError(f"No se pudo aplanar JSON: {e}")


def unflatten_json_obj(data: Dict, delimiter: str = '.', as_list: bool = True) -> Dict:
    """
    Desaplana un objeto JSON aplanado.
    
    Args:
        data: Datos aplanados
        delimiter: Delimitador usado en las claves
        as_list: Si es True, usa unflatten_list para manejar listas
        
    Returns:
        Objeto desaplanado
        
    Raises:
        ValueError: Si no se puede desaplanar
    """
    try:
        if as_list:
            return unflatten_list(data, delimiter)
        return unflatten(data, delimiter)
    except Exception as e:
        logger.error(f"Error al desaplanar JSON: {e}")
        raise ValueError(f"No se pudo desaplanar JSON: {e}")


def deep_merge(dict1: Dict, dict2: Dict) -> Dict:
    """
    Combina dos diccionarios de manera recursiva.
    
    Args:
        dict1: Primer diccionario
        dict2: Segundo diccionario (sus valores tienen prioridad)
        
    Returns:
        Diccionario combinado
    """
    result = dict1.copy()
    
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
            
    return result


def safe_get(data: Dict, path: str, default: Any = None) -> Any:
    """
    Obtiene un valor de un diccionario usando una ruta de acceso.
    
    Args:
        data: Diccionario de datos
        path: Ruta de acceso (formato: "key1.key2.key3")
        default: Valor por defecto si no se encuentra
        
    Returns:
        Valor encontrado o valor por defecto
    """
    try:
        parts = path.split('.')
        current = data
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return default
                
        return current
    except Exception:
        return default
