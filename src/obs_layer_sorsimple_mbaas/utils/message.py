import base64
import jmespath

from typing import List


def encode_base64(message: str):
    """
    Codifica el mensaje usando base64.

    Args:
        message (str): Mensaje a codificar.

    Returns:
        str: Mensaje codificado.
    """
    try:
        # Encode the message to bytes
        message_bytes = message.encode('ascii')
        # Encode the bytes to Base64
        base64_bytes = base64.b64encode(message_bytes)
        # Convert the Base64 bytes back to a string
        base64_message = base64_bytes.decode('ascii')

        return base64_message
    except Exception as e:
        return f"Error intentando codificar el mensaje: {e}"

def decode_base64(base64_message: str):
    """
    Decodifica el mensaje usando base64.

    Args:
        base64_message (str): Mensaje codificado en base64.

    Returns:
        str: Mensaje decodificado.
    """
    try:
        # Decode the Base64 message
        base64_message_bytes = base64_message.encode('ascii')
        decoded_bytes = base64.b64decode(base64_message_bytes)
        decoded_message = decoded_bytes.decode('ascii')

        return decoded_message
    except Exception as e:
        return f"Error intentando decodificar el mensaje: {e}"
    
def extract_configured_vars(id_service: str, list_jmespath_querys: List[list], event: dict):
    """
    Extrae las variables configuradas del archivo de parametrización.
    
    Args:
        id_service (str): ID Servicio.
        list_jmespath_querys (List[list]): Lista de listas que contiene los path de microservicios.
        event (dict): Evento con datos procesados servcio mbaas.

    Yields:
        tuple: Tupla (jmespath_query, value)
    """
    try:
        for jmespath_query, is_priority in list_jmespath_querys:
            is_priority = is_priority.lower() == "true"
            tmp_var = jmespath.search(jmespath_query, event)
            
            if is_priority and id_service != "Observabilidad":
                yield (jmespath_query.split('.')[-1], tmp_var)
            elif is_priority and id_service == "Observabilidad":
                if tmp_var:
                    yield (jmespath_query.split('.')[-1], tmp_var)
                continue
    except jmespath.exceptions.JMESPathTypeError as e:
        raise TypeError(f"Error de tipo en la búsqueda de JMESPath: {e}")
    except jmespath.exceptions.ParseError as e:
        raise ValueError(f"Error al analizar la expresión JMESPath: {e}")
    except KeyError as e:
        raise KeyError(f"No se encontró el campo en el evento: {e}")
    except Exception as e:
        raise RuntimeError(f"Error desconocido al intentar extraer las variables: {e}")

def concat_event_list_values(event: dict) -> dict:
    """
    Convierte a string y concatena los elementos de la lista.

    Args:
        event (str): Evento con datos procesados del servicio mbaas.

    Returns:
        dict: Diccionario con datos procesados del servicio mbaas.
    """
    for key, value in event.items():
        if isinstance(value, list):
            value_ = [str(item) for item in value]
            event[key] = '|'.join(value_)

    return event

def is_event_flatten(event: dict) -> bool:
    """
    Evalua si el evento procesado está aplanado.

    Args:
        event (dict): Evento con datos procesados servcio mbaas.

    Returns:
        bool: True si el evento está aplanado, False si el evento no está aplanado.
    """
    if not isinstance(event, dict):
        raise ValueError(f"El evento no tiene un valor válido. Se espera dict.")
        
    return all(not isinstance(value, (dict, list, tuple, set)) for value in event.values())
