"""utils/jmespath.py"""

import jmespath

from typing import List, Dict, Any, Tuple, Optional, Generator


class PathAnalyzer:
    """
    Clase para analizar y navegar por estructuras JSON complejas.
    """
    
    @staticmethod
    def _create_unique_key(key: str, index: int, keys: List[str]) -> str:
        """Crea un identificador único para claves que aparecen múltiples veces."""
        return f"{key}_{index}" if keys.count(key) > 1 else key
    
    @staticmethod
    def _handle_list_navigation(current_event: list, key: str, path_so_far: List[str]) -> Tuple[Any, str, str]:
        """
        Maneja la navegación a través de listas.
        
        Returns:
            Tuple[Any, str, str]: (nuevo_evento, tipo_encontrado, clave_proyeccion_lista)
        """
        previous_path = '.'.join(path_so_far[:-1])
        list_projection_key = f"{previous_path}[*].{key}"
        
        # Buscar el primer elemento de la lista que contenga la clave
        for item in current_event:
            if isinstance(item, dict) and key in item:
                return item[key], type(item[key]).__name__, list_projection_key
        
        # No se encontró la clave en ningún elemento
        return "FIELD_NOT_FOUND", "FIELD_NOT_FOUND", list_projection_key
    
    @staticmethod
    def _handle_dict_navigation(current_event: dict, key: str) -> Tuple[Any, str]:
        """
        Maneja la navegación a través de diccionarios.
        
        Returns:
            Tuple[Any, str]: (nuevo_evento, tipo_encontrado)
        """
        if key in current_event:
            current_value = current_event[key]
            return current_value, type(current_value).__name__
        
        return "FIELD_NOT_FOUND", "FIELD_NOT_FOUND"
    
    @staticmethod
    def _handle_invalid_navigation(current_event: Any) -> str:
        """Maneja casos donde no se puede navegar más en la estructura."""
        return f"Cannot navigate further: {type(current_event).__name__}"

def get_type_at_each_level(event: dict = None, query: str = None) -> dict:
    """
    Determina el tipo de estructura en cada nivel del diccionario (evento).
    
    Esta función analiza una ruta JMESPath y determina qué tipo de estructura
    hay en cada nivel del JSON, identificando diccionarios, listas, y valores simples.

    Args:
        event (dict, optional): Estructura JSON a analizar.
        query (str, optional): Ruta JMESPath a analizar.

    Returns:
        dict: Diccionario con el tipo de estructura en cada nivel de la ruta.
    """
    # Guard clause para validaciones tempranas
    if not event or not query:
        return {}
    
    analyzer = PathAnalyzer()
    keys = query.split('.')
    current_event = event
    type_at_levels = {}
    path_so_far = []
    
    # Iteramos por cada clave en la ruta
    for i, key in enumerate(keys):
        # Si ya no podemos seguir navegando, salimos del bucle
        if current_event is None or current_event == "FIELD_NOT_FOUND":
            break
           
        key_id = analyzer._create_unique_key(key, i, keys)
        path_so_far.append(key)
       
        # Procesamos el evento según su tipo
        current_event, type_at_levels = _process_event_by_type(
            analyzer, current_event, key, key_id, path_so_far, type_at_levels
        )
   
    return type_at_levels

def _process_event_by_type(analyzer, current_event, key, key_id, path_so_far, type_at_levels):
    """
    Procesa un evento según su tipo (lista, diccionario u otro).
   
    Args:
        analyzer: Instancia de PathAnalyzer
        current_event: Evento actual a procesar
        key: Clave actual a procesar
        key_id: Identificador único de la clave
        path_so_far: Camino recorrido hasta ahora
        type_at_levels: Diccionario con los tipos en cada nivel
       
    Returns:
        tuple: (evento_actualizado, tipos_actualizados)
    """
    if isinstance(current_event, list):
        return _process_list_event(
            analyzer, current_event, key, key_id, path_so_far, type_at_levels
        )
    elif isinstance(current_event, dict):
        return _process_dict_event(
            analyzer, current_event, key, key_id, type_at_levels
        )
    else:
        # Caso para tipos que no son listas ni diccionarios
        type_at_levels[key_id] = analyzer._handle_invalid_navigation(current_event)
        return None, type_at_levels

def _process_list_event(analyzer, current_event, key, key_id, path_so_far, type_at_levels):
    """Procesa un evento de tipo lista."""
    current_event, type_name, list_projection_key = analyzer._handle_list_navigation(
        current_event, key, path_so_far
    )
   
    if current_event == "FIELD_NOT_FOUND":
        type_at_levels[key_id] = "FIELD_NOT_FOUND"
        return "FIELD_NOT_FOUND", type_at_levels
   
    # Registrar la proyección de lista y el tipo encontrado
    type_at_levels[list_projection_key] = "list projection"
    type_at_levels[key_id] = type_name
   
    return current_event, type_at_levels

def _process_dict_event(analyzer, current_event, key, key_id, type_at_levels):
    """Procesa un evento de tipo diccionario."""
    current_event, type_name = analyzer._handle_dict_navigation(current_event, key)
   
    if current_event == "FIELD_NOT_FOUND":
        type_at_levels[key_id] = "FIELD_NOT_FOUND"
        return "FIELD_NOT_FOUND", type_at_levels
   
    type_at_levels[key_id] = type_name
   
    return current_event, type_at_levels

class QueryBuilder:
    """
    Clase para construir consultas JMESPath optimizadas.
    """
    
    @staticmethod
    def _extract_list_projections(type_at_levels: dict) -> Dict[str, List[str]]:
        """Extrae las proyecciones de lista del análisis de tipos."""
        list_projections = {}
        
        for key, value in type_at_levels.items():
            if value == "list projection":
                parts = key.split('[*].')
                if len(parts) == 2:
                    parent, child = parts
                    if parent not in list_projections:
                        list_projections[parent] = []
                    list_projections[parent].append(child)
        
        return list_projections
    
    @staticmethod
    def _filter_valid_keys(type_at_levels: dict) -> List[str]:
        """Filtra las claves válidas para la construcción de la consulta."""
        invalid_patterns = ('not found', 'Cannot navigate', 'list projection')
        return [
            key for key in type_at_levels 
            if not any(pattern in type_at_levels[key] for pattern in invalid_patterns)
        ]
    
    @staticmethod
    def _parse_key_indices(keys: List[str]) -> List[Tuple[str, int]]:
        """Parsea las claves y sus índices para mantener el orden original."""
        key_indices = []
        
        for key in keys:
            if '_' in key and key.split('_')[-1].isdigit():
                base_key = key.split('_')[0]
                index = int(key.split('_')[-1])
                key_indices.append((base_key, index))
            else:
                key_indices.append((key, 0))
        
        return sorted(key_indices, key=lambda x: x[1])
    
    @staticmethod
    def _build_path_with_indices(key_indices: List[Tuple[str, int]], 
                                list_projections: Dict[str, List[str]]) -> List[str]:
        """Construye la ruta con los índices de lista apropiados."""
        current_path = []
        
        for base_key, _ in key_indices:
            # Verificar si esta clave es parte de una proyección de lista
            for parent, children in list_projections.items():
                if base_key in children and parent in current_path:
                    parent_index = current_path.index(parent)
                    current_path[parent_index] = f"{parent}[0]"
            
            current_path.append(base_key)
        
        # Agregar índices a los padres de lista que no los tengan
        for i, part in enumerate(current_path):
            if part in list_projections and '[' not in part:
                current_path[i] = f"{part}[0]"
        
        return current_path

def construct_jmespath_query(type_at_levels: dict = None) -> str:
    """
    Construye el query JMESPath a partir del análisis de la estructura.
    
    Esta función toma el resultado de get_type_at_each_level y construye
    una consulta JMESPath válida que tenga en cuenta listas y estructuras anidadas.

    Args:
        type_at_levels (dict, optional): Diccionario con la estructura del evento.

    Returns:
        str: Query JMESPath construido a partir de las estructuras del evento.
    """
    if not type_at_levels:
        return ""
    
    builder = QueryBuilder()
    
    # Paso 1: Identificar proyecciones de lista
    list_projections = builder._extract_list_projections(type_at_levels)
    
    # Paso 2: Filtrar claves válidas
    valid_keys = builder._filter_valid_keys(type_at_levels)
    
    # Paso 3: Parsear índices de claves
    key_indices = builder._parse_key_indices(valid_keys)
    
    # Paso 4: Construir ruta con índices
    current_path = builder._build_path_with_indices(key_indices, list_projections)
    
    return '.'.join(current_path)

class ManualIndexingNavigator:
    """
    Clase especializada para navegar manualmente por estructuras JSON complejas.
    Maneja casos especiales donde las listas requieren índices explícitos.
    """
    
    @staticmethod
    def _can_navigate_dict(current: Any, part: str) -> bool:
        """Verifica si podemos navegar a través de un diccionario."""
        return isinstance(current, dict) and part in current
    
    @staticmethod
    def _can_navigate_list(current: Any) -> bool:
        """Verifica si podemos navegar a través de una lista."""
        return isinstance(current, list) and len(current) > 0
    
    @staticmethod
    def _can_access_list_element(current: list, part: str) -> bool:
        """Verifica si podemos acceder a un campo específico en el primer elemento de la lista."""
        if not current:
            return False
        
        first_element = current[0]
        return (first_element is not None and 
                isinstance(first_element, dict) and 
                part in first_element)
    
    @staticmethod
    def _add_index_to_previous_part(modified_parts: List[str]) -> None:
        """Añade índice [0] a la parte anterior cuando encontramos una lista."""
        if modified_parts:
            modified_parts[-1] = f"{modified_parts[-1]}[0]"
    
    @staticmethod
    def _navigate_through_dict(current: dict, part: str, modified_parts: List[str]) -> Any:
        """Navega a través de un diccionario y actualiza las partes modificadas."""
        modified_parts.append(part)
        return current[part]
    
    @staticmethod
    def _navigate_through_list(current: list, part: str, modified_parts: List[str]) -> Optional[Any]:
        """Navega a través de una lista y actualiza las partes modificadas."""
        if not ManualIndexingNavigator._can_access_list_element(current, part):
            return None
        
        modified_parts.append(part)
        return current[0][part]
    
    def navigate_query_path(self, query_parts: List[str], event: dict) -> List[str]:
        """
        Navega a través de la estructura de datos siguiendo el camino del query.
        
        Args:
            query_parts: Lista de partes del query separadas por punto
            event: Estructura JSON a navegar
            
        Returns:
            Lista de partes modificadas del query con índices añadidos donde sea necesario
        """
        modified_parts = []
        current = event
        
        for part in query_parts:
            if self._can_navigate_dict(current, part):
                current = self._navigate_through_dict(current, part, modified_parts)
                
            elif self._can_navigate_list(current):
                # Cuando encontramos una lista, añadimos índice a la parte anterior
                self._add_index_to_previous_part(modified_parts)
                
                # Intentamos navegar a través del primer elemento de la lista
                current = self._navigate_through_list(current, part, modified_parts)
                if current is None:
                    break
            else:
                # No podemos navegar más, salimos del bucle
                break
        
        return modified_parts

class DataExtractor:
    """
    Clase para extraer datos usando diferentes estrategias.
    """
    
    @staticmethod
    def _extract_direct(query: str, event: dict) -> Optional[Any]:
        """Estrategia 1: Extracción directa usando la ruta original."""
        try:
            return jmespath.search(query, event)
        except (jmespath.exceptions.JMESPathTypeError, jmespath.exceptions.ParseError):
            return None
    
    @staticmethod
    def _extract_with_structure_analysis(query: str, event: dict) -> Optional[Any]:
        """Estrategia 2: Extracción usando análisis de estructura."""
        try:
            type_at_levels = get_type_at_each_level(event=event, query=query)
            new_query = construct_jmespath_query(type_at_levels=type_at_levels)
            
            if new_query:
                return jmespath.search(new_query, event)
        except Exception:
            pass
        return None
    
    @staticmethod
    def _extract_with_manual_indexing(query: str, event: dict) -> Optional[Any]:
        """Estrategia 3: Extracción manual para casos especiales con listas."""
        if not query or not event:
            return None
        
        try:
            # Dividir el query en partes individuales
            query_parts = query.split('.')
            
            # Usar el navegador especializado para construir el camino modificado
            navigator = ManualIndexingNavigator()
            modified_parts = navigator.navigate_query_path(query_parts, event)
            
            # Construir y ejecutar la consulta modificada
            return DataExtractor._build_and_execute_modified_query(modified_parts, event)
            
        except Exception:
            # Si cualquier cosa falla, devolvemos None de manera silenciosa
            return None
    
    @staticmethod
    def _build_and_execute_modified_query(modified_parts: List[str], event: dict) -> Optional[Any]:
        """
        Construye y ejecuta la consulta JMESPath modificada.
        
        Args:
            modified_parts: Lista de partes del query con índices añadidos
            event: Estructura JSON donde ejecutar la consulta
            
        Returns:
            Resultado de la consulta o None si falla
        """
        if not modified_parts:
            return None
        
        modified_query = '.'.join(modified_parts)
        return jmespath.search(modified_query, event)
    
    def extract_single_field(self, query: str, event: dict) -> Any:
        """
        Extrae un campo usando múltiples estrategias de extracción.
        
        Args:
            query: Consulta JMESPath a ejecutar
            event: Estructura JSON donde buscar
            
        Returns:
            Valor extraído o None si no se encontró
        """
        type_at_levels = get_type_at_each_level(event=event, query=query)
        
        for value in type_at_levels.values():
            if value == "FIELD_NOT_FOUND":
                return "FIELD_NOT_FOUND"
        
        # Intentar cada estrategia en orden
        strategies = [
            self._extract_direct,
            self._extract_with_structure_analysis,
            self._extract_with_manual_indexing
        ]
        
        for strategy in strategies:
            result = strategy(query, event)
            if result is not None:
                return result
        
        return None

def extract_from_message_selected_fields(paths: List[list], event: dict) -> Generator[Tuple[str, Any], None, None]:
    """
    Extrae los campos del evento usando los queries definidos.
    
    Esta función intenta extraer valores de un JSON utilizando rutas JMESPath.
    Utiliza múltiples estrategias de extracción para manejar diferentes tipos de estructuras.

    Args:
        paths (List[list]): Lista con los queries a extraer y si están habilitados.
        event (dict): Estructura JSON para extraer los valores.

    Yields:
        tuple: Tupla con el query original y el valor extraído.
        
    Raises:
        TypeError: Error de tipo en JMESPath
        ValueError: Error al analizar expresión JMESPath
        KeyError: Campo no encontrado en el evento
        RuntimeError: Error desconocido durante la extracción
    """
    if not paths or not event:
        return
    
    extractor = DataExtractor()
    
    try:
        for path, enabled in paths:
            # Validar que la variable esté habilitada
            if not _is_variable_enabled(enabled):
                continue
            
            # Extraer el valor usando las estrategias disponibles
            result = extractor.extract_single_field(path, event)
            
            yield (path.split('.')[-1], result)
    
    except jmespath.exceptions.JMESPathTypeError as e:
        raise TypeError(f"Error de tipo en la búsqueda de JMESPath: {e}")
    except jmespath.exceptions.ParseError as e:
        raise ValueError(f"Error al analizar la expresión JMESPath: {e}")
    except KeyError as e:
        raise KeyError(f"No se encontró el campo en el evento: {e}")
    except Exception as e:
        raise RuntimeError(f"Error al intentar extraer las variables: {e}")

def _is_variable_enabled(enabled: str) -> bool:
    """
    Función auxiliar para verificar si una variable está habilitada.
    
    Args:
        enabled: String que indica si la variable está habilitada
        
    Returns:
        bool: True si la variable está habilitada
    """
    return enabled.lower() == "true"
