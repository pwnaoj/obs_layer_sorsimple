"""infrastructure/persistence/database_service.py"""

import os
import json
import psycopg2

from psycopg2.extras import RealDictCursor
from typing import Any, Dict, List, Optional, Tuple, Union

from obs_layer_sorsimple_mbaas.common.utils.log import logger
from obs_layer_sorsimple_mbaas.common.exceptions.domain_exceptions import RepositoryError


class DatabaseService:
    """
    Servicio para operaciones de base de datos PostgreSQL.
    
    Proporciona métodos para conectarse y realizar operaciones
    con una base de datos PostgreSQL.
    """
    
    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        """
        Inicializa el servicio de base de datos.
        
        Args:
            db_config: Configuración de conexión (por defecto, usa variables de entorno)
        """
        self.db_config = db_config or {
            'host': os.environ.get('DB_HOST'),
            'dbname': os.environ.get('DB_NAME'),
            'user': os.environ.get('DB_USER'),
            'password': os.environ.get('DB_PASSWORD'),
            'port': os.environ.get('DB_PORT', 5432)
        }
        self._connection = None
        
    def _get_connection(self):
        """
        Obtiene una conexión a la base de datos.
        
        Returns:
            Conexión a la base de datos
            
        Raises:
            RepositoryError: Si no se puede establecer la conexión
        """
        try:
            if self._connection is None or self._connection.closed:
                self._connection = psycopg2.connect(**self.db_config)
            return self._connection
        except Exception as e:
            error_msg = f"Error al conectar con la base de datos: {e}"
            logger.error(error_msg)
            raise RepositoryError(error_msg)
    
    def _execute_query(self, query: str, params: Optional[Tuple] = None, 
                      fetch: bool = True) -> Union[List[Dict], int]:
        """
        Ejecuta una consulta SQL.
        
        Args:
            query: Consulta SQL a ejecutar
            params: Parámetros para la consulta
            fetch: Si es True, devuelve resultados; si es False, devuelve filas afectadas
            
        Returns:
            Resultados de la consulta o número de filas afectadas
            
        Raises:
            RepositoryError: Si ocurre un error en la operación
        """
        connection = None
        cursor = None
        
        try:
            connection = self._get_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            # Ejecutar consulta
            cursor.execute(query, params)
            
            if fetch:
                # Obtener resultados
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
            else:
                # Confirmar cambios y devolver filas afectadas
                connection.commit()
                return cursor.rowcount
        except Exception as e:
            # Revertir cambios en caso de error
            if connection:
                connection.rollback()
                
            error_msg = f"Error al ejecutar consulta: {e}"
            logger.error(error_msg)
            raise RepositoryError(error_msg, {"query": query})
        finally:
            # Cerrar cursor
            if cursor:
                cursor.close()
    
    def get_events(self, sql: str, params: Optional[Tuple] = None) -> List[Dict]:
        """
        Obtiene eventos de la base de datos.
        
        Args:
            sql: Consulta SQL
            params: Parámetros para la consulta
            
        Returns:
            Lista de eventos
            
        Raises:
            RepositoryError: Si ocurre un error en la operación
        """
        try:
            return self._execute_query(sql, params, fetch=True)
        except Exception as e:
            error_msg = f"Error al obtener eventos: {e}"
            logger.error(error_msg)
            raise RepositoryError(error_msg)
    
    def save_events(self, sql: str, params: Optional[Tuple] = None) -> int:
        """
        Guarda eventos en la base de datos.
        
        Args:
            sql: Consulta SQL
            params: Parámetros para la consulta
            
        Returns:
            Número de filas afectadas
            
        Raises:
            RepositoryError: Si ocurre un error en la operación
        """
        try:
            return self._execute_query(sql, params, fetch=False)
        except Exception as e:
            error_msg = f"Error al guardar eventos: {e}"
            logger.error(error_msg)
            raise RepositoryError(error_msg)
    
    def close(self):
        """
        Cierra la conexión a la base de datos.
        """
        if self._connection and not self._connection.closed:
            self._connection.close()
            self._connection = None
