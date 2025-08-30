"""src/core/rds_service.py"""

import json
import psycopg2
import psycopg2.pool
import time

from datetime import datetime
from contextlib import contextmanager
from sys import exc_info
from typing import Dict, List, Optional

from .interfaces.data_storage import DatabaseService
from ..utils.settings import DB_HOST, DB_PORT, DB_NAME, DB_USERNAME, DB_PASSWORD
from ..utils.log import logger


class RDSService(DatabaseService):

    def __init__(self, host: str = DB_HOST, port: str = DB_PORT, 
                 db_name: str = DB_NAME, min_conn: int = 1, max_conn: int = 10) -> None:
        """
        Inicializa el cliente RDS con un pool de conexiones.

        Args:
            host (str, optional): Host de la BD. Defaults to DB_HOST.
            port (str, optional): Puerto de la BD. Defaults to DB_PORT.
            db_name (str, optional): Nombre de la BD. Defaults to DB_NAME.
            min_conn (int, optional): Número mínimo de conexiones a la BD. Defaults to 1.
            max_conn (int, optional): Número máximo de conexiones a la BD. Defaults to 10.
        """
        self._host = host
        self._port = port
        self._db_name = db_name
        self._min_conn = min_conn
        self._max_conn = max_conn
        self._pool = None
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """
        Inicializa el pool de conexiones.

        Args:
            host (str, optional): Host de la BD. Defaults to DB_HOST.
            port (str, optional): Puerto de la BD. Defaults to DB_PORT.
            db_name (str, optional): Nombre de la BD. Defaults to DB_NAME.
            min_conn (int, optional): Número mínimo de conexiones a la BD. Defaults to 1.
            max_conn (int, optional): Número máximo de conexiones a la BD. Defaults to 10.
        """
        try:
            self._pool = psycopg2.pool.SimpleConnectionPool(
                minconn=self._min_conn,
                maxconn=self._max_conn,
                host=self._host,
                database=self._db_name,
                user=DB_USERNAME,
                password=DB_PASSWORD,
                port=self._port,
                connect_timeout=3,
                application_name='lbdobssorsimple',
                options="-c statement_timeout=5000 -c idle_in_transaction_session_timeout=10000"
            )
            logger.info(f"Pool de conexiones inicializado con min={self._min_conn}, max={self._max_conn}.")
        except Exception as e:
            logger.error(f"Error al inicializar el pool de conexiones: {e}")
            raise
    
    def check_pool_health(self) -> Dict:
        """
        Verifica el estado de salud del pool de conexiones.

        Returns:
            Dict: Estadísticas del pool de conexiones.
        """
        if not self._pool:
            return {"status": "not _initialized"}
        
        try:
            # Comprobar el número de conexiones
            used = len(self._pool._used)
            free = len(self._pool._pool)
            max_conn = self._max_conn
            
            healt_info = {
                "status": "healthy",
                "connections": {
                    "used": used,
                    "free": free,
                    "total": used + free,
                    "max": max_conn,
                    "utilization_percent": round((used / max_conn)*100, 2)                    
                }
            }
            
            # Realizar un ping simple para verificar la conectividad
            with self.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    result = cur.fetchone()
                    healt_info["database_connection"] = "ok" if result and result[0] == 1 else "error"
            
            return healt_info
        
        except Exception as e:
            logger.error(f"Error al verificar salud del pool: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    @contextmanager
    def connection(self):
        """
        Devuelve una conexión del pool de conexiones.

        Yields:
            _type_: Conexión del pool.
        """
        conn = None
        start_time = datetime.now()
        timeout_seconds = 3
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                # Verificar si excedimos el timeout
                elapsed = (datetime.now() - start_time).total_seconds()
                if elapsed > timeout_seconds:
                    logger.warning(f"Timeout de {timeout_seconds}s excedido al intentar obtener conexión.")
                    raise TimeoutError(f"No se pudo obtener una conexión en {timeout_seconds} segundos.")
                
                # Intentar obtener una conexión
                conn = self._pool.getconn()
                logger.debug("Conexión obtenida del pool.")
                
                # Verificar que la conexión sea válida
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                
                break
            except psycopg2.pool.PoolError:
                # Pool está vacío (todas las conexiones están en uso)
                retry_count += 1
                wait_time = 0.1 * (2 ** retry_count)  # Backoff exponencial
                logger.warning(f"Pool vacío. Reintento {retry_count}/{max_retries} en {wait_time:.2f}s")
                time.sleep(wait_time)
                
            except psycopg2.OperationalError as e:
                # Error de conexión
                logger.warning(f"Conexión defectuosa detectada: {str(e)}")
                
                if conn:
                    try:
                        # No devolvemos esta conexión al pool
                        self._pool.closeall()
                        self._pool = None
                        self._initialize_pool()  # Reinicializar el pool
                        logger.info(f"Pool reinicializado después de detectar conexión defctuosa.")
                    except Exception as reinit_error:
                        logger.error(f"Error al reinicializar el pool: {reinit_error}")
                
                # Incrementar contador de reintentos
                retry_count += 1
                if retry_count >= max_retries:
                    raise
        
        if conn is None:
            raise RuntimeError("No se pudo obtener una conexión después de múltiples intentos.")
        
        try:
            yield conn

        except psycopg2.OperationalError as e:
            logger.error(f"Error durante el uso de la conexión: {str(e)}.")
            
            try:
                # Marcar esta conexión como fallida
                conn = None
                
                # Reinicializar el pool si es necesario
                self._pool.closeall()
                self._pool = None
                self._initialize_pool()
                logger.info(f"Pool reinicizalizado.")
            except Exception as reset_error:
                logger.error(f"Error al reinicializar el pool: {reset_error}")
            
            # Propagar el error
            raise
            
        finally:
            # Devolver la conexión al pool si siguie siendo válida
            if conn:
                try:
                    self._pool.putconn(conn)
                    logger.debug("Conexión devuelta al pool.")
                except Exception as return_error:
                    logger.error(f"Error al devolver conexión al pool: {str(return_error)}.")

    def get_events(self, sql: str, params: tuple = None) -> Optional[Dict]:
        """
        Devuelve los eventos de la Base de Datos.

        Args:
            sql (str): Query de consulta a la BD.
            params (tuple, optional): Parámetros para completar el query. Defaults to None.

        Returns:
            Optional[Dict]: Devuelve el diccionario con los datos de la BD.
        """
        try:
            with self.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params or ())
                    result = cur.fetchone()
                    
                    return json.loads(result[0]) if result and isinstance(result[0], str) else result[0] if result else []
        except Exception as e:
            logger.error(f"Error al intentar consultar la BD: {e}")
            raise

    def save_events(self, sql: str, params: tuple) -> None:
        """
        Inserta/Actualiza los datos asociados al cliente.

        Args:
            sql (str): Query de consulta a la BD.
            params (tuple): Parámetros para completar el query.
        """
        try:
            with self.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                conn.commit()
        except Exception as e:
            logger.error(f"Error al intentar guardar/actualizar los datos en la BD: {e}")
            raise

    def delete_events(self, sql: str, params: tuple) -> None:
        """
        Elimina registros de la BD.

        Args:
            sql (str): Query de consulta a la BD.
            params (tuple): Parámetros para completar el query.
        """
        try:
            with self.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                conn.commit()
        except Exception as e:
            logger.error(f"Error al intentar eliminar registros de la BD: {e}")
            raise

    @classmethod
    def close_pool(self) -> None:
        """
        Cierra todas las conexiones de la BD.
        """
        if self._pool:
            self._pool.closeall()
            self._pool = None
            logger.info("Pool de conexiones cerrado.")
