"""infrastructure/repositories/entity_repository.py"""

import json

from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from obs_layer_sorsimple_mbaas.infrastructure.repositories.base_repository import BaseRepository
from obs_layer_sorsimple_mbaas.common.utils.log import logger
from obs_layer_sorsimple_mbaas.common.exceptions.domain_exceptions import RepositoryError


class EntityRepository(BaseRepository):
    """
    Repositorio para acceso a datos de entidades.
    
    Implementa las operaciones específicas para persistir y recuperar
    entidades en la base de datos.
    """
    
    def __init__(self, db_service):
        """
        Inicializa el repositorio con un servicio de base de datos.
        
        Args:
            db_service: Servicio de base de datos para operaciones de persistencia
        """
        self.db_service = db_service
    
    def find(self, entity_name: str, session_id: str, date_str: Optional[str] = None) -> List[Dict]:
        """
        Busca entidades por nombre y session_id.
        
        Args:
            entity_name: Nombre de la entidad/tabla
            session_id: ID de sesión a buscar
            date_str: Fecha en formato YYYYMMDD (por defecto, hoy)
            
        Returns:
            Lista de entidades encontradas
            
        Raises:
            RepositoryError: Si ocurre un error en la operación
        """
        if date_str is None:
            date_str = datetime.now().strftime('%Y%m%d')
            
        try:
            query = ""
            result = self.db_service.get_events(
                sql=query,
                params=(session_id, date_str)
            )
            
            return result or []
        except Exception as e:
            self._handle_error(
                "find", e,
                entity_name=entity_name,
                session_id=session_id,
                date=date_str
            )
    
    def find_tidnid(self, session_id: str, querys: Dict, date_str: Optional[str] = None) -> Optional[str]:
        """
        Busca el tidnid asociado a una sesión.
        
        Args:
            session_id: ID de sesión
            querys: Querys para consultas
            date_str: Fecha en formato YYYYMMDD (por defecto, hoy)
            
        Returns:
            El tidnid si se encuentra, o None
            
        Raises:
            RepositoryError: Si ocurre un error en la operación
        """
        if querys.get('find_tidnid') is None:
            raise ValueError("No hay querys configurados para consultar tidnid en DB.")
        
        if date_str is None:
            date_str = datetime.now().strftime('%Y%m%d')
            
        try:
            query = querys.get('find_tidnid')
            result = self.db_service.get_events(
                sql=query,
                params=(date_str, session_id)
            )
            
            return result[0] if result and len(result) > 0 else None
        except Exception as e:
            self._handle_error(
                "find_tidnid", e,
                session_id=session_id,
                date=date_str
            )
    
    def save(self, entity_name: str, session_id: str, tidnid: str, 
             data: Union[Dict, List], querys: Dict, date_str: Optional[str] = None) -> bool:
        """
        Guarda los datos de una entidad.
        
        Args:
            entity_name: Nombre de la entidad/tabla
            session_id: ID de sesión
            tidnid: Identificador del tipo y número de documento
            data: Datos a guardar (serán convertidos a JSON)
            querys: Querys para consultas
            date_str: Fecha en formato YYYYMMDD (por defecto, hoy)
            
        Returns:
            True si se guardó correctamente
            
        Raises:
            RepositoryError: Si ocurre un error en la operación
        """
        if querys.get('save') is None:
            raise ValueError("No hay querys configurados para insertar data a DB.")
        
        if date_str is None:
            date_str = datetime.now().strftime('%Y%m%d')
            
        try:
            # Convertir data a string JSON
            data_json = json.dumps(data, default=str)
            
            # Construir consulta
            query = querys.get('save').format(entity_name=entity_name)
            
            # Ejecutar consulta
            self.db_service.save_events(
                sql=query,
                params=(session_id, tidnid, data_json, date_str)
            )
            
            logger.info(
                f"Entidad guardada: entity_name={entity_name}, session_id={session_id}, date={date_str}"
            )
            return True
            
        except Exception as e:
            self._handle_error(
                "save", e,
                entity_name=entity_name,
                session_id=session_id,
                tidnid=tidnid,
                date=date_str
            )
