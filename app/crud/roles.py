from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
import logging
from app.schemas.roles import RolCreate, RolUpdate, RolEstado
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


def create_rol(db: Session, rol: RolCreate) -> Optional[bool]:
    try:
        sentencia = text("""
            INSERT INTO roles (
                nombre_rol, descripcion
            ) VALUES (
                :nombre_rol, :descripcion
            )
        """)
        db.execute(sentencia, rol.model_dump())
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al crear rol: {e}")
        raise Exception("Error de base de datos al crear el rol")
    

def get_rol_by_nombre(db: Session, nombre: str):
    try:
        query = text("""
                    SELECT roles.id_rol, roles.nombre_rol, roles.descripcion, roles.estado
                    FROM roles 
                    WHERE nombre_rol = :name
                """)
        result = db.execute(query, {"name": nombre}).mappings().first()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener rol por nombre: {e}")
        raise Exception("Error de base de datos al obtener el rol")
    
    
def get_rol_by_id(db: Session, rol_id: int):
    try:
        query = text("""
                    SELECT roles.id_rol, roles.nombre_rol, roles.descripcion, roles.estado
                    FROM roles 
                    WHERE id_rol = :rol_id
                """)
        result = db.execute(query, {"rol_id": rol_id}).mappings().first()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener rol por id: {e}")
        raise Exception("Error de base de datos al obtener el rol")
      
    
def get_all_roles(db: Session):
    try:
        query = text("""
                    SELECT roles.id_rol, roles.nombre_rol, roles.descripcion
                    FROM roles
                """)
        result = db.execute(query).mappings().all()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener los roles: {e}")
        raise Exception("Error de base de datos al obtener los roles")    
    
    
def update_rol_by_id(db: Session, rol_id: int, rol: RolUpdate) -> Optional[bool]:
    try:
        rol_data = rol.model_dump(exclude_unset=True)
        if not rol_data:
            return False

        set_clauses = ", ".join([f"{key} = :{key}" for key in rol_data.keys()])
        sentencia = text(f"""
            UPDATE roles
            SET {set_clauses}
            WHERE id_rol = :id_rol
        """)

        # Agregar el id_rol
        rol_data["id_rol"] = rol_id

        result = db.execute(sentencia, rol_data)
        db.commit()

        # devuelve true si la operacion afecto mas de 0 registros en la bd
        return result.rowcount > 0
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al actualizar usuario {rol_id}: {e}")
        raise Exception("Error de base de datos al actualizar la rol")
    
    
def cambiar_rol_estado(db: Session, id_rol: int, nuevo_estado: bool) -> bool:
    try:
        sentencia = text("""
            UPDATE roles
            SET estado = :nuevo_estado
            WHERE id_rol = :id_rol              
        """)
        result = db.execute(sentencia, {"nuevo_estado": nuevo_estado, "id_rol": id_rol})
        
        # Verificar si alguna fila fue afectada
        if result.rowcount > 0:
            db.commit()
            return True
        else:
            db.rollback()  
            return False
    except SQLAlchemyError as e:
        db.rollback() 
        logger.error(f"Error al cambiar el estado del rol {id_rol}: {e}")
        raise Exception("Error de base de datos al cambiar el estado del rol")
