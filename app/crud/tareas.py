from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import logging
from app.schemas.tareas import TareaCreate, TareaUpdate

logger = logging.getLogger(__name__)

# Crear una tarea
def create_tarea(db: Session, tarea: TareaCreate):
    try:
        sentencia = text("""
            INSERT INTO tareas (id_usuario, descripcion, fecha_hora_init, estado, fecha_hora_fin)
            VALUES (:id_usuario, :descripcion, :fecha_hora_init, :estado, :fecha_hora_fin)
        """)
        db.execute(sentencia, tarea.model_dump())
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al crear tarea: {e}")
        raise Exception("Error al crear la tarea")

# Obtener todas las tareas
def get_all_tareas(db: Session):
    try:
        query = text("SELECT * FROM tareas")
        result = db.execute(query).mappings().all()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener todas las tareas: {e}")
        raise Exception("Error al obtener todas las tareas")

# Obtener tareas por usuario
def get_tareas_by_user(db: Session, id_usuario: int, usuario_actual: int, rol_actual: int):
    try:
        # Si el rol es operario, solo puede consultar sus propias tareas
        if rol_actual == 4 and id_usuario != usuario_actual:
            raise Exception("No tienes permiso para ver tareas de otros usuarios")

        query = text("SELECT * FROM tareas WHERE id_usuario = :id_usuario")
        result = db.execute(query, {"id_usuario": id_usuario}).mappings().all()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener tareas del usuario {id_usuario}: {e}")
        raise Exception("Error al obtener las tareas del usuario")


# Obtener tarea por ID
def get_tarea_by_id(db: Session, id_tarea: int):
    try:
        query = text("SELECT * FROM tareas WHERE id_tarea = :id_tarea")
        result = db.execute(query, {"id_tarea": id_tarea}).mappings().first()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener tarea {id_tarea}: {e}")
        raise Exception("Error al obtener la tarea")

# Actualizar tarea por ID de tarea 
def update_tarea(db: Session, id_tarea: int, tarea: TareaUpdate):
    try:
        fields = tarea.model_dump(exclude_unset=True)
        if not fields:
            return False

        set_clause = ", ".join([f"{key} = :{key}" for key in fields.keys()])
        fields["id_tarea"] = id_tarea

        query = text(f"UPDATE tareas SET {set_clause} WHERE id_tarea = :id_tarea")
        result = db.execute(query, fields)
        db.commit()
        return result.rowcount > 0
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al actualizar tarea {id_tarea}: {e}")
        raise Exception("Error al actualizar la tarea")

#  Actualizar tarea por id de usuario 
def update_tarea_by_user(db: Session, id_usuario: int, tarea: TareaUpdate):
    try:
        fields = tarea.model_dump(exclude_unset=True)
        if not fields:
            return False

        set_clause = ", ".join([f"{key} = :{key}" for key in fields.keys()])
        fields["id_usuario"] = id_usuario

        query = text(f"UPDATE tareas SET {set_clause} WHERE id_usuario = :id_usuario")
        result = db.execute(query, fields)
        db.commit()
        return result.rowcount > 0
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al actualizar tareas del usuario {id_usuario}: {e}")
        raise Exception("Error al actualizar las tareas del usuario")

# # Eliminar tarea
# def delete_tarea(db: Session, id_tarea: int):
#     try:
#         query = text("DELETE FROM tareas WHERE id_tarea = :id_tarea")
#         result = db.execute(query, {"id_tarea": id_tarea})
#         db.commit()
#         return result.rowcount > 0
#     except SQLAlchemyError as e:
#         db.rollback()
#         logger.error(f"Error al eliminar tarea {id_tarea}: {e}")
#         raise Exception("Error al eliminar la tarea")
