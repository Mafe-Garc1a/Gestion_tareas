from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Dict, Optional
import logging
from app.schemas.ventas import VentaCreate, VentaUpdate, VentaEstado
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from datetime import datetime, timezone
from datetime import date
from app.crud.detalle_huevos import delete_all_detalle_huevos_by_id_venta           
from app.crud.detalle_salvamento import delete_all_detalle_salvamento_by_id_venta   

logger = logging.getLogger(__name__)

def create_venta(db: Session, venta: VentaCreate) -> Optional[Dict]:
    '''
        Crea una venta en la base de datos y devuelve la venta completa
        incluyendo el nombre del usuario y el método de pago.
    '''
    try:
        sentencia = text("""
            INSERT INTO ventas (
                fecha_hora, id_usuario,
                tipo_pago, estado
            ) VALUES (
                :fecha_hora, :id_usuario,
                :tipo_pago, :estado
            )
        """)
        
        venta_data = venta.model_dump()
        venta_data['tipo_pago'] = 1  # SE CREA CON VALOR POR DEFECTO 1 (DEBE EXISTIR EN BASE DE DATOS UN REGISTRO CON ID=1 "COMO EFECTIVO" EN LA TABLA METODO_PAGO )
        
        resultado = db.execute(sentencia, venta_data)
        db.commit()
        
        id_venta_creada = resultado.lastrowid
        if id_venta_creada is None:
            logger.warning(f"No se pudo recuperar el id de la venta")
            return None
        
        consulta = text("""
            SELECT 
                ventas.id_usuario,
                usuarios.nombre AS nombre_usuario, 
                ventas.tipo_pago,
                metodo_pago.nombre AS metodo_pago,
                ventas.id_venta, 
                ventas.fecha_hora,
                ventas.estado
            FROM ventas
            LEFT JOIN usuarios ON usuarios.id_usuario = ventas.id_usuario
            LEFT JOIN metodo_pago ON metodo_pago.id_tipo = ventas.tipo_pago
            WHERE id_venta = :id           
        """)
        
        data_consulta = db.execute(consulta, {"id": id_venta_creada}).mappings().first()
        
        if data_consulta is None:
            logger.warning(f"No se pudo recuperar la venta con ID {id_venta_creada}")
            return None
        
        return dict(data_consulta)
    except IntegrityError as e:
        db.rollback()
        logger.error(f"Error de integridad: {e}")
        raise # se deja que el route lo traduzca a HTTP
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al crear venta: {e}")
        raise
       

# def get_all_ventas(db: Session):
#     try:
#         query = text("""
#                     SELECT 
#                         ventas.id_usuario, 
#                         ventas.tipo_pago, 
#                         ventas.id_venta, 
#                         ventas.fecha_hora,
#                         COALESCE((
#                             SELECT SUM((precio_venta - valor_descuento) * cantidad) 
#                             FROM detalle_huevos 
#                             WHERE detalle_huevos.id_venta = ventas.id_venta
#                         ), 0)
#                         +
#                         COALESCE((
#                             SELECT SUM((precio_venta - valor_descuento) * cantidad)
#                             FROM detalle_salvamento 
#                             WHERE detalle_salvamento.id_venta = ventas.id_venta
#                         ), 0) AS total,
#                         ventas.estado
#                     FROM ventas
#                 """)
#         result = db.execute(query).mappings().all()
#         return result
#     except SQLAlchemyError as e:
#         logger.error(f"Error al obtener las ventas: {e}")
#         raise Exception("Error de base de datos al obtener las ventas")
    

# OFFSET :skip salta un numero de filas (por ejemplo, los usuarios ya mostrados).
# FETCH NEXT :limit ROWS ONLY obtiene solo las filas de esa "pagina".
# Usamos parametros :skip y :limit para evitar inyeccion SQL.
def get_all_ventas_pag(db: Session, skip: int = 0, limit: int = 10):

    '''
    Obtiene las ventas con paginacion.
    '''

    try:
        # 1. contar ventas
        count_query = text("""
            SELECT COUNT(id_venta) AS total
            FROM ventas
        """)
        total_result = db.execute(count_query).scalar()

        # 2. Consultar ventas paginadas
        data_query = text("""
            SELECT 
                ventas.id_usuario,
                usuarios.nombre AS nombre_usuario, 
                ventas.tipo_pago,
                metodo_pago.nombre AS metodo_pago,
                ventas.id_venta, 
                ventas.fecha_hora,
                COALESCE((
                    SELECT SUM((precio_venta - valor_descuento) * cantidad) 
                    FROM detalle_huevos 
                    WHERE detalle_huevos.id_venta = ventas.id_venta
                ), 0)
                +
                COALESCE((
                    SELECT SUM((precio_venta - valor_descuento) * cantidad)
                    FROM detalle_salvamento 
                    WHERE detalle_salvamento.id_venta = ventas.id_venta
                ), 0) AS total,
                ventas.estado
            FROM ventas
            LEFT JOIN usuarios ON usuarios.id_usuario = ventas.id_usuario
            LEFT JOIN metodo_pago ON metodo_pago.id_tipo = ventas.tipo_pago
            ORDER BY id_venta
            LIMIT :limit OFFSET :skip              
        """)

        result = db.execute(data_query, {"skip": skip, "limit": limit}).mappings().all()

        # 3. Retornar resultados
        return {
            "cant_ventas": total_result or 0,
            "ventas": [dict(row) for row in result]
        }
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener las ventas: {e}", exc_info=True)
        raise Exception ("Error de base de datos al obtener ventas")
    
    
def get_ventas_by_date_range_pag(db: Session, fecha_inicio: str, fecha_fin: str, skip: int = 0, limit: int = 10):
    
    '''
    Obtiene las ventas por rango de fechas y paginacion
    '''

    try:
        # 1. contar ventas
        count_query = text("""
            SELECT COUNT(id_venta) AS total
            FROM ventas
            WHERE DATE(fecha_hora) BETWEEN :fecha_inicio AND :fecha_fin
        """)
        total_result = db.execute(count_query, {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin}).scalar()
        
        # 2. Consultar ventas paginadas
        data_query = text("""
            SELECT 
                ventas.id_usuario,
                usuarios.nombre AS nombre_usuario, 
                ventas.tipo_pago,
                metodo_pago.nombre AS metodo_pago,
                ventas.id_venta, 
                ventas.fecha_hora,
                COALESCE((
                    SELECT SUM((precio_venta - valor_descuento) * cantidad) 
                    FROM detalle_huevos 
                    WHERE detalle_huevos.id_venta = ventas.id_venta
                ), 0)
                +
                COALESCE((
                    SELECT SUM((precio_venta - valor_descuento) * cantidad)
                    FROM detalle_salvamento 
                    WHERE detalle_salvamento.id_venta = ventas.id_venta
                ), 0) AS total,
                ventas.estado
            FROM ventas
            LEFT JOIN usuarios ON usuarios.id_usuario = ventas.id_usuario
            LEFT JOIN metodo_pago ON metodo_pago.id_tipo = ventas.tipo_pago
            WHERE DATE(fecha_hora) BETWEEN :fecha_inicio AND :fecha_fin
            ORDER BY fecha_hora ASC, id_venta
            LIMIT :limit OFFSET :skip
        """)
        
        
        result = db.execute(data_query, {
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
            "skip": skip,
            "limit": limit
        }).mappings().all()
        
        # 3. Retornar resultados
        return {
            "cant_ventas": total_result or 0,
            "ventas": [dict(row) for row in result]
        }
        
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener venta por rango de fechas y/o paginacion: {e}")
        raise Exception("Error de base de datos al obtener la venta")


def get_ventas_by_usuario_pag(db: Session, usuario_id: int, skip: int = 0, limit: int = 10):

    '''
    Obtiene las ventas que ha registrado un usuario con paginacion.
    '''

    try:
        # 1. contar ventas
        count_query = text("""
            SELECT COUNT(id_venta) AS total
            FROM ventas
            WHERE id_usuario = :usuario_id
        """)
        total_result = db.execute(count_query, {"usuario_id": usuario_id}).scalar()

        # 2. Consultar ventas paginadas
        data_query = text("""
            SELECT 
                ventas.id_usuario,
                usuarios.nombre AS nombre_usuario, 
                ventas.tipo_pago,
                metodo_pago.nombre AS metodo_pago,
                ventas.id_venta, 
                ventas.fecha_hora,
                COALESCE((
                    SELECT SUM((precio_venta - valor_descuento) * cantidad) 
                    FROM detalle_huevos 
                    WHERE detalle_huevos.id_venta = ventas.id_venta
                ), 0)
                +
                COALESCE((
                    SELECT SUM((precio_venta - valor_descuento) * cantidad)
                    FROM detalle_salvamento 
                    WHERE detalle_salvamento.id_venta = ventas.id_venta
                ), 0) AS total,
                ventas.estado
            FROM ventas
            LEFT JOIN usuarios ON usuarios.id_usuario = ventas.id_usuario
            LEFT JOIN metodo_pago ON metodo_pago.id_tipo = ventas.tipo_pago
            WHERE id_usuario = :usuario_id
            ORDER BY id_venta
            LIMIT :limit OFFSET :skip              
        """)

        result = db.execute(data_query, {"usuario_id": usuario_id, "skip": skip, "limit": limit}).mappings().all()

        # 3. Retornar resultados
        return {
            "cant_ventas": total_result or 0,
            "ventas": [dict(row) for row in result]
        }
    
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener las ventas by id_usuario: {e}", exc_info=True)
        raise Exception ("Error de base de datos al obtener ventas")    
   
    
def get_ventas_by_tipo_pago_pag(db: Session, tipo_id: int, skip: int = 0, limit: int = 10):

    '''
    Obtiene las ventas que ha registrado un usuario con paginacion.
    '''

    try:
        # 1. contar ventas
        count_query = text("""
            SELECT COUNT(id_venta) AS total
            FROM ventas
            WHERE tipo_pago = :tipo_id
        """)
        total_result = db.execute(count_query, {"tipo_id": tipo_id}).scalar()

        # 2. Consultar ventas paginadas
        data_query = text("""
            SELECT 
                ventas.id_usuario,
                usuarios.nombre AS nombre_usuario, 
                ventas.tipo_pago,
                metodo_pago.nombre AS metodo_pago,
                ventas.id_venta, 
                ventas.fecha_hora,
                COALESCE((
                    SELECT SUM((precio_venta - valor_descuento) * cantidad) 
                    FROM detalle_huevos 
                    WHERE detalle_huevos.id_venta = ventas.id_venta
                ), 0)
                +
                COALESCE((
                    SELECT SUM((precio_venta - valor_descuento) * cantidad)
                    FROM detalle_salvamento 
                    WHERE detalle_salvamento.id_venta = ventas.id_venta
                ), 0) AS total,
                ventas.estado
            FROM ventas
            LEFT JOIN usuarios ON usuarios.id_usuario = ventas.id_usuario
            LEFT JOIN metodo_pago ON metodo_pago.id_tipo = ventas.tipo_pago
            WHERE tipo_pago = :tipo_id
            ORDER BY id_venta
            LIMIT :limit OFFSET :skip              
        """)
        result = db.execute(data_query, {"tipo_id": tipo_id, "skip": skip, "limit": limit}).mappings().all()
        
        # 3. Retornar resultados
        return {
            "cant_ventas": total_result or 0,
            "ventas": [dict(row) for row in result]
        }
        
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener las ventas by tipo_pago: {e}", exc_info=True)
        raise Exception ("Error de base de datos al obtener ventas")    

    
def get_venta_by_id(db: Session, venta_id: int):
    try:
        query = text("""
                SELECT 
                    ventas.id_usuario,
                    usuarios.nombre AS nombre_usuario, 
                    ventas.tipo_pago,
                    metodo_pago.nombre AS metodo_pago,
                    ventas.id_venta, 
                    ventas.fecha_hora,
                    COALESCE((
                        SELECT SUM((precio_venta - valor_descuento) * cantidad) 
                        FROM detalle_huevos 
                        WHERE detalle_huevos.id_venta = ventas.id_venta
                    ), 0)
                    +
                    COALESCE((
                        SELECT SUM((precio_venta - valor_descuento) * cantidad)
                        FROM detalle_salvamento 
                        WHERE detalle_salvamento.id_venta = ventas.id_venta
                    ), 0) AS total,
                    ventas.estado
                FROM ventas
                LEFT JOIN usuarios ON usuarios.id_usuario = ventas.id_usuario
                LEFT JOIN metodo_pago ON metodo_pago.id_tipo = ventas.tipo_pago
                WHERE ventas.id_venta = :venta_id
                """)
        result = db.execute(query, {"venta_id": venta_id}).mappings().first()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener venta por id: {e}")
        raise Exception("Error de base de datos al obtener la venta")
      
    
def update_venta_by_id(db: Session, venta_id: int, venta: VentaUpdate) -> Optional[bool]:
    try:
        # Solo los campos enviados por el cliente
        venta_data = venta.model_dump(exclude_unset=True)
        if not venta_data:
            return False

        set_clauses = ", ".join([f"{key} = :{key}" for key in venta_data.keys()])
        sentencia = text(f"""
            UPDATE ventas
            SET {set_clauses}
            WHERE id_venta = :id_venta
        """)

        venta_data["id_venta"] = venta_id

        result = db.execute(sentencia, venta_data)
        db.commit()

        # devuelve true si la operacion afecto mas de 0 registros en la bd
        return result.rowcount > 0
    
    except IntegrityError as e:
        db.rollback()
        logger.error(f"Error de integridad: {e}")
        raise # se deja que el route lo traduzca a HTTP
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al actualizar venta: {venta_id}: {e}")
        raise
    
    
def cambiar_venta_estado(db: Session, id_venta: int, nuevo_estado: bool) -> bool:
    try:
        #casos:
        # True -> False (permitir) 
        # True -> True (salta)
        # False -> True (no permitir, ya está cancelada)
        # False -> False (no permitir, ya está cancelada)
        
        dato = db.execute(text("""
            SELECT estado 
            FROM ventas 
            WHERE id_venta = :id_venta"""), {"id_venta": id_venta}).mappings().first()
                
        if not dato:
            logger.error(f"Venta con id {id_venta} no encontrada.")
            return False
        
        # Si el estado actual es 0 (cancelado), no permitir cambios
        if dato['estado'] == 0:
            raise HTTPException(status_code=400, detail=f"La venta {id_venta} ya está cancelada, no se puede actualizar")
        
        # si es diferente al actual, actualizar.
        if nuevo_estado != dato['estado']:
            sentencia = text("""
                UPDATE ventas 
                SET estado = :estado 
                WHERE id_venta = :id_venta""")
            
            result = db.execute(sentencia, {"estado": nuevo_estado, "id_venta": id_venta})
            
            if result.rowcount == 0:
                logger.warning(f"No se pudo actualizar el estado de la venta {id_venta}.")
                return False
            
            success_detalle_huevos = delete_all_detalle_huevos_by_id_venta(db, id_venta)
            success_detalle_salvamento = delete_all_detalle_salvamento_by_id_venta(db, id_venta)
                
            if success_detalle_huevos != True or success_detalle_salvamento != True:
                logger.error(f"Error al eliminar los detalles de la venta {id_venta}.")
                return False
        
        # Commit para aplicar los cambios
        db.commit()
        return True

    except SQLAlchemyError as e:
        db.rollback() 
        logger.error(f"Error al cambiar el estado de la venta {id_venta}: {e}")
        raise
    

def delete_venta_by_id(db: Session, venta_id: int) -> Optional[bool]:
    '''
        Solo se puede eliminar despues de cancelada
    '''

    try:
        consulta = text("""
            SELECT id_venta, estado
            FROM ventas
            WHERE id_venta = :id_venta
        """)
        res_consulta = db.execute(consulta, {'id_venta': venta_id}).mappings().first()

        # Si no existe la venta, no hay nada que eliminar
        if not res_consulta:
            return False
        
        # Si la venta está activa, no se elimina
        if res_consulta['estado']:
            return False
        
        # Si está inactiva, eliminar
        sentencia = text("""
            DELETE 
            FROM ventas
            WHERE id_venta = :id_venta
        """)
        result = db.execute(sentencia, {'id_venta': venta_id})
        db.commit()
        
        return result.rowcount > 0
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al eliminar venta {venta_id}: {e}")
        raise Exception("Error de base de datos al eliminar la venta")
    

def get_all_detalle_by_id_venta(db: Session, venta_id: int):
    try:
        sentencia = text("""
            SELECT 'huevos' AS tipo, id_detalle, id_producto, cantidad, id_venta, valor_descuento, precio_venta
            FROM detalle_huevos
            WHERE id_venta = :venta_id

            UNION ALL

            SELECT 'salvamento' AS tipo, id_detalle, id_producto, cantidad, id_venta, valor_descuento, precio_venta
            FROM detalle_salvamento
            WHERE id_venta = :venta_id;                      
        """)
    
        result = db.execute(sentencia, {"venta_id": venta_id}).mappings().all()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener detalles de la venta: {e}")
        raise Exception("Error de base de datos al obtener detalles de la venta")
