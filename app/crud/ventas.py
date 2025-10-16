from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
import logging
from app.schemas.ventas import VentaCreate, VentaUpdate 
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone
from datetime import date


logger = logging.getLogger(__name__)

def create_venta(db: Session, venta: VentaCreate) -> Optional[bool]:
    try:
        sentencia = text("""
            INSERT INTO ventas (
                fecha_hora, id_usuario,
                tipo_pago
            ) VALUES (
                :fecha_hora, :id_usuario,
                :tipo_pago
            )
        """)
        
        # convertir datos enviados por el cliente en diccionario
        venta_data = venta.model_dump()
        # agregar campo fecha_hora (porque no lo envia el cliente)
        # fecha_hora se esta guardando con la zona horaria UTC, osea que al mostrar debo convertirlo a zona horaria Colombia
        venta_data['fecha_hora'] = datetime.now(timezone.utc)
         
        db.execute(sentencia, venta_data)
        db.commit()
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al crear venta: {e}")
        raise Exception("Error de base de datos al crear la venta")
       

def get_all_ventas(db: Session):
    try:
        query = text("""
                    SELECT 
                        ventas.id_usuario, 
                        ventas.tipo_pago, 
                        ventas.id_venta, 
                        CONVERT_TZ(ventas.fecha_hora, '+00:00', '-05:00') AS fecha_hora,
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
                        ), 0) AS total
                    FROM ventas
                """)
        result = db.execute(query).mappings().all()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener las ventas: {e}")
        raise Exception("Error de base de datos al obtener las ventas")
    
    
def get_ventas_by_fecha(db: Session, fecha: date):
    try:
        query = text("""
                    SELECT 
                        ventas.id_usuario, 
                        ventas.tipo_pago, 
                        ventas.id_venta, 
                        CONVERT_TZ(ventas.fecha_hora, '+00:00', '-05:00') AS fecha_hora,
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
                        ), 0) AS total
                    FROM ventas
                    WHERE DATE(fecha_hora) = :date
                """)
        result = db.execute(query, {"date": fecha}).mappings().all()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener venta por fecha: {e}")
        raise Exception("Error de base de datos al obtener la venta")
    

    
def get_ventas_by_usuario(db: Session, usuario_id: id):
    try:
        query = text("""
                    SELECT 
                        ventas.id_usuario, 
                        ventas.tipo_pago, 
                        ventas.id_venta, 
                        CONVERT_TZ(ventas.fecha_hora, '+00:00', '-05:00') AS fecha_hora,
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
                        ), 0) AS total
                    FROM ventas
                    WHERE id_usuario = :usuario_id
                """)
        result = db.execute(query, {"usuario_id": usuario_id}).mappings().all()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener venta por fecha: {e}")
        raise Exception("Error de base de datos al obtener la venta")
    
    
def get_venta_by_id(db: Session, venta_id: int):
    try:
        query = text("""
                    SELECT 
                        ventas.id_usuario, 
                        ventas.tipo_pago, 
                        ventas.id_venta, 
                        CONVERT_TZ(ventas.fecha_hora, '+00:00', '-05:00') AS fecha_hora,
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
                        ), 0) AS total
                    FROM ventas
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

        # Agregar el id_venta
        venta_data["id_venta"] = venta_id

        result = db.execute(sentencia, venta_data)
        db.commit()

        # devuelve true si la operacion afecto mas de 0 registros en la bd
        return result.rowcount > 0
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al actualizar usuario {venta_id}: {e}")
        raise Exception("Error de base de datos al actualizar la venta")
    

def delete_venta_by_id(db: Session, venta_id: int) -> Optional[bool]:
    try:
        sentencia = text("""
                    DELETE 
                    FROM ventas
                    WHERE id_venta = :id_venta
                """)
        result = db.execute(sentencia, {'id_venta': venta_id})
        db.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Venta no encontrada")
        return result.rowcount > 0
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al eliminar venta {venta_id}: {e}")
        raise Exception("Error de base de datos al eliminar la venta")
    
