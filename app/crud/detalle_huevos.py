from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional
from fastapi import HTTPException
import logging

from app.schemas.detalle_huevos import DetalleHuevosCreate, DetalleHuevosUpdate

logger = logging.getLogger(__name__)

def create_detalle_huevos(db: Session, detalle_h: DetalleHuevosCreate) -> dict:
    try:
        stock_disponible = db.execute(text("SELECT cantidad_disponible FROM stock WHERE id_producto = :id_producto"), {"id_producto": detalle_h.id_producto}).mappings().first()
        if not stock_disponible or stock_disponible['cantidad_disponible'] < detalle_h.cantidad:
            raise HTTPException(status_code=400, detail="Stock insuficiente para completar la operación")

        sentencia = text("""
            INSERT INTO detalle_huevos(
                id_producto, cantidad, id_venta,
                valor_descuento, precio_venta
            ) VALUES (
                :id_producto, :cantidad, :id_venta,
                :valor_descuento, :precio_venta
            )
        """)
        db.execute(sentencia, detalle_h.model_dump())
        id_creado = db.execute(text("SELECT LAST_INSERT_ID()")).scalar()
        
        db.execute(text("""
            UPDATE stock
            SET cantidad_disponible = cantidad_disponible - :cantidad
            WHERE id_producto = :id_producto
        """), {"cantidad": detalle_h.cantidad, "id_producto": detalle_h.id_producto})
        db.commit()
        return {"id_detalle_huevo": id_creado}
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al crear detalle_huevos: {e}")
        raise Exception("Error de base de datos al crear el detalle_huevos")
    

def update_detalle_huevos_by_id(db: Session, detalle_id: int, detalle_h: DetalleHuevosUpdate) -> Optional[bool]:
    try:
        # Solo los campos enviados por el cliente
        detalle_huevos_data = detalle_h.model_dump(exclude_unset=True)
        if not detalle_huevos_data:
            return False  # nada que actualizar
        

        datos_anteriores = db.execute(text("""
            SELECT id_producto, cantidad
            FROM detalle_huevos
            WHERE id_detalle = :id_detalle
        """), {"id_detalle": detalle_id}).mappings().first()

        id_producto_anterior = datos_anteriores['id_producto']
        cantidad_anterior = datos_anteriores['cantidad']


        id_producto_nuevo = detalle_huevos_data.get('id_producto', id_producto_anterior)
        cantidad_nueva = detalle_huevos_data.get('cantidad', cantidad_anterior)


        if id_producto_nuevo != id_producto_anterior:
            
            stock_nuevo = db.execute(text("""
                SELECT cantidad_disponible FROM stock WHERE id_producto = :id
            """), {"id": id_producto_nuevo}).mappings().first()

            if not stock_nuevo:
                raise HTTPException(status_code=400, detail="Error validacion")
            
            if stock_nuevo['cantidad_disponible'] < cantidad_nueva:
                raise HTTPException(status_code=400, detail="Error validacion")
        else:
            diferencia_cantidad = cantidad_nueva - cantidad_anterior
            if diferencia_cantidad > 0:
                stock_actual = db.execute(text("""
                    SELECT cantidad_disponible FROM stock WHERE id_producto = :id
                """), {"id": id_producto_nuevo}).mappings().first()

                if not stock_actual or stock_actual['cantidad_disponible'] < diferencia_cantidad:
                    raise HTTPException(status_code=400, detail="Stock insuficiente")
                
        if id_producto_nuevo != id_producto_anterior:
            db.execute(text("""
                UPDATE stock
                SET cantidad_disponible = cantidad_disponible + :cant
                WHERE id_producto = :id_ant
            """), {"cant": cantidad_anterior, "id_ant": id_producto_anterior})

            db.execute(text("""
                UPDATE stock
                SET cantidad_disponible = cantidad_disponible - :cantidad_nueva
                WHERE id_producto = :id_producto_nuevo
            """), {"cantidad_nueva": cantidad_nueva, "id_producto_nuevo": id_producto_nuevo})
        else:
            diferencia = cantidad_nueva - cantidad_anterior
            if diferencia != 0:
                db.execute(text("""
                    UPDATE stock
                    SET cantidad_disponible = cantidad_disponible - :dife
                    WHERE id_producto = :id_producto_nuevo
                """), {"dife": diferencia, "id_producto_nuevo": id_producto_nuevo})

        # Construir dinámicamente la sentencia UPDATE
        set_clauses = ", ".join([f"{key} = :{key}" for key in detalle_huevos_data.keys()])
        sentencia = text(f"""
            UPDATE detalle_huevos
            SET {set_clauses}
            WHERE id_detalle = :id_detalle
        """)

        # Agregar el id_detalle
        detalle_huevos_data["id_detalle"] = detalle_id

        result = db.execute(sentencia, detalle_huevos_data)
        db.commit()

        return result.rowcount > 0
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al actualizar detalle_huevos {detalle_id}: {e}")
        print("Error al actualizar detalle_huevos:", e)
        raise 


def get_detalle_huevos_by_id(db: Session, id_detalle: int):
    try:
        query = text("""SELECT id_detalle, id_producto, cantidad, id_venta, 
                        valor_descuento, precio_venta
                    FROM detalle_huevos
                    WHERE id_detalle = :id_detalle
                """)
        result = db.execute(query, {"id_detalle": id_detalle}).mappings().first()
        return result
    except SQLAlchemyError as e:  
        logger.error(f"Error de BD al obtener detalle {e}")
        raise Exception("Error de base de datos al obtener el detalle") 

def get_detalle_huevos_by_id_venta(db:Session, id:int):
    try:
        query = text("""SELECT * FROM detalle_huevos INNER JOIN ventas ON detalle_huevos.id_venta=ventas.id_venta
                     WHERE detalle_huevos.id_venta = :id_venta
                """)
        result = db.execute(query, {"id_venta": id}).mappings().all()
        return result
    except SQLAlchemyError as e:
        logger.error(f"Error al obtener detalle_huevos por id_venta: {e}")
        raise Exception("Error de base de datos al obtener el detalle_huevos por id_venta")
    

def delete_detalle_huevos_by_id(db: Session, detalle_id: int):
    try:
        data = db.execute(text("""
            SELECT id_producto, cantidad FROM detalle_huevos WHERE id_detalle = :id_detalle
        """), {"id_detalle": detalle_id}).mappings().first()

        sentencia = text("""
            DELETE FROM detalle_huevos
            WHERE id_detalle = :id_detalle
        """)
        result = db.execute(sentencia, {"id_detalle": detalle_id})

        db.execute(text("""
            UPDATE stock
            SET cantidad_disponible = cantidad_disponible + :cantidad
            WHERE id_producto = :id_producto
        """), data)

        db.commit()
        return result.rowcount > 0
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al eliminar detalle_huevos {detalle_id}: {e}")
        raise Exception("Error de base de datos al eliminar el detalle_huevos")
    
def delete_all_detalle_huevos_by_id_venta(db: Session, id_venta: int):
    try:
        # Obtener los detalles de la venta
        data = db.execute(text("""
            SELECT id_detalle, id_producto, cantidad
            FROM detalle_huevos
            WHERE id_venta = :id_venta
        """), {'id_venta': id_venta}).mappings().all()

        if not data:
            # Si no se encuentran detalles, podemos considerarlo un caso no crítico
            logger.info(f"No se encontraron detalles para la venta con id {id_venta}.")
            return True  # No hay errores, pero tampoco hay detalles que eliminar.

        # Eliminar cada detalle y actualizar la tabla salvamento
        for detalle in data:
            db.execute(text("""
                DELETE FROM detalle_huevos
                WHERE id_detalle = :id_detalle
            """), {"id_detalle": detalle['id_detalle']})

            # Actualizar el stock de gallinas en la tabla salvamento
            db.execute(text("""
                UPDATE stock
                SET cantidad_disponible = cantidad_disponible + :cantidad 
                WHERE id_producto = :id_producto
            """), {
                "cantidad": detalle['cantidad'],
                "id_producto": detalle['id_producto']
            })

        # *No commit aquí*. La función solo realiza las acciones SQL, y la transacción se maneja en la función llamadora.
        return True

    except SQLAlchemyError as e:
        logger.error(f"Error al eliminar detalles de venta con id_venta {id_venta}: {e}")
        raise Exception("Error al eliminar los detalles de venta")
    
def get_all_products_stock(db: Session):
    try:
        # Obtener los detalles de la venta
        data = text("""
            SELECT 
                stock.id_producto,
                stock.unidad_medida,
                tipo_huevos.color,
                tipo_huevos.tamaño AS tamanio
            FROM 
                stock
            INNER JOIN 
                produccion_huevos ON stock.id_produccion = produccion_huevos.id_produccion
            INNER JOIN 
                tipo_huevos ON produccion_huevos.id_tipo_huevo = tipo_huevos.id_tipo_huevo 
        """)
        result = db.execute(data).mappings().all()

        # *No commit aquí*. La función solo realiza las acciones SQL, y la transacción se maneja en la función llamadora.
        return result

    except SQLAlchemyError as e:
        logger.error(f"error al obtener productos")
        raise Exception("Error de base datos al obtener productos")


