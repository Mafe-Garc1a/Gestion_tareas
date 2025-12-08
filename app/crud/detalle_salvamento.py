from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional
import logging

from app.schemas.detalle_salvamento import CreateDetalleSalvamento, DetalleSalvamentoUpdate

# app./crud/detalle_salvamento
logger = logging.getLogger(__name__) # Agarra la ubicación del archivo con el que estamos trabajando

def create_detalle_salvamento(db: Session, detalle_salvamento: CreateDetalleSalvamento) -> dict:
    try:
        # Verificar cantidad disponible
        salvamento_query = db.execute(text("SELECT cantidad_gallinas FROM salvamento WHERE id_salvamento = :id_producto"),
                                {"id_producto": detalle_salvamento.id_producto}).mappings().first()
        
        if not salvamento_query or salvamento_query["cantidad_gallinas"] < detalle_salvamento.cantidad:
            raise HTTPException(status_code=400, detail="Error validación")
        
        sentencia = text("""
            INSERT INTO detalle_salvamento(
                id_producto, cantidad, id_venta,
                valor_descuento, precio_venta
            ) VALUES (
                :id_producto, :cantidad, :id_venta,
                :valor_descuento, :precio_venta
            )
        """)
        db.execute(sentencia, detalle_salvamento.model_dump())
        id_creado = db.execute(text("SELECT LAST_INSERT_ID()")).scalar()

        
        # Actualizar cantidad
        db.execute(text("""
            UPDATE salvamento
            SET cantidad_gallinas = cantidad_gallinas - :cantidad
            WHERE id_salvamento = :id_producto
        """), {"cantidad": detalle_salvamento.cantidad, "id_producto": detalle_salvamento.id_producto})

        db.commit() # Guardar cambios permanentemente
        return {"id_detalle_salvamento": id_creado}
    except SQLAlchemyError as e:
        db.rollback() 
        logger.error(f"Error al crear detalle salvamento: {e}")
        raise Exception("Error de base de datos al crear el detalle de salvamento")
    
def get_detalle_by_id(db: Session, id_detalle: int):
    try:
        query = text("""SELECT id_detalle, id_producto, cantidad, id_venta, 
                        valor_descuento, precio_venta
                    FROM detalle_salvamento
                    WHERE id_detalle = :id_detalle
                """)
        result = db.execute(query, {"id_detalle": id_detalle}).mappings().first()
        return result
    except SQLAlchemyError as e:  
        logger.error(f"Error de BD al obtener detalle {e}")
        raise Exception("Error de base de datos al obtener el detalle")
    
def get_detalle_by_id_venta(db: Session, id_venta: int):
    try:
        query = text("""SELECT id_detalle, id_producto, cantidad, id_venta, 
                        valor_descuento, precio_venta
                    FROM detalle_salvamento
                    WHERE id_venta = :id_venta
                """)
        result = db.execute(query, {"id_venta": id_venta}).mappings().all()
        return result
    except SQLAlchemyError as e:  
        logger.error(f"Error de BD al obtener detalle por id de venta {id_venta}: {e}")
        raise Exception("Error de base de datos al obtener el detalle por id de venta")
    
def update_detalle_salvamento_by_id(db: Session, detalle_id: int, detalle: DetalleSalvamentoUpdate) -> Optional[bool]:
    try:
        # Solo los campos enviados por el cliente
        detalle_salvamento_data = detalle.model_dump(exclude_unset=True)
        if not detalle_salvamento_data:
            return False  # nada que actualizar
        
        # Obtener el detalle actual
        detalle_anterior = db.execute(text("""
            SELECT id_producto, cantidad FROM detalle_salvamento WHERE id_detalle = :id_detalle
        """), {"id_detalle": detalle_id}).mappings().first()

        if not detalle_anterior:
            raise Exception("Detalle no encontrado")

        id_producto_ant = detalle_anterior["id_producto"]
        cantidad_ant = detalle_anterior["cantidad"]
        
        # Determinar nuevos valores (si no vienen, usar los antiguos)
        id_producto_nuevo = detalle_salvamento_data.get("id_producto", id_producto_ant)
        cantidad_nueva = detalle_salvamento_data.get("cantidad", cantidad_ant)

        # Validar cantidad suficiente si se va a restar stock
        if id_producto_nuevo != id_producto_ant:
            # Si el producto cambia, verificar el stock del nuevo producto
            salvamento_nuevo = db.execute(text("""
                SELECT cantidad_gallinas FROM salvamento WHERE id_salvamento = :id
            """), {"id": id_producto_nuevo}).mappings().first()
            if not  salvamento_nuevo:
                raise HTTPException(status_code=400, detail="Error validación")
            if  salvamento_nuevo["cantidad_gallinas"] < cantidad_nueva:
                raise HTTPException(status_code=400, detail="Error validación")
        else:
            # Si es el mismo producto y aumenta cantidad, validar cantidad
            diferencia = cantidad_nueva - cantidad_ant
            if diferencia > 0:
                salvamento_actual = db.execute(text("""
                    SELECT cantidad_gallinas FROM salvamento WHERE id_salvamento = :id
                """), {"id": id_producto_nuevo}).mappings().first()
                if not salvamento_actual:
                    raise HTTPException(status_code=400, detail="El producto no existe en el salvamento")
                if salvamento_actual["cantidad_gallinas"] < diferencia:
                    raise HTTPException(status_code=400, detail="Cantidad insuficiente")

        # Ajustar el stock según los cambios detectados
        if id_producto_nuevo != id_producto_ant:
            # Devolver cantidad anterior y restar a la nueva
            db.execute(text("""
                UPDATE salvamento
                SET cantidad_gallinas = cantidad_gallinas + :cant
                WHERE id_salvamento = :id_ant
            """), {"cant": cantidad_ant, "id_ant": id_producto_ant})

            db.execute(text("""
                UPDATE salvamento
                SET cantidad_gallinas = cantidad_gallinas - :cant
                WHERE id_salvamento = :id_nuevo
            """), {"cant": cantidad_nueva, "id_nuevo": id_producto_nuevo})

        else:
            # Mismo producto diferencia de la cantidad antigua
            diferencia = cantidad_nueva - cantidad_ant
            if diferencia != 0:
                db.execute(text("""
                    UPDATE salvamento
                    SET cantidad_gallinas = cantidad_gallinas - :dif
                    WHERE id_salvamento = :id_prod
                """), {"dif": diferencia, "id_prod": id_producto_nuevo})
        
                
        # Agregar el id_detalle
        detalle_salvamento_data["id_detalle"] = detalle_id
        # Construir dinámicamente la sentencia UPDATE
        set_clauses = ", ".join([f"{key} = :{key}" for key in detalle_salvamento_data.keys()])
        sentencia = text(f"""
            UPDATE detalle_salvamento
            SET {set_clauses}
            WHERE id_detalle = :id_detalle  
        """)
        result = db.execute(sentencia, detalle_salvamento_data)
        
        db.commit()
        return result.rowcount > 0
    
    except SQLAlchemyError as e:  
        db.rollback()
        logger.error(f"Error al actualizar el detalle de salvamento {e}")
        raise Exception(f"Error de base de datos al actualizar el detalle de salvamento {e}" )

def delete_detalle_salvamento_by_id(db: Session, id_detalle: int) -> Optional[bool]:
    try:
        # Obtener cantidad e id_producto antes de borrar
        data = db.execute(text("""
            SELECT id_producto, cantidad FROM detalle_salvamento WHERE id_detalle = :id_detalle
        """), {"id_detalle": id_detalle}).mappings().first()
        if not data:
            return False
        
        query = text("DELETE FROM detalle_salvamento WHERE id_detalle = :id_detalle")
        result = db.execute(query, {"id_detalle": id_detalle})
        
        # Devolver la cantidad al salvamento
        db.execute(text("""
            UPDATE salvamento
            SET cantidad_gallinas = cantidad_gallinas + :cantidad
            WHERE id_salvamento = :id_producto
        """), data)
        
        db.commit()
        
        return result.rowcount > 0  
        
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al eliminar detalle salvamento {e}")
        raise Exception(f"Error de base de datos al eliminar el detalle de salvamento {e}")

def delete_all_detalle_salvamento_by_id_venta(db: Session, id_venta: int) -> Optional[bool]:
    try:
        # Obtener información sibre detalles salvamento por id venta 
        data = db.execute(text("""
            SELECT  id_detalle, id_producto, cantidad FROM detalle_salvamento WHERE id_venta = :id_venta
        """), {"id_venta": id_venta}).mappings().all()
        if not data:
            # Si no se encuentran detalles, podemos considerarlo un caso no critico 
            logger.info(f"No se encontraron detalles para la venta con id {id_venta}.")
            return True #No hay errores, pero tampoco hay detalles que eliminar.
        
        # Eliminar todos los detalles que encontro en la consulta de arriba
        # Recordar q el data almacena los datos consultados y por eso el for, pueden ser varios
        for detalle in data: 
            db.execute(text("""
                DELETE FROM detalle_salvamento
                WHERE id_detalle = :id_detalle
            """), {"id_detalle": detalle['id_detalle']})
            
            # Actualizar el stock 
            db.execute(text("""
                UPDATE salvamento
                SET cantidad_gallinas = cantidad_gallinas + :cantidad
                WHERE id_salvamento = :id_producto
            """), {
                "cantidad": detalle['cantidad'], 
                "id_producto": detalle['id_producto']
            })
        
        #db.commit() *no commit aqui*
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error al eliminar detalles de venta {id_venta}: {e}")
        raise Exception(f"Error de base de datos al eliminar el detalle de la venta")
    
def get_all_products_salvamento(db: Session):
    try:
        # Obtener los detalles de la venta
        data = text("""
            SELECT 
                salvamento.id_salvamento,
                tipo_gallinas.raza,
                tipo_gallinas.descripcion
            FROM salvamento
            INNER JOIN tipo_gallinas ON salvamento.id_tipo_gallina = tipo_gallinas.id_tipo_gallinas 
        """)
        result = db.execute(data).mappings().all()

        # *No commit aquí*. La función solo realiza las acciones SQL, y la transacción se maneja en la función llamadora.
        return result

    except SQLAlchemyError as e:
        logger.error(f"error al obtener productos")
        raise Exception("Error de base datos al obtener productos")