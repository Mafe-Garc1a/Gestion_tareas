from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.crud.permisos import verify_permissions
from app.router.dependencies import get_current_user
from core.database import get_db
from app.schemas.detalle_salvamento import CreateDetalleSalvamento, DetalleSalvamentoOut, DetalleSalvamentoUpdate
from app.schemas.users import UserOut
from app.crud import detalle_salvamento as crud_detalle_salvamento
from sqlalchemy.exc import SQLAlchemyError
from typing import List

router = APIRouter()

@router.post("/crear", status_code=status.HTTP_201_CREATED)
def create_detalle_salvamento(
    detalle_salvamento: CreateDetalleSalvamento, 
    db: Session = Depends(get_db), 
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol = user_token.id_rol
        modulo = 9     
        if not verify_permissions(db, id_rol, modulo, 'insertar'):
            raise HTTPException(status_code=401, detail="Usuario no autorizado para ingresar detalles salvamento")
        
        crud_detalle_salvamento.create_detalle_salvamento(db, detalle_salvamento)
        return {"message": "Detalle Salvamento creado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get("/by-id_detalle", response_model=DetalleSalvamentoOut)
def get_detalle_salvamento(
    id_detalle: int,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol = user_token.id_rol   
        modulo = 9   
        if not verify_permissions(db, id_rol, modulo, 'seleccionar'):
            raise HTTPException(status_code=401, detail="Usuario no autorizado para ver detalle salvamento")

        detalle_salvamento = crud_detalle_salvamento.get_detalle_by_id(db, id_detalle)
        if not detalle_salvamento:
            raise HTTPException(status_code=404, detail="Detalle Salvamento no encontrado")
        return detalle_salvamento
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/by-detallew-venta/{id_venta}", response_model=List[DetalleSalvamentoOut])
def get_detalles_por_venta(
    id_venta: int,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol = user_token.id_rol 
        modulo = 9     
        if not verify_permissions(db, id_rol, modulo, 'seleccionar'):
            raise HTTPException(status_code=401, detail="Usuario no autorizado para ver detalle salvamento")

        detalles_venta_salvamento = crud_detalle_salvamento. get_detalle_by_id_venta(db, id_venta)        
        if not detalles_venta_salvamento:  # "falsy" en Python
            raise HTTPException(
                status_code=404, 
                detail=f"No se encontraron detalles de salvamentos para la venta {id_venta}"
            )
            
        return detalles_venta_salvamento # Lista
        
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.put("/{id_detalle}")
def update_detalle_salvamento(
    id_detalle: int,
    detalle_salvamento: DetalleSalvamentoUpdate,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol = user_token.id_rol 
        modulo = 9     
        if not verify_permissions(db, id_rol, modulo, 'actualizar'):
            raise HTTPException(status_code=401, detail="Usuario no autorizado para actualizar detalle salvamento")
        
        success = crud_detalle_salvamento.update_detalle_salvamento_by_id(db, id_detalle, detalle_salvamento)
        if not success:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, 
                detail=f"Detalle de salvamento con ID {id_detalle} no encontrado"
            )
        return {"message": f"Detalle de salvamento {id_detalle} actualizado correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{id_detalle}")
def delete_detalle_salvamento(
    id_detalle: int,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol = user_token.id_rol 
        modulo = 9     
        if not verify_permissions(db, id_rol, modulo, 'borrar'):
            raise HTTPException(status_code=401, detail="Usuario no autorizado para eliminar detalle salvamento")
        
        success = crud_detalle_salvamento.delete_detalle_salvamento_by_id(db, id_detalle)
        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"Detalle de salvamento con ID {id_detalle} no encontrado"
            )  
        return {"message": "Detalle de salvamento eliminado correctamente"}
        
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))