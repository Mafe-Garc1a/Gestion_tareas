from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.crud.permisos import verify_permissions
from app.router.dependencies import get_current_user
from core.database import get_db
from app.schemas.metodo_pago import MetodoPagoCreate, MetodoPagoOut, MetodoPagoUpdate
from app.schemas.users import UserOut
from app.crud import metodo_pago as crud_metodosPago
from sqlalchemy.exc import SQLAlchemyError

router = APIRouter()


# ///////////
modulo = 8 # ID del módulo de métodos de pago (cambiar)
# ///////////
@router.post("/crear", status_code=status.HTTP_201_CREATED)
def create_metodoPago(    
    metodoPago: MetodoPagoCreate,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:

        id_rol = user_token.id_rol

        if not verify_permissions(db, id_rol, modulo, 'insertar'):
            raise HTTPException(status_code=401, detail= 'Usuario no autorizado')
        crud_metodosPago.create_metodoPago(db, metodoPago)
        return {"message": "Metodo de pago creado correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/by-id")
def get_metodoPago(    
    metodoPago_id: int,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user) ):
    try:

        id_rol = user_token.id_rol

        if not verify_permissions(db, id_rol, modulo, 'seleccionar'):
            raise HTTPException(status_code=401, detail= 'Usuario no autorizado')
        metodoPago = crud_metodosPago.get_metodoPago_by_id(db, metodoPago_id)
        if not metodoPago:
            raise HTTPException(status_code=404, detail="Metodo de pago no encontrado")
        return metodoPago
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get("/all-metodosPago", response_model=List[MetodoPagoOut])
def get_metodosPago(
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user) 
):
    try:
        id_rol = user_token.id_rol
        if not verify_permissions(db, id_rol, modulo, 'seleccionar'):
            raise HTTPException(status_code=401, detail="Usuario no autorizado")
        users = crud_metodosPago.get_metodosPago(db)
        return users
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/by-id/{metodoPago_id}")
def update_metodosPago(
    metodoPago_id: int,
    metodoPago: MetodoPagoUpdate,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol = user_token.id_rol
        if not verify_permissions(db, id_rol, modulo, 'actualizar'):
            raise HTTPException(status_code=401, detail="Usuario no autorizado")
        
        success = crud_metodosPago.update_metodoPago_by_id(db, metodoPago_id, metodoPago)
        if not success:
            raise HTTPException(status_code=400, detail="No se pudo actualizar el metodo de pago")
        return {"message": "Metodo de pago actualizado correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
