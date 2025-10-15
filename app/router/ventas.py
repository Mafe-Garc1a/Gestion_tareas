from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from core.database import get_db
from app.router.dependencies import get_current_user
from app.crud.permisos import verify_permissions
from app.schemas.ventas import VentaCreate, VentaOut, VentaUpdate
from app.schemas.users import UserOut
from app.crud import ventas as crud_ventas
from sqlalchemy.exc import SQLAlchemyError
from datetime import date

router = APIRouter()
modulo = 5

@router.post("/crear", status_code=status.HTTP_201_CREATED)
def create_venta(    
    venta: VentaCreate,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol = user_token.id_rol

        if not verify_permissions(db, id_rol, modulo, 'insertar'):
            raise HTTPException(status_code=401, detail= 'Usuario no autorizado')
        crud_ventas.create_venta(db, venta)
        return {"message": "Venta creada correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    
@router.get("/all-ventas", response_model=List[VentaOut])
def get_ventas(
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user) 
):
    try:
        id_rol = user_token.id_rol
        if not verify_permissions(db, id_rol, modulo, 'seleccionar'):
            raise HTTPException(status_code=401, detail="Usuario no autorizado")
        ventas = crud_ventas.get_all_ventas(db)
        if not ventas:
            raise HTTPException(status_code=404, detail="Ninguna venta encontrada")
        return ventas
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/all-by-fecha", response_model=List[VentaOut])
def get_ventas_by_fecha(    
    fecha: date,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)):
    try:

        id_rol = user_token.id_rol

        if not verify_permissions(db, id_rol, modulo, 'seleccionar'):
            raise HTTPException(status_code=401, detail= 'Usuario no autorizado')
        ventas = crud_ventas.get_ventas_by_fecha(db, fecha)
        if not ventas:
            raise HTTPException(status_code=404, detail="Ninguna venta encontrada")
        return ventas
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get("/all-by-usuario", response_model=List[VentaOut])
def get_ventas_by_usuario(    
    usuario_id: int,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)):
    try:
        id_rol = user_token.id_rol

        if not verify_permissions(db, id_rol, modulo, 'seleccionar'):
            raise HTTPException(status_code=401, detail= 'Usuario no autorizado')
        ventas = crud_ventas.get_ventas_by_usuario(db, usuario_id)
        if not ventas:
            raise HTTPException(status_code=404, detail="Ninguna venta encontrada")
        return ventas
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/by-id", response_model=VentaOut)
def get_venta_by_id(    
    venta_id: int,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user) ):
    try:
        id_rol = user_token.id_rol

        if not verify_permissions(db, id_rol, modulo, 'seleccionar'):
            raise HTTPException(status_code=401, detail= 'Usuario no autorizado')
        venta = crud_ventas.get_venta_by_id(db, venta_id)
        if not venta:
            raise HTTPException(status_code=404, detail="Venta no encontrada")
        return venta
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    
@router.put("/by-id/{venta_id}")
def update_venta_by_id(
    venta_id: int,
    venta: VentaUpdate,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol = user_token.id_rol

        if not verify_permissions(db, id_rol, modulo, 'actualizar'):
            raise HTTPException(status_code=401, detail= 'Usuario no autorizado')
        success = crud_ventas.update_venta_by_id(db, venta_id, venta)
        if not success:
            raise HTTPException(status_code=400, detail="No se pudo actualizar la venta")
        return {"message": "Venta actualizada correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@router.delete("/by-id/{venta_id}")
def delete_venta_by_id(
    venta_id: int,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol = user_token.id_rol

        if not verify_permissions(db, id_rol, modulo, 'borrar'):
            raise HTTPException(status_code=401, detail= 'Usuario no autorizado')
        success = crud_ventas.delete_venta_by_id(db, venta_id)
        if not success:
            raise HTTPException(status_code=400, detail="No se pudo actualizar la venta")
        return {"message": "Venta eliminada correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    