from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from core.database import get_db
from app.router.dependencies import get_current_user
from app.crud.permisos import verify_permissions
from app.schemas.roles import RolCreate, RolOut, RolUpdate
from app.schemas.users import UserOut
from app.crud import roles as crud_roles
from sqlalchemy.exc import SQLAlchemyError

router = APIRouter()

modulo = 1 

@router.post("/crear", status_code=status.HTTP_201_CREATED)
def create_rol(    
    rol: RolCreate,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol = user_token.id_rol

        if not verify_permissions(db, id_rol, modulo, 'insertar'):
            raise HTTPException(status_code=401, detail= 'Usuario no autorizado')
        crud_roles.create_rol(db, rol)
        return {"message": "Rol creado correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get("/by-nombre", response_model=RolOut)
def get_rol_by_nombre(    
    nombre_rol: str,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user) ):
    try:

        id_rol = user_token.id_rol

        if not verify_permissions(db, id_rol, modulo, 'seleccionar'):
            raise HTTPException(status_code=401, detail= 'Usuario no autorizado')
        rol = crud_roles.get_rol_by_nombre(db, nombre_rol)
        if not rol:
            raise HTTPException(status_code=404, detail="rol no encontrado")
        return rol
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    
@router.get("/by-id", response_model=RolOut)
def get_rol_by_id(    
    rol_id: int,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)):
    try:

        id_rol = user_token.id_rol

        if not verify_permissions(db, id_rol, modulo, 'seleccionar'):
            raise HTTPException(status_code=401, detail= 'Usuario no autorizado')
        rol = crud_roles.get_rol_by_id(db, rol_id)
        if not rol:
            raise HTTPException(status_code=404, detail="Rol no encontrado")
        return rol
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    
@router.get("/all-roles", response_model=List[RolOut])
def get_roles(
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user) 
):
    try:
        id_rol = user_token.id_rol
        if not verify_permissions(db, id_rol, modulo, 'seleccionar'):
            raise HTTPException(status_code=401, detail="Usuario no autorizado")
        roles = crud_roles.get_all_roles(db)
        if not roles:
            raise HTTPException(status_code=404, detail="Ningun rol encontrado")
        return roles
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    
@router.put("/by-id/{rol_id}")
def update_rol_by_id(
    rol_id: int,
    rol: RolUpdate,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol = user_token.id_rol

        if not verify_permissions(db, id_rol, modulo, 'actualizar'):
            raise HTTPException(status_code=401, detail= 'Usuario no autorizado')
        success = crud_roles.update_rol_by_id(db, rol_id, rol)
        if not success:
            raise HTTPException(status_code=400, detail="No se pudo actualizar el rol")
        return {"message": "rol actualizado correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@router.delete("/by-id/{rol_id}")
def delete_rol_by_id(
    rol_id: int,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol = user_token.id_rol

        if not verify_permissions(db, id_rol, modulo, 'borrar'):
            raise HTTPException(status_code=401, detail= 'Usuario no autorizado')
        success = crud_roles.delete_rol_by_id(db, rol_id)
        if not success:
            raise HTTPException(status_code=400, detail="No se pudo actualizar el rol")
        return {"message": "rol eliminado correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))