from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.crud.permisos import verify_permissions
from app.router.dependencies import get_current_user
from core.database import get_db
from app.schemas.users import UserCreate, UserOut, UserUpdate
from app.crud import users as crud_users
from sqlalchemy.exc import SQLAlchemyError

router = APIRouter()
modulo = 4

@router.post("/crear", status_code=status.HTTP_201_CREATED)
def create_user(  
    user: UserCreate,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol = user_token.id_rol
        if (user.id_rol ==  1 or user.id_rol == 2):
            modulo = 10
        else:
            modulo = 4
        
        if not verify_permissions(db, id_rol, modulo, 'insertar'):
            raise HTTPException(status_code=401, detail= 'Usuario no autorizado')

        crud_users.create_user(db, user)
        return {"message": "Usuario creado correctamente"}
    
    # except Exception as e:
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/by-email", response_model=UserOut)
def get_user(    
    email: str,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user) ):
    try:

        id_rol = user_token.id_rol

        if not email == user_token.email:
            if not verify_permissions(db, id_rol, modulo, 'seleccionar'):
                raise HTTPException(status_code=401, detail= 'Usuario no autorizado')
            
        user = crud_users.get_user_by_email(db, email)
        if not user:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        return user
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    

    
@router.get("/all-except-admins", response_model=List[UserOut])
def get_users(
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user) 
):
    try:
        id_rol = user_token.id_rol
        if not verify_permissions(db, id_rol, modulo, 'seleccionar'):
            raise HTTPException(status_code=401, detail="Usuario no autorizado")
        users = crud_users.get_all_user_except_admins(db)
        return users
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/by-id/{user_id}")
def update_user(
    user_id: int,
    user: UserUpdate,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:

        id_rol = user_token.id_rol
        if not verify_permissions(db, id_rol, modulo, 'actualizar'):
            raise HTTPException(status_code=401, detail="Usuario no autorizado")
        
        success = crud_users.update_user_by_id(db, user_id, user)
        if not success:
            raise HTTPException(status_code=400, detail="No se pudo actualizar el usuario")
        return {"message": "Usuario actualizado correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/by-document", response_model=UserOut)
def get_user(
    document: str,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol=user_token.id_rol

        if not verify_permissions(db, id_rol, modulo, 'seleccionar'):
            raise HTTPException(status_code=401, detail="usuario no autorizado")

        user = crud_users.get_user_by_document_number(db, document)
        if not user:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        return user
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/by-role", response_model=List[UserOut])
def get_user(
    role: str,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol=user_token.id_rol

        if not verify_permissions(db, id_rol, modulo, 'seleccionar'):
            raise HTTPException(status_code=401, detail="usuario no autorizado")

        user = crud_users.get_user_by_role(db, role)
        if not user:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        return user
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/cambiar-estado/{user_id}", status_code=status.HTTP_200_OK)
def change_user_status(
    user_id: int,
    nuevo_estado: bool,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        # Verificar permisos del usuario
        id_rol = user_token.id_rol
        if not verify_permissions(db, id_rol, modulo, 'actualizar'):
            raise HTTPException(status_code=401, detail="Usuario no autorizado")

        success = crud_users.change_user_status(db, user_id, nuevo_estado)
        if not success:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")

        return {"message": f"Estado del usuario actualizado a {nuevo_estado}"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))