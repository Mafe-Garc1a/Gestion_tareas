from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.crud.permisos import verify_permissions
from app.router.dependencies import get_current_user
from core.database import get_db

from app.schemas.tareas import TareaCreate, TareaOut, TareaUpdate
from app.schemas.users import UserOut
from app.crud import tareas as crud_tareas

router = APIRouter()
modulo = 6  # ID del módulo


# PARA VER TODAS LAS TAREAS REGISTRADAS 
@router.get("/todas", response_model=List[TareaOut])
def get_all_tareas(
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol = user_token.id_rol
        if not verify_permissions(db, id_rol, modulo, 'seleccionar'):
            raise HTTPException(status_code=401, detail="Usuario no autorizado")

        tareas = crud_tareas.get_all_tareas(db)
        return tareas
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
# Crear una tarea
@router.post("/crear", status_code=status.HTTP_201_CREATED)
def create_tarea(
    tarea: TareaCreate,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol = user_token.id_rol
        # Validar permiso
        if not verify_permissions(db, id_rol, modulo, 'insertar'):
            raise HTTPException(status_code=401, detail="Usuario no autorizado para crear tareas")

        crud_tareas.create_tarea(db, tarea)
        return {"message": "Tarea creada correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

# Obtener todas las tareas de un usuario
@router.get("/usuario/{id_usuario}", response_model=List[TareaOut])
def get_tareas_usuario(
    id_usuario: int,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        rol_actual = user_token.id_rol
        usuario_actual = user_token.id_usuario

        # Si es OPERARIO (id_rol = 4), solo puede ver sus propias tareas
        if rol_actual == 4:
            if id_usuario != usuario_actual:
                raise HTTPException(status_code=401, detail="No tienes permiso para ver tareas de otros usuarios")
            # No validamos verify_permissions, se salta esa parte
        else:
            # Otros roles sí pasan por el control de permisos normal
            if not verify_permissions(db, rol_actual, modulo, 'seleccionar'):
                raise HTTPException(status_code=401, detail="Usuario no autorizado para ver tareas")

        tareas = crud_tareas.get_tareas_by_user(db, id_usuario, usuario_actual, rol_actual)
        if not tareas:
            raise HTTPException(status_code=404, detail="No hay tareas para este usuario")
        return tareas

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))




@router.put("/usuario/{id_usuario}")
def update_tarea_usuario(
    id_usuario: int,
    tarea: TareaUpdate,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol = user_token.id_rol
        if not verify_permissions(db, id_rol, modulo, 'actualizar'):
            raise HTTPException(status_code=401, detail="Usuario no autorizado para editar tareas")

        success = crud_tareas.update_tarea_by_user(db, id_usuario, tarea)
        if not success:
            raise HTTPException(status_code=404, detail="No se encontró ninguna tarea asociada al usuario")
        return {"message": "Tareas del usuario actualizadas correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


# Actualizar tarea por ID
@router.put("/{id_tarea}")
def update_tarea(
    id_tarea: int,
    tarea: TareaUpdate,
    db: Session = Depends(get_db),
    user_token: UserOut = Depends(get_current_user)
):
    try:
        id_rol = user_token.id_rol
        if not verify_permissions(db, id_rol, modulo, 'actualizar'):
            raise HTTPException(status_code=401, detail="Usuario no autorizado para editar tareas")

        success = crud_tareas.update_tarea(db, id_tarea, tarea)
        if not success:
            raise HTTPException(status_code=404, detail="No se encontró la tarea")
        return {"message": "Tarea actualizada correctamente"}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

# # Eliminar tarea por ID
# @router.delete("/{id_tarea}")
# def delete_tarea(
#     id_tarea: int,
#     db: Session = Depends(get_db),
#     user_token: UserOut = Depends(get_current_user)
# ):
#     try:
#         id_rol = user_token.id_rol
#         if not verify_permissions(db, id_rol, modulo, 'eliminar'):
#             raise HTTPException(status_code=401, detail="Usuario no autorizado para eliminar tareas")

#         crud_tareas.delete_tarea(db, id_tarea)
#         return {"message": "Tarea eliminada correctamente"}
#     except SQLAlchemyError as e:
#         raise HTTPException(status_code=500, detail=str(e))
