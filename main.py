from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


from app.router import modulos
from app.router import permisos
from app.router import roles
from app.router import users


from app.router import auth

from app.router import ventas
from app.router import tareas
from app.router import detalle_huevos
from app.router import metodo_pago
from app.router import detalle_salvamento


app = FastAPI()


app.include_router(auth.router, prefix="/access", tags=["login"])

app.include_router(modulos.router, prefix="/modulos", tags=["modulos"])
app.include_router(permisos.router, prefix="/permisos", tags=["permisos"])
app.include_router(roles.router, prefix="/roles", tags=["roles"])
app.include_router(users.router, prefix="/users", tags=["users"])

# Incluir en el objeto app los routers


# app.include_router(fincas.router, prefix="/fincas", tags=["fincas"])

app.include_router(ventas.router, prefix="/ventas", tags=["ventas"])
app.include_router(tareas.router, prefix="/tareas", tags=["tareas"])
app.include_router(detalle_huevos.router, prefix="/detalle_huevos", tags=["detalle_huevos"])
app.include_router(metodo_pago.router, prefix="/metodo_pago", tags=["metodo_pago"])
app.include_router(detalle_salvamento.router, prefix="/detalle_salvamento", tags=["detalle_salvamento"])

# Configuración de CORS para permitir todas las solicitudes desde cualquier origen
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permitir solicitudes desde cualquier origen
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],  # Permitir estos métodos HTTP
    allow_headers=["*"],  # Permitir cualquier encabezado en las solicitudes
)

@app.get("/")
def read_root():
    return {
                "message": "ok",
                "autor": "ADSO 2925889"
            }