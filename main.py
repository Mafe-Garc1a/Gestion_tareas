from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.router import permisos
from app.router import auth

app = FastAPI()

# Incluir en el objeto app los routers

app.include_router(auth.router, prefix="/access", tags=["login"])
app.include_router(permisos.router, prefix="/permisos", tags=["permisos"])

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