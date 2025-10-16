from pydantic import BaseModel, Field
from typing import Optional

class ModuloBase(BaseModel):
    nombre_modulo: str = Field( min_length=3, max_length=30)

class ModuloCreate(ModuloBase):
    pass

class ModuloUpdate(BaseModel):
    nombre_modulo: Optional[str] = Field(default=None, min_length=3, max_length=30)

class ModuloOut(ModuloBase):
    id_modulo: int