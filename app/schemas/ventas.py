from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from decimal import Decimal

class VentaBase(BaseModel):
    id_usuario: int
    tipo_pago: int

class VentaCreate(VentaBase):
    pass

class VentaUpdate(BaseModel):
    id_usuario: Optional [int] = None
    tipo_pago: Optional [int] = None
    
    
class VentaOut(VentaBase):
    id_venta: int
    fecha_hora: datetime
    total: Decimal