from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from decimal import Decimal

class VentaBase(BaseModel):
    # id_usuario = id de quien registra la venta
    id_usuario: int
    tipo_pago: int
    fecha_hora: datetime
    estado: bool

class VentaCreate(VentaBase):
    pass

class VentaUpdate(BaseModel):
    id_usuario: Optional [int] = None
    tipo_pago: Optional [int] = None


class VentaEstado(BaseModel):
    estado: Optional[bool] = None
    
    
class VentaOut(VentaBase):
    id_venta: int
    # este campo es calculado
    total: Decimal
    
class ventaPag(BaseModel):
    page: int
    page_size: int
    total_ventas: int
    total_pages: int
    ventas: List[VentaOut]