from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
from datetime import datetime
from decimal import Decimal

class VentaBase(BaseModel):
    # id_usuario = id de quien registra la venta
    id_usuario: int
    fecha_hora: datetime

class VentaCreate(VentaBase):
    pass

class VentaUpdate(BaseModel):
    tipo_pago: Optional [int] = None


class VentaEstado(BaseModel):
    estado: Optional[bool] = None
    
    
class VentaOut(VentaBase):
    id_venta: int
    nombre_usuario: str
    tipo_pago: int
    metodo_pago: str
    # este campo es calculado
    total: Decimal
    estado: bool
    
class ventaPag(BaseModel):
    page: int
    page_size: int
    total_ventas: int
    total_pages: int
    ventas: List[VentaOut]
    

class DatosVentaCreate(BaseModel):
    id_usuario: int
    nombre_usuario: str
    tipo_pago: int
    metodo_pago: str
    id_venta: int
    fecha_hora: datetime
    estado: bool

    
class VentaCreateResponse(BaseModel):
    message: str
    data_venta: DatosVentaCreate
    
    
class DetalleVenta(BaseModel):
    tipo: str
    id_detalle: int
    id_producto: int
    descripcion: str
    cantidad: int
    id_venta: int
    valor_descuento: Decimal
    precio_venta: Decimal
    
