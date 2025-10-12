from app.schemas.users import UserOut
from pydantic import BaseModel, EmailStr, Field
from typing import Optional

class ResponseLoggin(BaseModel):
    user: UserOut
    access_token: str