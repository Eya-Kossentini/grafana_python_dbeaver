"""Authentication schemas."""
from typing import List, Optional
from pydantic import BaseModel, EmailStr


class Token(BaseModel):
    """Token schema for JWT authentication."""
    access_token: str
    token_type: str


class TokenData(BaseModel):
    """Token data schema."""
    email: Optional[str] = None
    role: Optional[str] = None
    permissions: Optional[List[str]] = None


class UserBase(BaseModel):
    """Base user schema."""
    email: EmailStr
    is_active: bool = True
    role: str = "user"
    permissions: List[str] = []


class UserCreate(UserBase):
    """User creation schema."""
    password: str


class UserUpdate(BaseModel):
    """User update schema."""
    email: Optional[EmailStr] = None
    password: Optional[str] = None
    is_active: Optional[bool] = None
    role: Optional[str] = None
    permissions: Optional[List[str]] = None


class UserResponse(UserBase):
    """User response schema."""
    id: int

    class Config:
        from_attributes = True


# For internal use (database)
class UserInDB(UserResponse):
    hashed_password: str

    class Config:
        from_attributes = True


class UserLogin(BaseModel):
    email: EmailStr
    password: str