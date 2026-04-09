"""Authentication endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from dependency_injector.wiring import inject, Provide
from typing import Dict, Any

from containers import Container
from auth_client.utils import verify_token

# Create router without prefix - the prefix will be added in the main app
router = APIRouter(tags=["auth"])

# OAuth2 scheme for token validation
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token")

@router.get("/validate")
@inject
async def validate_token(
    token: str = Depends(oauth2_scheme)
) -> Dict[str, Any]:
    """
    Validate a token from auth_service.
    This endpoint is used to verify if a token is valid and get its permissions.
    """
    payload = verify_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return {
        "email": payload.get("sub"),
        "role": payload.get("role", "user"),
        "permissions": payload.get("permissions", [])
    }