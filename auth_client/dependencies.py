# webapp/auth/dependencies.py
from typing import List, Optional, Any, Dict
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from dependency_injector.wiring import inject, Provide
from functools import wraps

# Local imports
from containers import Container
from auth_client.config import SECRET_KEY, ALGORITHM, TOKEN_URL
from auth_client.permission import expand_permission_groups
from auth_client.utils import verify_token

oauth2_scheme = OAuth2PasswordBearer(tokenUrl=TOKEN_URL)

class TokenData:
    """Token data model."""
    def __init__(self, email: Optional[str] = None, role: Optional[str] = "user", permissions: List[str] = None):
        self.email = email
        self.role = role
        self.permissions = permissions or []

def validate_permissions(permissions: List[str]) -> List[str]:
    """Validate that all permissions exist in our system"""
    from auth_client.permission import API_PERMISSIONS
    invalid_perms = [p for p in permissions if p not in API_PERMISSIONS]
    if invalid_perms:
        raise ValueError(f"Invalid permissions: {', '.join(invalid_perms)}")
    return permissions

def permission_required(required_permission: str):
    """Dependency to check if user has required permission."""
    @inject
    async def check_permission(
        current_user: Dict[str, Any] = Depends(get_current_user)
    ) -> Dict[str, Any]:
        if not current_user or not current_user.get("permissions"):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not enough permissions"
            )
        
        # Check if user has the required permission
        if required_permission not in current_user["permissions"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission '{required_permission}' required"
            )
        
        return current_user
    return check_permission

@inject
async def get_current_user(
    token: str = Depends(oauth2_scheme)
) -> Dict[str, Any]:
    """
    Get the current authenticated user from the token.
    """
    try:
        # Verify the token
        payload = verify_token(token)
        if not payload:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token",
                headers={"WWW-Authenticate": "Bearer"},
            )
            
        email: str = payload.get("sub")
        if email is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        # Merge direct permissions with any provided permission groups
        direct_permissions = payload.get("permissions", [])
        permission_groups = payload.get("permission_groups", [])
        expanded_group_permissions = list(expand_permission_groups(permission_groups))
        merged_permissions = list({*direct_permissions, *expanded_group_permissions})

        # Return user data from token
        return {
            "email": email,
            "role": payload.get("role", "user"),
            "permissions": merged_permissions,
            "permission_groups": permission_groups,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in get_current_user: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

def role_required(role: str):
    """Dependency wrapper to check if user has required role."""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, current_user: Dict[str, Any] = Depends(get_current_user), **kwargs):
            if not current_user["is_active"]:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Inactive user"
                )

            if current_user["role"] != role:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Role {role} required"
                )
            return await func(*args, current_user=current_user, **kwargs)

        return wrapper

    return decorator

def validate_user_permissions(permissions: List[str]) -> List[str]:
    """Validate a list of permissions."""
    from webapp.ADM.machine_assets.machine_setup.user.services.user_service import AVAILABLE_PERMISSIONS

    valid_permissions = []
    for permission in permissions:
        try:
            resource, action = permission.split(":")
            if resource in AVAILABLE_PERMISSIONS and action in AVAILABLE_PERMISSIONS[resource]:
                valid_permissions.append(permission)
        except ValueError:
            continue

    if not valid_permissions:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid permissions provided"
        )

    return valid_permissions