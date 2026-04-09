"""Core authentication utilities."""
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from jose import JWTError, jwt
from dependency_injector.wiring import inject, Provide
from fastapi import Depends
from auth_client.permission import expand_permission_groups
from auth_client.utils import verify_token

# Configuration
SECRET_KEY = "your-secret-key-here"  # Must match auth_service
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

class AuthService:
    """Service for handling authentication operations."""
    
    def __init__(self, db):
        self.db = db

    @classmethod
    def get_instance(cls, db) -> 'AuthService':
        """Get an instance of AuthService with injected dependencies."""
        return cls(db=db)

    def verify_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Verify a JWT token and return the payload."""
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            return payload
        except JWTError:
            return None

    def get_token_data(self, token: str) -> Optional[Dict[str, Any]]:
        """Get token data including permissions."""
        payload = self.verify_token(token)
        if not payload:
            return None
        
        direct_permissions = payload.get("permissions", [])
        permission_groups = payload.get("permission_groups", [])
        expanded_group_permissions = list(expand_permission_groups(permission_groups))
        merged_permissions = list({*direct_permissions, *expanded_group_permissions})

        return {
            "email": payload.get("sub"),
            "role": payload.get("role", "user"),
            "permissions": merged_permissions,
            "permission_groups": permission_groups,
        }