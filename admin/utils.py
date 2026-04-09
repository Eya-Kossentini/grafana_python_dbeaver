"""Authentication utilities."""
from typing import Optional, Dict, Any
from jose import jwt

from admin.config import SECRET_KEY, ALGORITHM

def verify_token(token: str) -> Optional[Dict[str, Any]]:
    """Verify a JWT token and return the payload."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.JWTError:
        return None 