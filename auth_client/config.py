"""Authentication configuration."""
import os

# JWT Settings - Must match auth_service settings
SECRET_KEY = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"  # Must be the same as auth_service
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Auth Service URL
AUTH_SERVICE_URL = os.getenv("AUTH_API_URL", "https://auth_demo.momes-solutions.com/")
#AUTH_SERVICE_URL = "http://192.168.1.140:8016/"
TOKEN_URL = f"{AUTH_SERVICE_URL}auth/token" 