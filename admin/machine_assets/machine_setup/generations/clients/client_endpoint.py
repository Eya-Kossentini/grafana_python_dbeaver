from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import psycopg2

# 🔌 Connexion PostgreSQL
def get_connection():
    return psycopg2.connect(
        host="127.0.0.1",
        port=5435,
        dbname="postgres",
        user="postgres",
        password="admin123"
    )
    
router = APIRouter(
    prefix="/clients",
    tags=["clients"]
)

class ClientCreate(BaseModel):
    id: int
    user_id: int
    name: str
    descritpion: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/client/")
def create_client(payload: ClientCreate):
    return {
        "message": "client received",
        "data": payload
    }
    
    


# ✅ GET depuis PostgreSQL
@router.get("/client/")
def get_clients(limit: int = 10000): 
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                user_id,
                name,
                description
            FROM staging.clients
            ORDER BY id DESC;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": r[0],
            "user_id": r[1],
            "name": r[2],
            "description": r[3]
        }
        for r in rows
    ]