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
    prefix="/part_type",
    tags=["lipart_typenes"]
)

class PartTypeCreate(BaseModel):
    id: int
    name: str
    description: str
    user_id: int
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/part_type/")
def create_part_type(payload: PartTypeCreate):
    return {
        "message": "part_type received",
        "data": payload
    }
    
    
# ✅ GET depuis PostgreSQL
@router.get("/part_type/")
def get_part_type(limit: int = 10000): 
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                name,
                description,
                user_id,
                is_active
            FROM staging.part_types
            ORDER BY id DESC;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": r[0],
            "name": r[1],
            "description": r[2],
            "user_id": r[3],
            "is_active": r[4]
        }
        for r in rows
    ]
