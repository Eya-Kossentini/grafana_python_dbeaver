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
    prefix="/part_group_type",
    tags=["part_group_type"]
)

class PartGroupTypeCreate(BaseModel):
    id: int
    name: str
    description: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/part_group_type/")
def create_part_group_type(payload: PartGroupTypeCreate):
    return {
        "message": "part_group_type received",
        "data": payload
    }
    
    
# ✅ GET depuis PostgreSQL
@router.get("/part_group_type/")
def get_part_group_type():
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                name,
                description
            FROM staging.part_group_types
            ORDER BY id DESC
            LIMIT 100;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": r[0],
            "name": r[1],
            "description": r[2]
        }
        for r in rows
    ]
