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
    prefix="/workplan_types",
    tags=["workplan_types"]
)

class WorkplansTypesCreate(BaseModel):
    id: int
    name: str
    description: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/workplan_types/")
def create_workplan_types(payload: WorkplansTypesCreate):
    return {
        "message": "workplantypes received",
        "data": payload
    }


# ✅ GET depuis PostgreSQL
@router.get("/workplan_types/")
def get_workplan_types(limit: int = 10000): 
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                name,
                description
            FROM staging.workplan_types
            ORDER BY id DESC;
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