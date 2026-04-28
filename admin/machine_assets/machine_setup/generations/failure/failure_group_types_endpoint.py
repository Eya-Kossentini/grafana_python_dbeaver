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
    prefix="/failure_group_types",
    tags=["failure_group_types"]
)

class FailureGroupTypesCreate(BaseModel):
    id: int
    failure_group_name: str
    failure_group_desc: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/failure_group_types/")
def create_failure_group_types(payload: FailureGroupTypesCreate):
    return {
        "message": "Failure Group Types received",
        "data": payload
    }


# ✅ GET depuis PostgreSQL
@router.get("/failure_group_types/")
def get_failure_group_types():
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                failure_group_name,
                failure_group_desc
            FROM staging.failure_group_types
            ORDER BY id DESC
            LIMIT 100;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": r[0],
            "failure_group_name": r[1],
            "failure_group_desc": r[2]
        }
        for r in rows
    ]