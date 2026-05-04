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
    prefix="/failure_types",
    tags=["failure_types"]
)

class FailureTypesCreate(BaseModel):
    failure_type_id: int
    failure_type_code: str
    failure_type_desc: str
    site_id: int
    failure_group_id: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/failure_types/")
def create_failure_types(payload: FailureTypesCreate):
    return {
        "message": "Failure Types received",
        "data": payload
    }


# ✅ GET depuis PostgreSQL
@router.get("/failure_types/")
def get_failure_types(limit: int = 10000): 
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                failure_type_id,
                failure_type_code,
                failure_type_desc,
                site_id,
                failure_group_id
            FROM staging.failure_types
            ORDER BY failure_type_id DESC;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "failure_type_id": r[0],
            "failure_type_code": r[1],
            "failure_type_desc": r[2],
            "site_id": r[3],
            "failure_group_id": r[4]
        }
        for r in rows
    ]