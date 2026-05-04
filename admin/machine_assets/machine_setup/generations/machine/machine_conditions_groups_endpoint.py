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
    prefix="/machine_condition_groups",
    tags=["machine_condition_groups"]
)

class MachineConditionsGroupsCreate(BaseModel):
    id: int
    group_name: str
    group_description: str
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/machine_condition_groups/")
def create_machine_condition_groups(payload: MachineConditionsGroupsCreate):
    return {
        "message": "machine_condition_groups received",
        "data": payload
    }
    

# ✅ GET depuis PostgreSQL
@router.get("/machine_condition_groups/")
def get_machine_condition_groups(limit: int = 10000): 
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                group_name,
                group_description,
                is_active
            FROM staging.machine_condition_groups
            ORDER BY id DESC;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": r[0],
            "group_name": r[1],
            "group_description": r[2],
            "is_active": r[3]
        }
        for r in rows
    ]