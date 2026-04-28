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
    prefix="/machine_groups",
    tags=["machine_groups"]
)

class MachineGroupsCreate(BaseModel):
    id: int
    name: str
    description: str
    user_id: int
    cell_id: int
    is_active: bool = True
    failure: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/machine_groups/")
def create_machine_groups(payload: MachineGroupsCreate):
    return {
        "message": "machine_groups received",
        "data": payload
    }
    
    


# ✅ GET depuis PostgreSQL
@router.get("/machine_groups/")
def get_machine_groups():
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                name,
                description,
                user_id,
                cell_id,
                is_active,
                failure
            FROM staging.machine_groups
            ORDER BY id DESC
            LIMIT 100;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": r[0],
            "name": r[1],
            "description": r[2],
            "user_id": r[3],
            "cell_id": r[4],
            "is_active": r[5],
            "failure": r[6]
        }
        for r in rows
    ]