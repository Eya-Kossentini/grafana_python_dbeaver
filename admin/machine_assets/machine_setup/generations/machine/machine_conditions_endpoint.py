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
    prefix="/machine_conditions",
    tags=["machine_conditions"]
)

class MachineConditionsCreate(BaseModel):
    id: int
    group_id: int
    condition_name: str
    condition_description: str
    color_rgb: str
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/machine_conditions/")
def create_machine_conditions (payload: MachineConditionsCreate):
    return {
        "message": "machine_conditions received",
        "data": payload
    }
    
    


# ✅ GET depuis PostgreSQL
@router.get("/machine_conditions/")
def get_machine_conditions ():
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                group_id,
                condition_name,
                condition_description,
                color_rgb,
                is_active
            FROM staging.machine_conditions 
            ORDER BY id DESC
            LIMIT 100;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": r[0],
            "group_id": r[1],
            "condition_name": r[2],
            "condition_description": r[3],
            "color_rgb": r[4],
            "is_active": r[5]
        }
        for r in rows
    ]