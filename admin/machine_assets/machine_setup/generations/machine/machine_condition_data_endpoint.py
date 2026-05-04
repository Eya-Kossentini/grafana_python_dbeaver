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
    prefix="/machine_condition_data",
    tags=["machine_condition_data"]
)

class MachineConditionsDataCreate(BaseModel):
    id: int
    date_from: datetime
    date_to: datetime
    station_id: int
    condition_id: int
    level: str
    condition_stamp: datetime
    condition_type: str
    color_rgb: str
    condition_created: datetime
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/machine_condition_data/")
def create_machine_condition_data (payload: MachineConditionsDataCreate):
    return {
        "message": "machine_conditionsç_data  received",
        "data": payload
    }
    
    


# ✅ GET depuis PostgreSQL
@router.get("/machine_condition_data/")
def get_machine_condition_data (limit: int = 10000): 
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
            id,
            date_from,
            date_to,
            station_id,
            condition_id,
            level,
            condition_stamp,
            condition_type,
            color_rgb,
            condition_created
            FROM staging.machine_condition_data
            ORDER BY id DESC;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": r[0],
            "date_from": r[1],
            "date_to": r[2],
            "station_id": r[3],
            "condition_id": r[4],
            "level": r[5],
            "condition_stamp": r[6],
            "condition_type": r[7],
            "color_rgb": r[8],
            "condition_created": r[9]
        }
        for r in rows
    ]