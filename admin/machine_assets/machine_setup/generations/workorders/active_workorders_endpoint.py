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
    prefix="/active_workorders",
    tags=["active_workorders"]
)

class ActiveWorkordersCreate(BaseModel):
    id: int
    workorder_id : int
    station_id: int
    state: int
    process_layer: int 
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/active_workorders/")
def create_active_workorders(payload: ActiveWorkordersCreate):
    return {
        "message": "active workorders received",
        "data": payload
    }


# ✅ GET depuis PostgreSQL
@router.get("/active_workorders/")
def get_active_workorders(limit: int = 10000): 
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                workorder_id,
                station_id,
                state, 
                process_layer
            FROM staging.active_workorders
            ORDER BY id DESC;
        """)

        rows = cur.fetchall()

    conn.close()
    
    return [
        {
            "id": r[0],
            "workorder_id": r[1],
            "station_id": r[2],
            "state": r[3],
            "process_layer": r[4]
        }
    for r in rows
    ]