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
    prefix="/stations",
    tags=["stations"]
)

class StationsCreate(BaseModel):
    id: int
    machine_group_id: int
    name: str
    description: str
    is_active: bool = True
    user_id: int
    info: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/stations/")
def create_stations(payload: StationsCreate):
    return {
        "message": "stations received",
        "data": payload
    }
    
    


# ✅ GET depuis PostgreSQL
@router.get("/stations/")
def get_stations(limit: int = 10000): 
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                machine_group_id,
                name,
                description,
                is_active,
                user_id,
                info
            FROM staging.stations
            ORDER BY id DESC;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": r[0],
            "machine_group_id": r[1],
            "name": r[2],
            "description": r[3],
            "is_active": r[4],
            "user_id": r[5],
            "info": r[6],
        }
        for r in rows
    ]
