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
    prefix="/line_station",
    tags=["line_station"]
)

class LineStationCreate(BaseModel):
    line_id: int
    station_id: int


@router.post("/line_station/")
def create_line_station(payload: LineStationCreate):
    return {
        "message": "line_station received",
        "data": payload
    }
    
    


# ✅ GET depuis PostgreSQL
@router.get("/line_station/")
def get_line_station():
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                line_id,
                station_id
            FROM staging.line_stations;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "line_id": r[0],
            "station_id": r[1]
        }
        for r in rows
    ]
