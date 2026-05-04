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
    prefix="/bookings",
    tags=["Bookings"]
)

class BookingCreate(BaseModel):
    id: int
    workorder_id: int
    station_id: int
    failed_id: Optional[int] = None
    serial_number_id: Optional[int] = None
    process_layer: Optional[int] = None
    date_of_booking: datetime
    state: str
    mesure_id: Optional[int] = None
    real_cycle_time: Optional[float] = None
    type: Optional[str] = None
    snr_booking: Optional[bool] = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/bookings/")
def create_booking(payload: BookingCreate):
    return {
        "message": "booking received",
        "data": payload
    }
    
    


# ✅ GET depuis PostgreSQL
@router.get("/bookings/")
def get_bookings(limit: int = 10000): 
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                workorder_id,
                station_id,
                state,
                date_of_booking,
                real_cycle_time
            FROM staging.bookings
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
            "date_of_booking": r[4],
            "real_cycle_time": float(r[5]) if r[5] else None
        }
        for r in rows
    ]