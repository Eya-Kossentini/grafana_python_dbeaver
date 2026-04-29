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
    prefix="/measurement_data",
    tags=["measurement_data"]
)

class MeasurementDataCreate(BaseModel):
    id: int
    station_id: int
    workorder_id: int
    book_date: datetime
    measure_name: str
    measure_value: int
    lower_limit: int
    upper_limit: int
    nominal: int
    tolerance:int
    measure_fail_code:int
    measure_type: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/measurement_data /")
def create_measurement_data (payload: MeasurementDataCreate):
    return {
        "message": "measurement_data  received",
        "data": payload
    }
    
    


# ✅ GET depuis PostgreSQL
@router.get("/measurement_data /")
def get_measurement_data():
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                station_id,
                workorder_id,
                book_date,
                measure_name,
                measure_value,
                lower_limit,
                upper_limit,
                nominal,
                tolerance,
                measure_fail_code,
                measure_type
            FROM staging.measurement_data 
            ORDER BY id DESC
            LIMIT 100;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": r[0],
            "station_id": r[1],
            "workorder_id": r[2],
            "book_date": r[3],
            "measure_name": r[4],
            "measure_value": r[5],
            "lower_limit": r[6],
            "upper_limit": r[7],
            "nominal": r[8],
            "tolerance": r[9],
            "measure_fail_code": r[10],
            "measure_type": r[11]
        }
        for r in rows
    ]