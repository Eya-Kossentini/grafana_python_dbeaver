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
    prefix="/lines",
    tags=["lines"]
)

class LinesCreate(BaseModel):
    id: int
    name: str
    description: str
    date: datetime
    user_id: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/lines/")
def create_lines(payload: LinesCreate):
    return {
        "message": "lines received",
        "data": payload
    }
    
    


# ✅ GET depuis PostgreSQL
@router.get("/lines/")
def get_lines(limit: int = 10000): 
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                name,
                description,
                date,
                user_id
            FROM staging.lines
            ORDER BY id DESC;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": r[0],
            "name": r[1],
            "description": r[2],
            "date": r[3],
            "user_id": r[4]
        }
        for r in rows
    ]
