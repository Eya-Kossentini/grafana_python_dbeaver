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
    prefix="/bom_headers",
    tags=["bom_headers"]
)

class BomHeadersCreate(BaseModel):
    id: int
    description: str
    valid_from: datetime
    valid_to: datetime
    part_master_id: int
    created_by: str
    updated_by: str 
    state: str 
    version: int
    is_current: bool
    previous_version_id: int
    created_at: Optional[datetime] = None


@router.post("/bom_headers/")
def create_bom_headers(payload: BomHeadersCreate):
    return {
        "message": "bom_headers received",
        "data": payload
    }


# ✅ GET depuis PostgreSQL
@router.get("/bom_headers/")
def get_bom_headers():
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                description,
                valid_from,
                valid_to,
                part_master_id,
                created_by,
                updated_by, 
                state, 
                version,
                is_current,
                previous_version_id                
            FROM staging.bom_headers
            ORDER BY id DESC
            LIMIT 100;
        """)

        rows = cur.fetchall()

    conn.close()
    
    return [
    {
        "id": r[0],
        "description": r[1],
        "valid_from": r[2],
        "valid_to": r[3],
        "part_master_id": r[4],
        "created_by": r[5],
        "updated_by":  r[6],
        "state":  r[7],
        "version": r[8],
        "is_current": r[9],
        "previous_version_id": r[10]
    }
    for r in rows
    ]