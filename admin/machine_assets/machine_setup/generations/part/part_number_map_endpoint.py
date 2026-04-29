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
    prefix="/part_number_map",
    tags=["part_number_map"]
)

class PartNumberMapCreate(BaseModel):
    id: int
    part_number: str
    description: str
    part_type_id: int
    part_group_id: int
    machine_group_id: int
    site_id: int
    unit_id: int
    customer_material_number: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/part_number_map/")
def create_part_number_map(payload: PartNumberMapCreate):
    return {
        "message": "part_number_map received",
        "data": payload
    }
    
    
# ✅ GET depuis PostgreSQL
@router.get("/part_number_map/")
def get_part_number_map():
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
            id,
            part_number,
            description,
            part_type_id,
            part_group_id,
            machine_group_id,
            site_id,
            unit_id,
            customer_material_number
            FROM staging.part_number_map
            ORDER BY id DESC
            LIMIT 100;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": r[0],
            "part_number": r[1],
            "description": r[2],
            "part_type_id": r[3],
            "part_group_id": r[4],
            "machine_group_id" : r[5],
            "site_id": r[6],
            "unit_id": r[7],
            "customer_material_number": r[8]
        }
        for r in rows
    ]
