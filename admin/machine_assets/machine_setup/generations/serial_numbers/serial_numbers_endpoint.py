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
    prefix="/serial_numbers",
    tags=["serial_numbers"]
)

class SerialNumbersCreate(BaseModel):
    id: int
    serial_number :  str
    serial_number_pos: int
    serial_number_ref_pos: int
    serial_number_active: int 
    serial_number_ref : str
    splitted: bool = False
    workorder_id: int
    part_id: int
    customer_part_number : str
    workorder_type: str
    serial_number_type: str
    cluster_name : str
    cluster_type: str
    created_on: datetime
    created_by: int
    company_code_id : int
    created_at: Optional[datetime] = None


@router.post("/serial_numbers/")
def create_serial_numbers(payload: SerialNumbersCreate):
    return {
        "message": "serial numbers received",
        "data": payload
    }


# ✅ GET depuis PostgreSQL
@router.get("/serial_numbers/")
def get_serial_numbers(limit: int = 10000): 
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                serial_number,
                serial_number_pos,
                serial_number_ref_pos,
                serial_number_active,
                serial_number_ref,
                splitted,
                workorder_id,
                part_id,
                customer_part_number ,
                workorder_type,
                serial_number_type,
                cluster_name,
                cluster_type,
                created_on,
                created_by,
                company_code_id
            FROM staging.serial_numbers
            ORDER BY id DESC;
        """)

        rows = cur.fetchall()

    conn.close()
    
    return [
        {
            "id": r[0],
            "serial_number": r[1],
            "serial_number_pos": r[2],
            "serial_number_ref_pos": r[3],
            "serial_number_active": r[4],
            "serial_number_ref": r[5],
            "splitted": r[6],
            "workorder_id": r[7],
            "part_id": r[8],
            "customer_part_number": r[9],
            "workorder_type": r[10],
            "serial_number_type": r[11],
            "cluster_name": r[12],
            "cluster_type": r[13],
            "created_on": r[14],
            "created_by": r[15],
            "company_code_id": r[16]
        }
    for r in rows
    ]