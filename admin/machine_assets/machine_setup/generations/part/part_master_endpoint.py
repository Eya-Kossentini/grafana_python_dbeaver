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
    prefix="/part_master",
    tags=["part_master"]
)

class PartMasterCreate(BaseModel):
    id: int
    part_number: str
    description: str
    part_status: str
    parttype_id: int
    partgroup_id: int
    case_type: str
    product: bool = False
    panel: bool = False
    variant: bool = False
    machine_group_id: int
    material_info: str
    parts_index: int
    edit_order_based_bom: bool = False
    site_id: int
    unit_id: int
    material_code: str
    no_of_panels: int
    customer_material_number: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/part_master/")
def create_part_master(payload: PartMasterCreate):
    return {
        "message": "part_master received",
        "data": payload
    }
    
    
# ✅ GET depuis PostgreSQL
@router.get("/part_master/")
def get_part_master():
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
            id,
            part_number,
            description,
            part_status,
            parttype_id,
            partgroup_id,
            case_type,
            product,
            panel,
            variant,
            machine_group_id,
            material_info,
            parts_index,
            edit_order_based_bom,
            site_id,
            unit_id,
            material_code,
            no_of_panels,
            customer_material_number
            FROM staging.part_master
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
            "part_status": r[3],
            "parttype_id": r[4],
            "partgroup_id" : r[5],
            "case_type": r[6],
            "product": r[7],
            "panel": r[8],
            "variant": r[9],
            "machine_group_id": r[10],
            "material_info": r[11],
            "parts_index": r[12],
            "edit_order_based_bom": r[13],
            "site_id": r[14],
            "unit_id": r[15],
            "material_code": r[16],
            "no_of_panels": r[17],
            "customer_material_number": r[18]
        }
        for r in rows
    ]
