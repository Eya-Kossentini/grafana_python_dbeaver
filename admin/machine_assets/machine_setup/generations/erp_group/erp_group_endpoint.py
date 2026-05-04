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
    prefix="/erp_groups",
    tags=["erp_groups"]
)

class ERPGroupCreate(BaseModel):
    id: int
    state: int
    erpgroup_no: str
    erp_group_description: str
    erpsystem: str
    sequentiel: bool
    seperate_station: bool
    fixed_layer: bool
    created_on: datetime
    edited_on: datetime
    modified_by: int
    user_id: int
    cst_id: int
    valid: bool= True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/erp_groups/")
def create_erp_groups(payload: ERPGroupCreate):
    return {
        "message": "ERP Groups received",
        "data": payload
    }


# ✅ GET depuis PostgreSQL
@router.get("/erp_groups/")
def get_erp_groups(limit: int = 10000): 
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                state,
                erpgroup_no,
                erp_group_description,
                erpsystem,
                erpsystem,
                separate_station,
                fixed_layer,
                created_on,
                edited_on,
                modified_by,
                user_id,
                cst_id,
                valid
            FROM staging.erp_groups
            ORDER BY id DESC;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": r[0],
            "state": r[1],
            "erpgroup_no": r[2],
            "erp_group_description": r[3],
            "erpsystem": r[4],
            "sequential": r[5],
            "separate_station": r[6],
            "fixed_layer": r[7],
            "created_on": r[8],
            "edited_on": r[9],
            "modified_by": r[10],
            "user_id": r[11],
            "cst_id": r[12],
            "valid": r[13]
        }
        for r in rows
    ]