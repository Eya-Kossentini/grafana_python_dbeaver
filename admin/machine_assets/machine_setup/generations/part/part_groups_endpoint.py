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
    prefix="/part_groups",
    tags=["part_groups"]
)

class PartGroupsCreate(BaseModel):
    id: int
    name: str
    description: str
    user_id: int
    part_type: str
    part_group_type_id: int
    costs: int =0
    circulating_lot: int=0
    is_active: bool = True
    state: int
    automatic_emptying: int
    master_workplan: str
    comment: str
    material_transfer: bool = False
    created_on: datetime
    edited_on: datetime
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/part_groups/")
def create_part_groups(payload: PartGroupsCreate):
    return {
        "message": "part_groups received",
        "data": payload
    }
    
    
# ✅ GET depuis PostgreSQL
@router.get("/part_groups/")
def get_part_groups():
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
              id,
              name,
              description,
              user_id,
              part_type,
              part_group_type_id,
              costs,
              circulating_lot,
              is_active,
              state,
              automatic_emptying,
              master_workplan,
              comment,
              material_transfer,
              created_on,
              edited_on
            FROM staging.part_groups
            ORDER BY id DESC
            LIMIT 100;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": r[0],
            "name": r[1],
            "description": r[2],
            "user_id": r[3],
            "part_type": r[4],
            "part_group_type_id" : r[5],
            "costs": r[6],
            "circulating_lot": r[7],
            "is_active": r[8],
            "state": r[9],
            "automatic_emptying": r[10],
            "master_workplan": r[11],
            "comment": r[12],
            "material_transfer": r[13],
            "created_on": r[14],
            "edited_on": r[15]
            
        }
        for r in rows
    ]
