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
    prefix="/bom_items",
    tags=["bom_items"]
)

class BomItemsCreate(BaseModel):
    id: int
    bom_header_id: int
    part_master_id: int
    quantity: int
    is_product: bool =False
    component_name: str
    layer: int
    created_at: Optional[datetime] = None


@router.post("/bom_items/")
def create_bom_items(payload: BomItemsCreate):
    return {
        "message": "bom_items received",
        "data": payload
    }


# ✅ GET depuis PostgreSQL
@router.get("/bom_items/")
def get_bom_items():
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                bom_header_id,
                part_master_id,
                quantity, 
                is_product,
                component_name,
                layer                
            FROM staging.bom_items
            ORDER BY id DESC
            LIMIT 100;
        """)

        rows = cur.fetchall()

    conn.close()
    
    return [
    {
        "id": r[0],
        "bom_header_id": r[1],
        "part_master_id": r[2],
        "quantity": r[3],
        "is_product":  r[4],
        "component_name":  r[5],
        "layer": r[6]
    }
    for r in rows
    ]