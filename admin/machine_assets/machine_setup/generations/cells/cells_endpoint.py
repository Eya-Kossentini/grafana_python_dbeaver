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
    prefix="/cells",
    tags=["cells"]
)

class CellsCreate(BaseModel):
    id: int
    name: str
    site_id: int
    user_id: int
    info: str
    is_active: bool
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/cells/")
def create_cells(payload: CellsCreate):
    return {
        "message": "cells received",
        "data": payload
    }
    
    


# ✅ GET depuis PostgreSQL
@router.get("/cells/")
def get_cells():
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                name,
                description,
                site_id,
                user_id,
                info,
                is_active
            FROM staging.cells
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
            "site_id": r[3],
            "user_id": r[4],
            "info": r[5],
            "is_active": r[6]
        }
        for r in rows
    ]