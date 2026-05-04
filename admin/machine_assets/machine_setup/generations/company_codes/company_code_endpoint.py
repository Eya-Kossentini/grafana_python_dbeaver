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
    prefix="/company_codes",
    tags=["company_codes"]
)

class CompanyCodeCreate(BaseModel):
    id: int
    user_id : int
    client_id: int
    name: str
    description: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/company_codes/")
def create_company_codes(payload: CompanyCodeCreate):
    return {
        "message": "company_codes received",
        "data": payload
    }
    
    


# ✅ GET depuis PostgreSQL
@router.get("/company_codes/")
def get_company_codes(limit: int = 10000): 
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                user_id,
                client_id,
                name,
                description
            FROM staging.company_codes
            ORDER BY id DESC;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": r[0],
            "user_id": r[1],
            "client_id": r[2],
            "name": r[3],
            "description": r[4]
        }
        for r in rows
    ]