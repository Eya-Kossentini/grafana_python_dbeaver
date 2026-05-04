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
    prefix="/sites",
    tags=["sites"]
)

class SiteCreate(BaseModel):
    id: int
    user_id: int
    company_code_id: int
    site_number: str
    site_external_number: str
    deletion_priority: str
    geo_coordinates: str
    description: str
    created_at: Optional[datetime] = None


@router.post("/sites/")
def create_site(payload: SiteCreate):
    return {
        "message": "site received",
        "data": payload
    }
    
    


# ✅ GET depuis PostgreSQL
@router.get("/sites/")
def get_sites(limit: int = 10000): 
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                user_id,
                company_code_id,
                site_number,
                site_external_number,
                deletion_priority,
                geo_coordinates,
                description
            FROM staging.sites
            ORDER BY id DESC;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": r[0],
            "user_id": r[1],
            "company_code_id": r[2],
            "site_number": r[3],
            "site_external_number": r[4],
            "deletion_priority": r[5],
            "geo_coordinates": r[6],
            "description": r[7]
        }
        for r in rows
    ]