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
    prefix="/workplans",
    tags=["workplans"]
)

class WorkplansCreate(BaseModel):
    id: int
    version: int
    is_current: bool
    user_id: int
    site_id: int
    client_id: int
    company_id: int
    source: int
    status: int
    product_vers_id: int
    workplan_status: str
    part_no: str
    part_desc: str
    workplan_desc: str
    workplan_type: str
    workplan_version_erp: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@router.post("/workplans/")
def create_workplans(payload: WorkplansCreate):
    return {
        "message": "Workplans received",
        "data": payload
    }


# ✅ GET depuis PostgreSQL
@router.get("/workplans/")
def get_workplans(limit: int = 10000): 
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                version,
                is_current,
                user_id,
                site_id,
                client_id,
                company_id,
                source,
                status,
                product_vers_id,
                workplan_status,
                part_no,
                part_desc,
                workplan_desc,
                workplan_type,
                workplan_version_erp
            FROM staging.workplans
            ORDER BY id DESC;
        """)

        rows = cur.fetchall()

    conn.close()
    
    return [
    {
        "id": r[0],
        "version": r[1],
        "is_current": r[2],
        "user_id": r[3],
        "site_id": r[4],
        "client_id": r[5],
        "company_id": r[6],
        "source": r[7],
        "status": r[8],
        "product_vers_id": r[9],
        "workplan_status": r[10],
        "part_no": r[11],
        "part_desc": r[12],
        "workplan_desc": r[13],
        "workplan_type": r[14],
        "workplan_version_erp": r[15]
    }
    for r in rows
    ]