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
    prefix="/work_orders",
    tags=["work_orders"]
)

class WorkordersCreate(BaseModel):
    id: int
    workorder_no : str
    workorder_type: str
    part_number: str
    workorder_qty: int
    startdate: datetime
    deliverydate: datetime
    unit: str
    bom_version: int
    workplan_type: str
    backflush: int
    source: int
    workplan_version: int
    workorder_desc: str
    workplan_valid_from: datetime
    status: str
    site_id: int
    client_id: int
    company_id: int
    workorder_state: str
    aps_planning_start_date : datetime 
    aps_planning_stamp : datetime      
    aps_planning_end_date : datetime    
    aps_order_fixation : datetime      
    created_at: Optional[datetime] = None


@router.post("/work_orders/")
def create_work_orders(payload: WorkordersCreate):
    return {
        "message": "workorders received",
        "data": payload
    }


# ✅ GET depuis PostgreSQL
@router.get("/work_orders/")
def get_work_orders(limit: int = 10000): 
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                workorder_no,
                workorder_type,
                part_number, 
                workorder_qty,
                startdate,
                deliverydate,
                unit,
                bom_version,
                workplan_type,
                backflush,
                source,
                workplan_version,
                workorder_desc,
                workplan_valid_from,
                status,
                site_id,
                client_id,
                company_id,
                workorder_state,
                aps_planning_start_date,
                aps_planning_stamp,    
                aps_planning_end_date,   
                aps_order_fixation   
            FROM staging.work_orders
            ORDER BY id DESC;
        """)

        rows = cur.fetchall()

    conn.close()
    
    return [
        {
            "id": r[0],
            "workorder_no": r[1],
            "workorder_type": r[2],
            "part_number": r[3],
            "workorder_qty": r[4],
            "startdate": r[5],
            "deliverydate": r[6],
            "unit": r[7],
            "bom_version": r[8],
            "workplan_type": r[9],
            "backflush": r[10],
            "source": r[11],
            "workplan_version": r[12],
            "workorder_desc": r[13],
            "workplan_valid_from": r[14],
            "status": r[15],
            "site_id": r[16],
            "client_id": r[17],
            "company_id": r[18],
            "workorder_state": r[19],
            "aps_planning_start_date": r[20],
            "aps_planning_stamp": r[21],
            "aps_planning_end_date": r[22],
            "aps_order_fixation": r[23]
        }
    for r in rows
    ]