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
    prefix="/worksteps",
    tags=["worksteps"]
)

class WorkstepsCreate(BaseModel):
    id: int
    workplan_id: int
    erp_group_id: int
    workstep_no: int
    step: int
    setup_time: int
    te_person: int
    te_machine: int
    te_time_base: int
    te_qty_base: int
    transport_time: int
    wait_time: int
    status: int
    panel_count: int
    workstep_desc: str
    erp_grp_no: str
    erp_grp_desc: str
    time_unit: str
    setup_flag: str
    workstep_version_erp:str
    info: str
    confirmation: str
    sequentiell: str
    workstep_type: str
    traceflag : str
    step_type: str
    created_at: Optional[datetime] = None
    stamp: datetime


@router.post("/worksteps/")
def create_worksteps(payload: WorkstepsCreate):
    return {
        "message": "worksteps received",
        "data": payload
    }


# ✅ GET depuis PostgreSQL
@router.get("/worksteps/")
def get_worksteps():
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                workplan_id,
                erp_group_id,
                workstep_no,
                step,
                setup_time,
                te_person,
                te_machine,
                te_time_base,
                te_qty_base,
                transport_time,
                wait_time,
                status,
                panel_count,
                workstep_desc,
                erp_grp_no,
                erp_grp_desc,
                time_unit,
                setup_flag,
                workstep_version_erp,
                info,
                confirmation,
                sequentiell,
                workstep_type,
                traceflag,
                step_type,
                stamp
            FROM staging.worksteps
            ORDER BY id DESC
            LIMIT 100;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "id": r[0],
            "workplan_id": r[1],
            "erp_group_id": r[2],
            "workstep_no": r[3],
            "step": r[4],
            "setup_time": float(r[5]) if r[5] is not None else None,
            "te_person": float(r[6]) if r[6] is not None else None,
            "te_machine": float(r[7]) if r[7] is not None else None,
            "te_time_base": float(r[8]) if r[8] is not None else None,
            "te_qty_base": float(r[9]) if r[9] is not None else None,
            "transport_time": float(r[10]) if r[10] is not None else None,
            "wait_time": float(r[11]) if r[11] is not None else None,
            "status": r[12],
            "panel_count": r[13],
            "workstep_desc": r[14],
            "erp_grp_no": r[15],
            "erp_grp_desc": r[16],
            "time_unit": r[17],
            "setup_flag": r[18],
            "workstep_version_erp": r[19],
            "info": r[20],
            "confirmation": r[21],
            "sequentiell": r[22],
            "workstep_type": r[23],
            "traceflag": r[24],
            "step_type": r[25],
            "stamp": r[26],
        }
        for r in rows
    ]