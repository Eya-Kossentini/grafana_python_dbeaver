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
    prefix="/assign_stations_to_erpgrp",
    tags=["assign_stations_to_erpgrp"]
)

class assign_stations_to_erpgrp_Create(BaseModel):
    station_id: int
    erp_group_id: int
    station_type: str
    user_id: int


@router.post("/assign_stations_to_erpgrp/")
def create_assign_stations_to_erpgrp(payload: assign_stations_to_erpgrp_Create):
    return {
        "message": "assign stations to erpgrp received",
        "data": payload
    }


# ✅ GET depuis PostgreSQL
@router.get("/assign_stations_to_erpgrp/")
def get_assign_stations_to_erpgrp(limit: int = 10000): 
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                station_id,
                erp_group_id,
                station_type,
                user_id
            FROM staging.assign_stations_to_erpgrp
            ORDER BY station_id DESC;
        """)

        rows = cur.fetchall()

    conn.close()

    return [
        {
            "station_id": r[0],
            "erp_group_id": r[1],
            "station_type": r[2],
            "user_id": r[3]
        }
        for r in rows
    ]