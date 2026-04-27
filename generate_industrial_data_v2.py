from __future__ import annotations

import argparse
import calendar
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable, cast
import psycopg2 as psycopg
import requests
from typing import Optional
from postgres_writer_v2 import PgConfig, PostgresWriter
from dotenv import load_dotenv
import re

BASE_URL ="https://core_demo.momes-solutions.com"
API_TOKEN = os.getenv("API_TOKEN")

POST_BASE_URL = "http://127.0.0.1:8000"

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def _clean_token(token: Optional[str]) -> str:
    if not token:
        raise ValueError("Token API manquant.")
    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)
    return token

def _auth_headers(token: Optional[str]) -> dict[str, str]:
    token = _clean_token(token)
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    
def _get_json_or_text(response: requests.Response) -> dict[str, Any]:
    try:
        data = response.json()
        if isinstance(data, dict):
            return data
        return {"raw": data}
    except Exception:
        return {"detail": response.text}


def _get_existing_id_by_get(
    endpoint: str,
    token: Optional[str],
    *,
    params: Optional[dict[str, Any]] = None,
    name_key_candidates: tuple[str, ...] = ("name",),
    wanted_value: Optional[str] = None,
    id_key: str = "id",
) -> Optional[int]:
    """
    Fallback générique:
    fait un GET sur un endpoint liste et cherche l'ID de l'élément voulu.
    """
    headers = _auth_headers(token)
    r = requests.get(
        f"{BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}",
        headers=headers,
        params=params or {},
        timeout=30,
        verify=False,
    )

    if r.status_code != 200:
        return None

    try:
        payload = r.json()
    except Exception:
        return None

    items: list[dict[str, Any]] = []
    if isinstance(payload, list):
        items = [x for x in payload if isinstance(x, dict)]
    elif isinstance(payload, dict):
        for key in ("items", "results", "data"):
            if isinstance(payload.get(key), list):
                items = [x for x in payload[key] if isinstance(x, dict)]
                break
        else:
            if id_key in payload:
                items = [payload]

    if not wanted_value:
        return None

    for item in items:
        for key in name_key_candidates:
            if str(item.get(key, "")).strip() == wanted_value:
                try:
                    return int(item[id_key])
                except Exception:
                    pass

    return None

def _extract_id_from_detail(detail: str) -> Optional[int]:
    """
    Essaie d'extraire un ID depuis des messages du type:
    - "... already exists (ID: 3)"
    - "... already exists ID 3"
    """
    if not detail:
        return None

    patterns = [
        r"\(ID:\s*(\d+)\)",
        r"ID[:\s]+(\d+)",
        r"id[:\s]+(\d+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, detail)
        if match:
            return int(match.group(1))
    return None

# ---------------------------------------------------------------------------
# PCB domain constants
# ---------------------------------------------------------------------------

AVG_CYCLE_TIME_SEC: float = 240.0
SHIFTS: list[tuple[str, int, int]] = [
    ("Day",   6, 14),
    ("Aftn", 14, 22),
    ("Night", 22,  6),
]
SHIFT_DURATION_S: float = 8.0 * 3600.0

MEASUREMENT_CATALOG: list[dict[str, Any]] = [
    {"measure_name": "Paste Volume (%)",          "lower_limit": 75,   "upper_limit": 125,  "nominal": 100,  "tolerance": 25,   "measure_type": "SPI"},
    {"measure_name": "Paste Height (mm)",         "lower_limit": 0.10, "upper_limit": 0.18, "nominal": 0.14, "tolerance": 0.04, "measure_type": "SPI"},
    {"measure_name": "Paste Area (%)",            "lower_limit": 80,   "upper_limit": 120,  "nominal": 100,  "tolerance": 20,   "measure_type": "SPI"},
    {"measure_name": "Peak Temp (C)",             "lower_limit": 235,  "upper_limit": 250,  "nominal": 243,  "tolerance": 7,    "measure_type": "Thermal"},
    {"measure_name": "Time Above 217C (s)",       "lower_limit": 45,   "upper_limit": 90,   "nominal": 60,   "tolerance": 15,   "measure_type": "Thermal"},
    {"measure_name": "Preheat Ramp (C/s)",        "lower_limit": 1.0,  "upper_limit": 3.0,  "nominal": 2.0,  "tolerance": 0.5,  "measure_type": "Thermal"},
    {"measure_name": "Component Offset X (mm)",   "lower_limit": -0.20,"upper_limit": 0.20, "nominal": 0.0,  "tolerance": 0.10, "measure_type": "AOI"},
    {"measure_name": "Component Offset Y (mm)",   "lower_limit": -0.20,"upper_limit": 0.20, "nominal": 0.0,  "tolerance": 0.10, "measure_type": "AOI"},
    {"measure_name": "Rotation Error (deg)",      "lower_limit": -1.5, "upper_limit": 1.5,  "nominal": 0.0,  "tolerance": 0.5,  "measure_type": "AOI"},
    {"measure_name": "Supply Voltage (V)",        "lower_limit": 4.85, "upper_limit": 5.15, "nominal": 5.0,  "tolerance": 0.10, "measure_type": "ICT"},
    {"measure_name": "Resistance R1 (Ohm)",       "lower_limit": 95,   "upper_limit": 105,  "nominal": 100,  "tolerance": 5,    "measure_type": "ICT"},
    {"measure_name": "Capacitance C1 (nF)",       "lower_limit": 90,   "upper_limit": 110,  "nominal": 100,  "tolerance": 10,   "measure_type": "ICT"},
    {"measure_name": "Insulation Resistance (MOhm)","lower_limit": 100,"upper_limit": 1000, "nominal": 500,  "tolerance": 400,  "measure_type": "Functional"},
    {"measure_name": "Current Consumption (mA)",  "lower_limit": 45,   "upper_limit": 55,   "nominal": 50,   "tolerance": 5,    "measure_type": "Functional"},
]

ALLOWED_CONDITIONS: list[dict[str, Any]] = [
    {"code": "1000", "desc": "Minor Stoppages & Waiting",  "group_id": 1, "color": "#d6a624"},
    {"code": "1001", "desc": "Cleaning",                   "group_id": 1, "color": "#099f95"},
    {"code": "1002", "desc": "Rate Deviation & Others",    "group_id": 1, "color": "#d12323"},
    {"code": "2000", "desc": "Change Over & Setup",        "group_id": 2, "color": "#4940c9"},
    {"code": "2001", "desc": "Part Shortage",              "group_id": 2, "color": "#d544c8"},
    {"code": "2002", "desc": "Machine Breakdown",          "group_id": 2, "color": "#8b1818"},
    {"code": "3000", "desc": "Preventive Maintenance",     "group_id": 3, "color": "#e6d628"},
    {"code": "3001", "desc": "Inventory Check",            "group_id": 3, "color": "#4daeea"},
    {"code": "3002", "desc": "Fire Drills",                "group_id": 3, "color": "#c56767"},
    {"code": "3003", "desc": "Trial & Pilot Run",          "group_id": 3, "color": "#3a884e"},
    {"code": "3004", "desc": "Meeting",                    "group_id": 3, "color": "#ea7f06"},
    {"code": "3005", "desc": "No Production & Break",      "group_id": 3, "color": "#3e6eac"},
    {"code": "3006", "desc": "Running",                    "group_id": 3, "color": "#13be1e"},
]

PCB_PROCESS_STEPS = [
    ("SPI",     "Solder Paste Inspection",      "SMT"),
    ("PNP",     "Pick and Place",               "SMT"),
    ("REFLOW",  "Reflow Oven",                  "SMT"),
    ("AOI_TOP", "AOI Top Side",                 "SMT"),
    ("THT",     "Through Hole Assembly",        "THT"),
    ("WAVE",    "Wave Soldering",               "THT"),
    ("AOI_BOT", "AOI Bottom Side",              "THT"),
    ("ICT",     "In-Circuit Test",              "TEST"),
    ("FUNC",    "Functional Test",              "TEST"),
    ("XRAY",    "X-Ray Inspection",             "TEST"),
    ("PROG",    "Programming",                  "TEST"),
    ("CLEAN",   "Cleaning / Conformal Coating", "FINISH"),
    ("LASER",   "Laser Marking",                "FINISH"),
    ("PACK",    "Packaging & Label",            "FINISH"),
]

PCB_COMPONENT_TYPES = [
    ("R",  "Resistor",   "0402"),
    ("C",  "Capacitor",  "0603"),
    ("U",  "IC",         "SOIC-8"),
    ("Q",  "Transistor", "SOT-23"),
    ("D",  "Diode",      "SOD-123"),
    ("L",  "Inductor",   "0805"),
    ("J",  "Connector",  "THT"),
    ("Y",  "Crystal",    "HC-49"),
    ("SW", "Switch",     "THT"),
    ("F",  "Fuse",       "1206"),
]

# ---------------------------------------------------------------------------
# HARDCODED lines, stations, and associations from production DB exports
# ---------------------------------------------------------------------------
# These exact IDs and names must be used — do NOT generate new ones.

LINES_FIXED: list[dict[str, Any]] = [
    {"id": 21, "name": "ASS-L01"},
    {"id": 22, "name": "FRA-SMT-L01"},
    {"id": 23, "name": "FRA-SMT-L02"},
    {"id": 24, "name": "FRA-SMT-L03"},
    {"id": 25, "name": "FRA-SMT-L04"},
    {"id": 26, "name": "FRA-SMT-L05"},
    {"id": 27, "name": "FRA-SMT-L06"},
    {"id": 28, "name": "THT-L01"},
    {"id": 29, "name": "THT-L02"},
    {"id": 30, "name": "THT-L03"},
    {"id": 31, "name": "THT-L04"},
    {"id": 32, "name": "THT-L05"},
    {"id": 33, "name": "TL-L01"},
    {"id": 34, "name": "TL-L02"},
    {"id": 35, "name": "TL-L03"},
    {"id": 36, "name": "TL-L04"},
]


#raisonnement

# station fixed = "nom machine group" :"nom"
# mapping machine_group_name_to_id = "SPI-GROUP": 3
#post api "machine_group_id": machine_group_name_to_id[station["machine_group"]] 


STATIONS_FIXED: list[dict[str, Any]] = [
    {"legacy_id": 4,   "machine_group": "PNP-GROUP", "name": "FRA-PP-L01-01",  "description": "Pick & Place 1"},
    {"legacy_id": 5,   "machine_group": "PNP-GROUP", "name": "FRA-PP-L01-02",  "description": "Pick & Place 2"},
    {"legacy_id": 6,   "machine_group": "PNP-GROUP", "name": "FRA-PP-L01-03",  "description": "Pick & Place 3"},
    {"legacy_id": 7,   "machine_group": "OVE-GROUP", "name": "FRA-OVE-L01-01", "description": "Reflow Oven"},
    {"legacy_id": 8,   "machine_group": "AOI-GROUP", "name": "FRA-AOI-L01-01", "description": "AOI"},
    {"legacy_id": 12,  "machine_group": "OVE-GROUP", "name": "FRA-OVE-L01-02", "description": ""},
    {"legacy_id": 21,  "machine_group": "AIM-GROUP", "name": "THT-AIM-L01-01", "description": None},
    {"legacy_id": 22,  "machine_group": "AIM-GROUP", "name": "THT-AIM-L02-01", "description": None},
    {"legacy_id": 23,  "machine_group": "AIM-GROUP", "name": "THT-AIM-L03-01", "description": None},
    {"legacy_id": 24,  "machine_group": "AIM-GROUP", "name": "THT-AIM-L04-01", "description": None},
    {"legacy_id": 25,  "machine_group": "AIM-GROUP", "name": "THT-AIM-L05-01", "description": None},
    {"legacy_id": 27,  "machine_group": "AOI-GROUP", "name": "FRA-AOI-L02-01", "description": None},
    {"legacy_id": 28,  "machine_group": "AOI-GROUP", "name": "FRA-AOI-L03-01", "description": None},
    {"legacy_id": 29,  "machine_group": "AOI-GROUP", "name": "FRA-AOI-L04-01", "description": None},
    {"legacy_id": 30,  "machine_group": "AOI-GROUP", "name": "FRA-AOI-L05-01", "description": None},
    {"legacy_id": 31,  "machine_group": "AOI-GROUP", "name": "FRA-AOI-L06-01", "description": None},
    {"legacy_id": 32,  "machine_group": "AOI-GROUP", "name": "THT-AOI-L01-01", "description": None},
    {"legacy_id": 33,  "machine_group": "AOI-GROUP", "name": "THT-AOI-L02-01", "description": None},
    {"legacy_id": 34,  "machine_group": "AOI-GROUP", "name": "THT-AOI-L03-01", "description": None},
    {"legacy_id": 35,  "machine_group": "AOI-GROUP", "name": "THT-AOI-L04-01", "description": None},
    {"legacy_id": 36,  "machine_group": "AOI-GROUP", "name": "THT-AOI-L05-01", "description": None},
    {"legacy_id": 37,  "machine_group": "AOI-GROUP", "name": "TL-AOI-L01-01",  "description": None},
    {"legacy_id": 38,  "machine_group": "AOI-GROUP", "name": "TL-AOI-L02-01",  "description": None},
    {"legacy_id": 39,  "machine_group": "AOI-GROUP", "name": "TL-AOI-L03-01",  "description": None},
    {"legacy_id": 40,  "machine_group": "AOI-GROUP", "name": "TL-AOI-L04-01",  "description": None},
    {"legacy_id": 41,  "machine_group": "BCT-GROUP", "name": "TL-BCT-L01-01",  "description": None},
    {"legacy_id": 42,  "machine_group": "BCT-GROUP", "name": "TL-BCT-L02-01",  "description": None},
    {"legacy_id": 43,  "machine_group": "BCT-GROUP", "name": "TL-BCT-L03-01",  "description": None},
    {"legacy_id": 44,  "machine_group": "BCT-GROUP", "name": "TL-BCT-L04-01",  "description": None},
    {"legacy_id": 45,  "machine_group": "CC-GROUP",  "name": "THT-CC-L01-01",  "description": None},
    {"legacy_id": 46,  "machine_group": "CC-GROUP",  "name": "THT-CC-L02-01",  "description": None},
    {"legacy_id": 47,  "machine_group": "CC-GROUP",  "name": "THT-CC-L03-01",  "description": None},
    {"legacy_id": 48,  "machine_group": "CC-GROUP",  "name": "THT-CC-L04-01",  "description": None},
    {"legacy_id": 49,  "machine_group": "CC-GROUP",  "name": "THT-CC-L05-01",  "description": None},
    {"legacy_id": 50,  "machine_group": "FPT-GROUP", "name": "TL-FPT-L01-01",  "description": None},
    {"legacy_id": 51,  "machine_group": "FPT-GROUP", "name": "TL-FPT-L02-01",  "description": None},
    {"legacy_id": 52,  "machine_group": "FPT-GROUP", "name": "TL-FPT-L03-01",  "description": None},
    {"legacy_id": 53,  "machine_group": "FPT-GROUP", "name": "TL-FPT-L04-01",  "description": None},
    {"legacy_id": 54,  "machine_group": "FTS-GROUP", "name": "TL-FTS-L01-01",  "description": None},
    {"legacy_id": 55,  "machine_group": "FTS-GROUP", "name": "TL-FTS-L02-01",  "description": None},
    {"legacy_id": 56,  "machine_group": "FTS-GROUP", "name": "TL-FTS-L03-01",  "description": None},
    {"legacy_id": 57,  "machine_group": "FTS-GROUP", "name": "TL-FTS-L04-01",  "description": None},
    {"legacy_id": 58,  "machine_group": "ICT-GROUP", "name": "TL-ICT-L01-01",  "description": None},
    {"legacy_id": 59,  "machine_group": "ICT-GROUP", "name": "TL-ICT-L02-01",  "description": None},
    {"legacy_id": 60,  "machine_group": "ICT-GROUP", "name": "TL-ICT-L03-01",  "description": None},
    {"legacy_id": 61,  "machine_group": "ICT-GROUP", "name": "TL-ICT-L04-01",  "description": None},
    {"legacy_id": 63,  "machine_group": "OVE-GROUP", "name": "FRA-OVE-L02-01", "description": None},
    {"legacy_id": 64,  "machine_group": "OVE-GROUP", "name": "FRA-OVE-L03-01", "description": None},
    {"legacy_id": 65,  "machine_group": "OVE-GROUP", "name": "FRA-OVE-L04-01", "description": None},
    {"legacy_id": 66,  "machine_group": "OVE-GROUP", "name": "FRA-OVE-L05-01", "description": None},
    {"legacy_id": 67,  "machine_group": "OVE-GROUP", "name": "FRA-OVE-L06-01", "description": None},
    {"legacy_id": 68,  "machine_group": "PO-GROUP",  "name": "THT-PO-L01-01",  "description": None},
    {"legacy_id": 69,  "machine_group": "PO-GROUP",  "name": "THT-PO-L02-01",  "description": None},
    {"legacy_id": 70,  "machine_group": "PO-GROUP",  "name": "THT-PO-L03-01",  "description": None},
    {"legacy_id": 71,  "machine_group": "PO-GROUP",  "name": "THT-PO-L04-01",  "description": None},
    {"legacy_id": 72,  "machine_group": "PO-GROUP",  "name": "THT-PO-L05-01",  "description": None},
    {"legacy_id": 75,  "machine_group": "PNP-GROUP", "name": "FRA-PP-L02-01",  "description": None},
    {"legacy_id": 76,  "machine_group": "PNP-GROUP", "name": "FRA-PP-L02-02",  "description": None},
    {"legacy_id": 77,  "machine_group": "PNP-GROUP", "name": "FRA-PP-L03-01",  "description": None},
    {"legacy_id": 78,  "machine_group": "PNP-GROUP", "name": "FRA-PP-L03-02",  "description": None},
    {"legacy_id": 79,  "machine_group": "PNP-GROUP", "name": "FRA-PP-L04-01",  "description": None},
    {"legacy_id": 80,  "machine_group": "PNP-GROUP", "name": "FRA-PP-L04-02",  "description": None},
    {"legacy_id": 81,  "machine_group": "PNP-GROUP", "name": "FRA-PP-L05-01",  "description": None},
    {"legacy_id": 82,  "machine_group": "PNP-GROUP", "name": "FRA-PP-L05-02",  "description": None},
    {"legacy_id": 83,  "machine_group": "PNP-GROUP", "name": "FRA-PP-L06-01",  "description": None},
    {"legacy_id": 84,  "machine_group": "PNP-GROUP", "name": "FRA-PP-L06-02",  "description": None},
    {"legacy_id": 85,  "machine_group": "SPI-GROUP", "name": "FRA-SPI-L01-01", "description": None},
    {"legacy_id": 86,  "machine_group": "SPI-GROUP", "name": "FRA-SPI-L02-01", "description": None},
    {"legacy_id": 87,  "machine_group": "SPI-GROUP", "name": "FRA-SPI-L03-01", "description": None},
    {"legacy_id": 88,  "machine_group": "SPI-GROUP", "name": "FRA-SPI-L04-01", "description": None},
    {"legacy_id": 89,  "machine_group": "SPI-GROUP", "name": "FRA-SPI-L05-01", "description": None},
    {"legacy_id": 90,  "machine_group": "SPI-GROUP", "name": "FRA-SPI-L06-01", "description": None},
    {"legacy_id": 91,  "machine_group": "SPP-GROUP", "name": "FRA-SPP-L01-01", "description": None},
    {"legacy_id": 92,  "machine_group": "SPP-GROUP", "name": "FRA-SPP-L02-01", "description": None},
    {"legacy_id": 93,  "machine_group": "SPP-GROUP", "name": "FRA-SPP-L03-01", "description": None},
    {"legacy_id": 94,  "machine_group": "SPP-GROUP", "name": "FRA-SPP-L04-01", "description": None},
    {"legacy_id": 95,  "machine_group": "SPP-GROUP", "name": "FRA-SPP-L05-01", "description": None},
    {"legacy_id": 96,  "machine_group": "SPP-GROUP", "name": "FRA-SPP-L06-01", "description": None},
    {"legacy_id": 97,  "machine_group": "WSM-GROUP", "name": "THT-WSM-L01-01", "description": None},
    {"legacy_id": 98,  "machine_group": "WSM-GROUP", "name": "THT-WSM-L02-01", "description": None},
    {"legacy_id": 99,  "machine_group": "WSM-GROUP", "name": "THT-WSM-L03-01", "description": None},
    {"legacy_id": 100, "machine_group": "WSM-GROUP", "name": "THT-WSM-L04-01", "description": None},
    {"legacy_id": 101, "machine_group": "WSM-GROUP", "name": "THT-WSM-L05-01", "description": None},
    {"legacy_id": 102, "machine_group": "ASM-GROUP", "name": "ASS-ASM-L01-01", "description": None},
]

machine_group_name_to_id = {
    "LAB-GROUP": 1,
    "SPP-GROUP": 2,
    "SPI-GROUP": 3,
    "PNP-GROUP": 4,
    "OVE-GROUP": 5,
    "AOI-GROUP": 6,
    "AIM-GROUP": 10,
    "PO-GROUP": 11,
    "WSM-GROUP": 12,
    "ICT-GROUP": 13,
    "FPT-GROUP": 14,
    "FTS-GROUP": 15,
    "ASM-GROUP": 16,
    "BCT-GROUP": 17,
    "CC-GROUP": 21,
}


# Exact machine groups from production DB export – covers every machine_group_id
# referenced by STATIONS_FIXED so the FK constraint is always satisfied.



# Exact associations from line_station_association export
LINE_STATION_ASSOCIATIONS_FIXED: list[tuple[int, int]] = [
    (21, 102),
    (22, 4), (22, 5), (22, 7), (22, 8), (22, 85), (22, 91),
    (23, 27), (23, 63), (23, 75), (23, 76), (23, 86), (23, 92),
    (24, 28), (24, 64), (24, 77), (24, 78), (24, 87), (24, 93),
    (25, 29), (25, 65), (25, 79), (25, 80), (25, 88), (25, 94),
    (26, 30), (26, 66), (26, 81), (26, 82), (26, 89), (26, 95),
    (27, 31), (27, 67), (27, 83), (27, 84), (27, 90), (27, 96),
    (28, 21), (28, 32), (28, 45), (28, 68), (28, 97),
    (29, 22), (29, 33), (29, 46), (29, 69), (29, 98),
    (30, 23), (30, 34), (30, 47), (30, 70), (30, 99),
    (31, 24), (31, 35), (31, 48), (31, 71), (31, 100),
    (32, 25), (32, 36), (32, 49), (32, 72), (32, 101),
    (33, 37), (33, 41), (33, 50), (33, 54), (33, 58),
    (34, 38), (34, 42), (34, 51), (34, 55), (34, 59),
    (35, 39), (35, 43), (35, 52), (35, 56), (35, 60),
    (36, 40), (36, 44), (36, 53), (36, 57), (36, 61),
]

# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def _dt_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def _d_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def _clamp_dt(dt: datetime, lo: datetime, hi: datetime) -> datetime:
    return max(lo, min(hi, dt))

def _random_between(a: datetime, b: datetime) -> datetime:
    if b < a:
        a, b = b, a
    span = (b - a).total_seconds()
    return a + timedelta(seconds=random.uniform(0, span)) if span > 0 else a

def _add_months(dt: datetime, months: int) -> datetime:
    y, m = dt.year, dt.month + months
    while m > 12:
        y += 1
        m -= 12
    while m < 1:
        y -= 1
        m += 12
    return dt.replace(year=y, month=m, day=min(dt.day, calendar.monthrange(y, m)[1]))

def _pg_config_from_env() -> PgConfig:
    password = os.getenv("PGPASSWORD", "080701")
    if not password:
        raise RuntimeError("Set PGPASSWORD environment variable")
    return PgConfig(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5435")),
        dbname=os.getenv("PGDATABASE", "postgres"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "admin123"),
        schema=os.getenv("PGSCHEMA", "public"),
        table="work_orders",
    )

def _get_next_id(conn, table: str, id_col: str = "id") -> int:
    with conn.cursor() as cur:
        cur.execute(cast(Any, f"SELECT COALESCE(MAX({id_col}), 0) FROM {table};"))
        (v,) = cur.fetchone()
    return int(v) + 1

def _chunks(it: Iterable[dict[str, Any]], n: int) -> Iterable[list[dict[str, Any]]]:
    buf: list[dict[str, Any]] = []
    for x in it:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

def _bulk_insert(conn, sql: str, rows: list[dict[str, Any]],
                 chunk: int = 5000, label: str = "") -> int:
    if not rows:
        if label:
            print(f"  - {label}: 0 rows (skipped)")
        return 0
    inserted = 0
    with conn.cursor() as cur:
        for batch in _chunks(iter(rows), chunk):
            cur.executemany(cast(Any, sql), batch)
            inserted += len(batch)
    if label:
        print(f"  + {label}: {inserted} rows")
    return inserted

# ---------------------------------------------------------------------------
# Shift helpers
# ---------------------------------------------------------------------------

def _shift_windows(ws: datetime, we: datetime) -> list[tuple[datetime, datetime]]:
    wins: list[tuple[datetime, datetime]] = []
    day = ws.replace(hour=0, minute=0, second=0, microsecond=0)
    while day < we:
        for _, sh, eh in SHIFTS:
            s = day.replace(hour=sh)
            e = (day + timedelta(days=1)).replace(hour=eh) if eh < sh else day.replace(hour=eh)
            s, e = max(s, ws), min(e, we)
            if e > s:
                wins.append((s, e))
        day += timedelta(days=1)
    return wins

def _snap_shift_start(dt: datetime) -> datetime:
    for ahead in range(25):
        c = dt + timedelta(hours=ahead)
        for _, sh, eh in SHIFTS:
            s = c.replace(hour=sh, minute=0, second=0, microsecond=0)
            e = ((c + timedelta(days=1)).replace(hour=eh, minute=0, second=0, microsecond=0)
                 if eh < sh else c.replace(hour=eh, minute=0, second=0, microsecond=0))
            if s <= c < e:
                return c
            if ahead == 0 and c < s:
                return s
    return dt

def _wo_duration(qty: int) -> timedelta:
    n_shifts = max(1, int((qty * AVG_CYCLE_TIME_SEC / SHIFT_DURATION_S) + 0.9999))
    days = max(1, (n_shifts + 2) // 3)
    return timedelta(days=days, hours=1)

def _booking_timestamps(ws: datetime, we: datetime, qty: int,
                         partial: bool) -> list[datetime]:
    wins = _shift_windows(ws, we)
    target = max(1, int(qty * random.uniform(0.40, 0.85)) if partial else qty)
    ts: list[datetime] = []
    for s, e in wins:
        if len(ts) >= target:
            break
        t = s + timedelta(seconds=30)
        while t < e and len(ts) < target:
            ts.append(_clamp_dt(t + timedelta(seconds=random.uniform(-20, 20)), s, e))
            t += timedelta(seconds=AVG_CYCLE_TIME_SEC)
    return ts[:target]

# ---------------------------------------------------------------------------
# State plan
# ---------------------------------------------------------------------------

def _state_plan(n: int) -> list[str]:
    n = max(1, n)
    nd = max(1, round(n * 0.30))
    nf = max(1, round(n * 0.10))
    na = max(1, round(n * 0.20))
    np = max(1, round(n * 0.20))
    no = max(0, n - nd - nf - na - np)
    out = list(("delivered",) * nd + ("finished",) * nf +
               ("active",) * na + ("planned",) * np + ("open",) * no)
    random.shuffle(out)
    if out:
        try:
            i = out.index("delivered")
            out[0], out[i] = out[i], out[0]
        except ValueError:
            pass
    return out

# ---------------------------------------------------------------------------
# Layer 0 – pure reference tables
# ---------------------------------------------------------------------------


def api_post(endpoint: str, payload: dict[str, Any], token: Optional[str] = None) -> dict[str, Any]:
    url = f"{BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"
    
    headers = _auth_headers(token)
    
    if token:
        headers["Authorization"] = f"Bearer {token}"

    response = requests.post(url, json=payload, headers=headers, timeout=30)

    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(
            f"POST {url} failed with status {response.status_code}: {response.text}"
        ) from e

    # si l'API ne renvoie pas de JSON, adapte ici
    return response.json()


def gen_company_codes_api(n: int = 3, token=None, client_id: int = 1) -> list[int]:
    if not token:
        raise ValueError("Token API manquant.")

    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)

    headers = _auth_headers(token)
    created_ids: list[int] = []

    for i in range(n):
        name = f"CC-{i+1:03d}"

        # 🔍 1. Vérifier si existe déjà
        r = requests.get(
            f"{BASE_URL}/company-codes/",
            params={"name": name},
            headers=headers,
            timeout=30,
            verify=False
        )

        if r.status_code == 200:
            results = r.json()

            # ⚠️ dépend du format API (liste ou objet paginé)
            if results:
                existing = results[0] if isinstance(results, list) else results.get("results", [])[0]
                existing_id = int(existing["id"])
                created_ids.append(existing_id)
                print(f"Reusing existing company code '{name}' (ID {existing_id})")
                continue

        elif r.status_code != 404:
            r.raise_for_status()

        # ➕ 2. Créer si n'existe pas
        payload = {
            "name": name,
            "description": f"Company Code {i+1}",
            "user_id": 1,
            "client_id": client_id
        }

        r = requests.post(
            f"{BASE_URL}/company-codes/",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False
        )
      
        
        print("STATUS:", r.status_code)
        print("RESPONSE:", r.text)

        if r.status_code in (200, 201):
            data = r.json()
            created_ids.append(int(data["id"]))
            continue

        # ⚠️ cas concurrence (race condition)
        if r.status_code == 409:
            # quelqu’un l’a créé entre le GET et le POST → on refait un GET
            r2 = requests.get(
                f"{BASE_URL}/company-codes/",
                params={"name": name},
                headers=headers,
                timeout=30,
                verify=False
            )

            if r2.status_code == 200:
                results = r2.json()
                if results:
                    existing = results[0] if isinstance(results, list) else results.get("results", [])[0]
                    existing_id = int(existing["id"])
                    created_ids.append(existing_id)
                    print(f"Recovered after conflict '{name}' (ID {existing_id})")
                    continue

            raise RuntimeError(f"Conflict on '{name}' but unable to recover ID.")

        r.raise_for_status()

    print(f"  + company_codes (API): {len(created_ids)} rows")
    return created_ids

def gen_clients_api(company_code_ids: list[int], n: int = 3, token=None) -> list[int]:
    if not token:
        raise ValueError("Token API manquant.")

    headers = _auth_headers(token)
    names = ["PCB-Corp", "ElektraTech", "CircuitMasters", "NexaPCB", "SolderPro"]

    created_ids: list[int] = []

    for i in range(n):
        cc_id = company_code_ids[i % len(company_code_ids)]
        client_name = f"{names[i % len(names)][:14]} {i+1}"

        # 🔍 1. CHECK via GET
        r = requests.get(
            f"{BASE_URL}/clients/clients",
            params={"name": client_name},
            headers=headers,
            timeout=30,
            verify=False
        )

        if r.status_code == 200:
            data = r.json()
            results = data if isinstance(data, list) else data.get("results", [])

            if results:
                existing_id = int(results[0]["id"])
                created_ids.append(existing_id)
                print(f"Reusing existing client '{client_name}' ({existing_id})")
                continue

        # ➕ 2. CREATE
        payload = {
            "user_id": 1,
            "name": client_name,
            "description": f"PCB manufacturing client {i+1}"
        }

        r = requests.post(
            f"{BASE_URL}/clients/",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False
        )

        print("CLIENT STATUS:", r.status_code)
        print("CLIENT RESPONSE:", r.text)

        r.raise_for_status()

        client_id = int(r.json()["id"])
        created_ids.append(client_id)

        # 🔗 3. LINK client → company_code
        update_payload = {
            "client_id": client_id
        }

        r = requests.patch(   # ✅ PATCH au lieu de PUT
            f"{BASE_URL}/company-codes/{cc_id}",
            json=update_payload,
            headers=headers,
            timeout=30,
            verify=False
        )

        print("LINK STATUS:", r.status_code)
        print("LINK RESPONSE:", r.text)

        r.raise_for_status()

    print(f"  + clients (API): {len(created_ids)} rows")
    return created_ids

def gen_sites_api(company_code_ids: list[int], n: int = 2, token=None):
    headers = _auth_headers(token)

    locs = [
        ("SITE-TN-01", "36.8065,10.1815", "Tunis Plant"),
        ("SITE-TN-02", "36.7370,10.2320", "Ariana Plant"),
        ("SITE-EU-01", "48.8566,2.3522", "Paris Plant")
    ]

    created_ids: list[int] = []

    for i in range(n):
        loc = locs[i % len(locs)]
        site_number = f"{loc[0]}-{i+1}"

        payload = {
            "user_id": 1,
            "company_code_id": company_code_ids[i % len(company_code_ids)],
            "site_number": site_number,
            "site_external_number": f"EXT-{i+1:04d}",
            "deletion_priority": 0,
            "geo_coordinates": loc[1],
            "description": loc[2]
        }

        r = requests.post(
            f"{BASE_URL}/sites/sites/",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False
        )

        print("SITE STATUS:", r.status_code)
        print("SITE RESPONSE:", r.text)

        if r.status_code in (200, 201):
            data = _get_json_or_text(r)
            site_id = data.get("id")
            if not site_id:
                raise RuntimeError(f"Site API response has no id: {data}")
            created_ids.append(int(site_id))
            continue
        
        if r.status_code in (400, 409):
            data = _get_json_or_text(r)
            detail = str(data.get("detail", ""))

            existing_id = _extract_id_from_detail(detail)
            if existing_id is not None:
                created_ids.append(existing_id)
                print(f"Reusing existing site '{site_number}' with ID {existing_id}")
                continue

            # fallback GET si l'API de conflit ne donne pas l'ID
            existing_id = _get_existing_id_by_get(
                "sites/sites/",
                token,
                wanted_value=site_number,
                name_key_candidates=("site_number", "name"),
            )
            if existing_id is not None:
                created_ids.append(existing_id)
                print(f"Reusing existing site '{site_number}' with ID {existing_id}")
                continue

            raise RuntimeError(
                f"Site '{site_number}' already exists, but ID could not be resolved."
            )

        r.raise_for_status()

    print(f"  + sites (API): {len(created_ids)} rows")
    return created_ids

def gen_part_types_api(n: int = 6, token=None) -> list[int]:
    headers = _auth_headers(token)

    base_items = [
        ("PCB Assembly", "Finished PCB assembly"),
        ("Raw PCB", "Bare printed circuit board"),
        ("SMD Component", "Surface-mount device"),
        ("THT Component", "Through-hole component"),
        ("Mechanical", "Mechanical / hardware part"),
        ("Consumable", "Solder, flux, cleaning agent"),
    ]

    ids: list[int] = []

    for i in range(n):
        name, desc = base_items[i % len(base_items)]

        payload = {
            "name": name,
            "description": desc,
            "user_id": 1,
            "is_active": True,
        }

        r = requests.post(
            f"{BASE_URL}/part-types/part-types/",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False,
        )

        print("PART TYPE STATUS:", r.status_code)
        print("PART TYPE RESPONSE:", r.text)

        if r.status_code in (200, 201):
            data = _get_json_or_text(r)
            ids.append(int(data["id"]))
            continue

        if r.status_code in (400, 409):
            data = _get_json_or_text(r)
            detail = str(data.get("detail", ""))

            existing_id = _extract_id_from_detail(detail)
            if existing_id is not None:
                ids.append(existing_id)
                print(f"Reusing existing part type '{name}' with ID {existing_id}")
                continue

            existing_id = _get_existing_id_by_get(
                "part-types/part-types/",
                token,
                wanted_value=name,
                name_key_candidates=("name",),
            )
            if existing_id is not None:
                ids.append(existing_id)
                print(f"Reusing existing part type '{name}' with ID {existing_id}")
                continue

            raise RuntimeError(
                f"Part type '{name}' already exists, but ID could not be resolved."
            )

        r.raise_for_status()

    print(f"  + part_types (API): {len(ids)} rows")
    return ids

def gen_part_group_types_api(n: int = 4, token=None) -> list[int]:
    headers = _auth_headers(token)

    items = [
        ("Finished Goods", "Shipped customer products"),
        ("WIP", "Work-in-progress assemblies"),
        ("Components", "Purchased electronic components"),
        ("Raw Material", "PCB substrates and bare boards"),
    ]

    created_ids: list[int] = []

    for i in range(n):
        name, desc = items[i % len(items)]

        payload = {
            "name": name,
            "description": desc,
        }

        r = requests.post(
            f"{BASE_URL}/part-group-types/part-group-types/",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False,
        )

        print("PART GROUP TYPE STATUS:", r.status_code)
        print("PART GROUP TYPE RESPONSE:", r.text)

        if r.status_code in (200, 201):
            data = _get_json_or_text(r)
            created_ids.append(int(data["id"]))
            continue

        if r.status_code in (400, 409):
            data = _get_json_or_text(r)
            detail = str(data.get("detail", ""))

            existing_id = _extract_id_from_detail(detail)
            if existing_id is not None:
                created_ids.append(existing_id)
                print(f"Reusing existing part group type '{name}' with ID {existing_id}")
                continue

            existing_id = _get_existing_id_by_get(
                "part-group-types/part-group-types/",
                token,
                wanted_value=name,
                name_key_candidates=("name",),
            )
            if existing_id is not None:
                created_ids.append(existing_id)
                print(f"Reusing existing part group type '{name}' with ID {existing_id}")
                continue

            raise RuntimeError(
                f"Part group type '{name}' already exists, but ID could not be resolved."
            )

        r.raise_for_status()

    print(f"  + part_group_types (API): {len(created_ids)} rows")
    return created_ids

def gen_workplan_types_api(n: int = 5, token=None) -> list[int]:
    headers = _auth_headers(token)

    items = [
        ("SMT", "Surface Mount Technology line"),
        ("THT", "Through-Hole Technology line"),
        ("MIX", "Mixed SMT + THT line"),
        ("TES", "Test & inspection only"),
        ("REW", "Rework / repair process"),
    ]

    created_ids: list[int] = []

    for i in range(n):
        code, desc = items[i % len(items)]

        payload = {
            "name": code,
            "description": desc,
        }

        r = requests.post(
            f"{BASE_URL}/workplan-types/workplan-types/",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False,
        )

        print("WORKPLAN TYPE STATUS:", r.status_code)
        print("WORKPLAN TYPE RESPONSE:", r.text)

        if r.status_code in (200, 201):
            data = _get_json_or_text(r)
            created_ids.append(int(data["id"]))
            continue

        if r.status_code in (400, 409):
            data = _get_json_or_text(r)
            detail = str(data.get("detail", ""))

            existing_id = _extract_id_from_detail(detail)
            if existing_id is not None:
                created_ids.append(existing_id)
                print(f"Reusing existing workplan type '{code}' with ID {existing_id}")
                continue

            existing_id = _get_existing_id_by_get(
                "workplan-types/workplan-types/",
                token,
                wanted_value=code,
                name_key_candidates=("name",),
            )
            if existing_id is not None:
                created_ids.append(existing_id)
                print(f"Reusing existing workplan type '{code}' with ID {existing_id}")
                continue

            raise RuntimeError(
                f"Workplan type '{code}' already exists, but ID could not be resolved."
            )

        r.raise_for_status()

    print(f"  + workplan_types (API): {len(created_ids)} rows")
    return created_ids

def gen_failure_group_types_api(n: int = 6, token=None) -> list[int]:
    headers = _auth_headers(token)

    items = [
        ("Solder Defects", "Bridging, opens, insufficient solder"),
        ("Component Defects", "Missing, wrong value, wrong orientation"),
        ("PCB Defects", "Board damage, contamination, delamination"),
        ("Process Defects", "Paste, placement, reflow process issues"),
        ("Electrical Failures", "Short, open circuit, ESD damage"),
        ("Cosmetic Defects", "Scratches, marks, flux residue"),
    ]

    created_ids: list[int] = []

    for i in range(n):
        name, desc = items[i % len(items)]
        failure_group_name = f"{name[:14]} {i+1}"

        payload = {
            "failure_group_name": failure_group_name,
            "failure_group_desc": desc,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        }

        r = requests.post(
            f"{BASE_URL}/failure-group-types/failure-group-types/",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False,
        )

        print("FAILURE GROUP TYPE STATUS:", r.status_code)
        print("FAILURE GROUP TYPE RESPONSE:", r.text)

        if r.status_code in (200, 201):
            data = _get_json_or_text(r)
            created_ids.append(int(data["id"]))
            continue

        if r.status_code in (400, 409):
            data = _get_json_or_text(r)
            detail = str(data.get("detail", ""))

            existing_id = _extract_id_from_detail(detail)
            if existing_id is not None:
                created_ids.append(existing_id)
                print(
                    f"Reusing existing failure group type '{failure_group_name}' "
                    f"with ID {existing_id}"
                )
                continue

            existing_id = _get_existing_id_by_get(
                "failure-group-types/failure-group-types/",
                token,
                wanted_value=failure_group_name,
                name_key_candidates=("failure_group_name", "name"),
            )
            if existing_id is not None:
                created_ids.append(existing_id)
                print(
                    f"Reusing existing failure group type '{failure_group_name}' "
                    f"with ID {existing_id}"
                )
                continue

            raise RuntimeError(
                f"Failure group type '{failure_group_name}' already exists, "
                f"but ID could not be resolved."
            )

        r.raise_for_status()

    print(f"  + failure_group_types (API): {len(created_ids)} rows")
    return created_ids

def gen_machine_condition_groups_api(n: int = 3, token=None) -> list[int]:
    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)

    headers = _auth_headers(token)

    items = [
        ("Unplanned Downtime", "Breakdowns, stoppages, deviations"),
        ("Planned Downtime", "Changeover, setup, maintenance"),
        ("Operational", "Running, breaks, meetings"),
    ]

    created_ids: list[int] = []

    for i in range(n):
        name, desc = items[i % len(items)]
        group_name = f"{name[:14]} {i+1}"

        payload = {
            "group_name": group_name,
            "group_description": desc,
            "is_active": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }

        r = requests.post(
            f"{BASE_URL}/machine-condition-groups/machine-condition-groups/",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False
        )

        print("STATUS:", r.status_code)
        print("RESPONSE:", r.text)

        if r.status_code in (200, 201):
            data = _get_json_or_text(r)
            created_ids.append(int(data["id"]))
            continue

        if r.status_code in (400, 409):
            data = _get_json_or_text(r)
            detail = str(data.get("detail", ""))

            existing_id = _extract_id_from_detail(detail)
            if existing_id is not None:
                created_ids.append(existing_id)
                print(f"Reusing existing machine condition group '{group_name}' with ID {existing_id}")
                continue

            existing_id = _get_existing_id_by_get(
                "machine-condition-groups/machine-condition-groups/",
                token,
                wanted_value=group_name,
                name_key_candidates=("group_name", "name"),
            )
            if existing_id is not None:
                created_ids.append(existing_id)
                print(f"Reusing existing machine condition group '{group_name}' with ID {existing_id}")
                continue

            raise RuntimeError(
                f"Machine condition group '{group_name}' already exists, but ID could not be resolved."
            )

        r.raise_for_status()

    print("Machine Condition Groups créés :", created_ids)
    return created_ids

def gen_cells_api(site_ids, n=5, token=None):

    if not site_ids:
        raise RuntimeError("❌ site_ids vide → impossible de continuer")

    headers = _auth_headers(token)

    cells_def = [
        ("SMT-LINE-A", "SMT Production Line A"),
        ("SMT-LINE-B", "SMT Production Line B"),
        ("THT-LINE", "THT Production Line"),
        ("TEST-CELL", "Test & Inspection Cell"),
        ("REWORK", "Rework Station"),
    ]

    created_ids = []

    for i in range(n):
        name, desc = cells_def[i % len(cells_def)]

        payload = {
            "name": name,
            "description": desc,
            "site_id": site_ids[i % len(site_ids)],
            "user_id": 1,
            "info": "Cell capacity",
            "is_active": True
        }

        r = requests.post(
            f"{BASE_URL}/cells/", 
            json=payload, 
            headers=headers,
            timeout=30,
            verify=False
        )

        print("STATUS:", r.status_code, r.text)

        if r.status_code in (200, 201):
            created_ids.append(r.json()["id"])
            continue

        if r.status_code in (400, 409):
            print(f"⚠️ exists → {name}")

            get_r = requests.get(
                f"{BASE_URL}/cells/", 
                json=payload, 
                headers=headers,
                timeout=30,
                verify=False
            )

            if get_r.status_code == 200:
                for c in get_r.json():
                    if c.get("name") == name:
                        created_ids.append(c["id"])
                        break
            continue

        r.raise_for_status()

    print("Cells finales :", created_ids)

    if not created_ids:
        raise RuntimeError("❌ aucune cell récupérée → pipeline bloqué")

    return created_ids

def gen_machine_groups_api(cell_ids: list[int], n: int = 10, token=None):

    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)

    headers = _auth_headers(token)

    mg_def = [
        ("SPI-GROUP",    "Solder Paste Inspection"),
        ("PNP-GROUP",    "Pick & Place Machines"),
        ("REFLOW-GROUP", "Reflow Ovens"),
        ("AOI-GROUP",    "Automated Optical Inspection"),
        ("ICT-GROUP",    "In-Circuit Testers"),
        ("FUNC-GROUP",   "Functional Testers"),
        ("WAVE-GROUP",   "Wave Solder Machines"),
        ("XRAY-GROUP",   "X-Ray Inspection"),
        ("PROG-GROUP",   "Programmers"),
        ("CLEAN-GROUP",  "Cleaning Machines"),
    ]

    created_ids = []

    for i in range(n):
        name, desc = mg_def[i % len(mg_def)]

        payload = {
            "name": f"{name[:14]} {i+1}",
            "description": desc,
            "user_id": 1,
            "cell_id": cell_ids[i % len(cell_ids)],
            "is_active": True,
            "failure": False
        }

        r = requests.post(
            f"{BASE_URL}/machine-groups/machine_groups/",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False
        )

        print("STATUS:", r.status_code)
        print("RESPONSE:", r.text)

        if r.status_code in (200, 201):
            created_ids.append(r.json()["id"])
            continue

        if r.status_code in (400, 409):
            print(f"⚠️ exists → {payload['name']}")
            continue

        r.raise_for_status()

    print("Machine Groups créés :", created_ids)
    return created_ids


def seed_machine_groups_api(cell_ids: list[int], token: str) -> dict[int, int]:
    if not cell_ids:
        raise ValueError("cell_ids est vide — impossible de créer les machine groups")

    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)

    headers = _auth_headers(token)
    url = f"{BASE_URL}/machine-groups/machine_groups/"

    # 🔥 1. définition locale SANS IDs
    machine_groups = [
        {"name": "LAB-GROUP", "description": "Labeling Machines"},
        {"name": "SPP-GROUP", "description": "Solder Paste Printer"},
        {"name": "SPI-GROUP", "description": "Solder Paste Inspection"},
        {"name": "PNP-GROUP", "description": "Pick & Place Machines"},
        {"name": "OVE-GROUP", "description": "Reflow Ovens"},
        {"name": "AOI-GROUP", "description": "Automated Optical Inspection"},
        {"name": "AIM-GROUP", "description": "Auto Insertion Machines"},
        {"name": "PO-GROUP", "description": "Pin-through-hole Ovens"},
        {"name": "WSM-GROUP", "description": "Wave Solder Machines"},
        {"name": "ICT-GROUP", "description": "In-Circuit Testers"},
        {"name": "FPT-GROUP", "description": "Flying Probe Testers"},
        {"name": "FTS-GROUP", "description": "Functional Test Systems"},
        {"name": "ASM-GROUP", "description": "Assembly Machines"},
        {"name": "BCT-GROUP", "description": "Box Build & Cable Testers"},
        {"name": "CC-GROUP", "description": "Conformal Coating Machines"},
    ]

    id_map: dict[int, int] = {}

    # 🔥 2. GET existing once (important optimisation)
    r = requests.get(url, headers=headers, timeout=30, verify=False)
    existing = r.json()
    existing_list = existing["results"] if isinstance(existing, dict) and "results" in existing else existing

    existing_by_name = {x["name"]: x for x in existing_list}

    # 🔥 3. create or reuse
    for i, mg in enumerate(machine_groups):
        name = mg["name"]

        # ✔️ reuse if exists
        if name in existing_by_name:
            api_id = existing_by_name[name]["id"]
            id_map[i + 1] = api_id
            continue

        # ✔️ create if not exists
        payload = {
            "name": name,
            "description": mg["description"],
            "user_id": 1,
            "cell_id": cell_ids[i % len(cell_ids)],
            "is_active": True,
            "failure": False,
        }

        r = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        print("STATUS:", r.status_code)
        print("RESPONSE:", r.text)

        r.raise_for_status()
        api_id = r.json()["id"]

        id_map[i + 1] = api_id

    print("Machine group id map:", id_map)
    return id_map

def seed_fixed_stations_api(
    token: str,
    machine_group_name_to_id: dict[str, int],
) -> dict[int, int]:
    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)

    headers = _auth_headers(token)
    url = f"{BASE_URL}/stations/stations/"
    station_legacy_to_api_id: dict[int, int] = {}

    r = requests.get(url, headers=headers, timeout=30, verify=False)
    r.raise_for_status()
    existing = r.json()
    existing_list = existing["results"] if isinstance(existing, dict) and "results" in existing else existing
    existing_by_name = {
        row["name"]: row for row in existing_list
        if isinstance(row, dict) and "name" in row
    }

    for station in STATIONS_FIXED:
        mg_name = station["machine_group"]
        if mg_name not in machine_group_name_to_id:
            raise ValueError(f"Machine group inconnu '{mg_name}' pour station '{station['name']}'")

        if station["name"] in existing_by_name:
            api_id = int(existing_by_name[station["name"]]["id"])
            station_legacy_to_api_id[station["legacy_id"]] = api_id
            print(f"Reusing existing station '{station['name']}' (ID {api_id})")
            continue

        payload = {
            "machine_group_id": machine_group_name_to_id[mg_name],
            "name": station["name"],
            "description": station.get("description"),
            "is_active": True,
            "user_id": 5,
            "info": station.get("info"),
        }

        r = requests.post(url, json=payload, headers=headers, timeout=30, verify=False)

        if r.status_code in (200, 201):
            api_id = int(r.json()["id"])
            station_legacy_to_api_id[station["legacy_id"]] = api_id
            print(f"Created station '{station['name']}' (ID {api_id})")
            continue

        if r.status_code in (400, 409):
            recovered_id = _get_existing_id_by_get(
                "stations/stations/",
                token,
                wanted_value=station["name"],
                name_key_candidates=("name",),
            )
            if recovered_id is not None:
                station_legacy_to_api_id[station["legacy_id"]] = recovered_id
                print(f"Recovered existing station '{station['name']}' (ID {recovered_id})")
                continue

        r.raise_for_status()

    return station_legacy_to_api_id

def seed_fixed_lines_api(token: str, station_legacy_to_api_id: dict[int, int]) -> dict[int, int]:
    """
    Create fixed lines through the API.
    station_legacy_to_api_id maps export station IDs -> API station IDs.
    Returns line legacy/export ID -> API line ID.
    """

    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)

    headers = _auth_headers(token)
    line_legacy_to_api_id: dict[int, int] = {}
    LINES_URL = f"{BASE_URL}/lines/lines/"

    for ln in LINES_FIXED:
        export_line_id = ln["id"]

        export_station_ids = [
            sid for lid, sid in LINE_STATION_ASSOCIATIONS_FIXED
            if lid == export_line_id
        ]

        api_station_ids = []
        for export_sid in export_station_ids:
            api_sid = station_legacy_to_api_id.get(export_sid)
            if api_sid is not None:
                api_station_ids.append(api_sid)
            else:
                print(f"⚠️ station export id introuvable dans station_legacy_to_api_id: {export_sid}")

        if not api_station_ids:
            print(f"⚠️ aucune station trouvée pour la line {ln['name']} — création impossible")
            continue

        payload = {
            "name": ln["name"],
            "description": "",
            "date": datetime.now().isoformat(),
            "user_id": 8,
            "station_ids": api_station_ids,
        }

        print("POST URL:", LINES_URL)
        print("PAYLOAD:", payload)

        r = requests.post(
            LINES_URL,
            json=payload,
            headers=headers,
            timeout=30,
            verify=False
        )

        print("STATUS:", r.status_code)
        print("RESPONSE:", r.text)

        if r.status_code in (200, 201):
            api_line_id = int(r.json()["id"])
            line_legacy_to_api_id[export_line_id] = api_line_id
            print(f"✅ created line {ln['name']} -> {api_line_id}")
            continue

        if r.status_code in (400, 409, 422):
            g = requests.get(
                LINES_URL,
                headers=headers,
                timeout=30,
                verify=False
            )

            if g.status_code == 200:
                body = g.json()
                items = body["results"] if isinstance(body, dict) and "results" in body else body

                found = False
                for item in items:
                    if item.get("name") == ln["name"]:
                        api_line_id = int(item["id"])
                        line_legacy_to_api_id[export_line_id] = api_line_id
                        print(f"♻️ reused line {ln['name']} -> {api_line_id}")
                        found = True
                        break

                if not found:
                    print(f"⚠️ line existe peut-être mais introuvable: {ln['name']}")
            else:
                print(f"❌ GET lines failed: {g.status_code}")

            continue

        r.raise_for_status()

    print("Lines créées/réutilisées :", line_legacy_to_api_id)
    return line_legacy_to_api_id

def build_line_station_associations(
    line_legacy_to_api_id: dict[int, int],
    station_legacy_to_api_id: dict[int, int],
) -> list[tuple[int, int]]:
    valid_associations: list[tuple[int, int]] = []

    for line_legacy_id, station_legacy_id in LINE_STATION_ASSOCIATIONS_FIXED:
        line_api_id = line_legacy_to_api_id.get(line_legacy_id)
        station_api_id = station_legacy_to_api_id.get(station_legacy_id)

        if line_api_id is None:
            print(f"⚠️ line export id introuvable dans line_legacy_to_api_id: {line_legacy_id}")
            continue

        if station_api_id is None:
            print(f"⚠️ station export id introuvable dans station_legacy_to_api_id: {station_legacy_id}")
            continue

        valid_associations.append((line_api_id, station_api_id))

    return valid_associations


def gen_erp_groups_api(token: str, window_start: datetime) -> dict[str, int]:
    """
    Create ERP groups through the API.
    Returns a mapping: process step code -> ERP group API id
    """

    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)

    headers = _auth_headers(token)
    now = datetime.now()
    ERP_GROUPS_URL = f"{BASE_URL}/erp-groups/erp-groups/"

    erp_group_code_to_api_id: dict[str, int] = {}

    for code, desc, _ in PCB_PROCESS_STEPS:
        erpgroup_no = f"ERP-{code}"

        payload = {
            "state": 1,
            "erpgroup_no": erpgroup_no,
            "erp_group_description": desc,
            "erpsystem": "SAP",
            "sequential": True,
            "separate_station": False,
            "fixed_layer": False,
            "created_on": window_start.isoformat(),
            "edited_on": now.isoformat(),
            "modified_by": 1,
            "user_id": 1,
            "cst_id": None,
            "valid": True,
        }

        print("POST URL:", ERP_GROUPS_URL)
        print("PAYLOAD:", payload)

        r = requests.post(
            ERP_GROUPS_URL,
            json=payload,
            headers=headers,
            timeout=30,
            verify=False,
        )

        print("STATUS:", r.status_code)
        print("RESPONSE:", r.text)

        if r.status_code in (200, 201):
            api_id = int(r.json()["id"])
            erp_group_code_to_api_id[code] = api_id
            print(f"✅ created ERP group {erpgroup_no} -> {api_id}")
            continue

        if r.status_code in (400, 409, 422):
            g = requests.get(
                ERP_GROUPS_URL,
                headers=headers,
                timeout=30,
                verify=False,
            )

            if g.status_code == 200:
                body = g.json()
                items = body["results"] if isinstance(body, dict) and "results" in body else body

                found = False
                for item in items:
                    if item.get("erpgroup_no") == erpgroup_no:
                        api_id = int(item["id"])
                        erp_group_code_to_api_id[code] = api_id
                        print(f"♻️ reused ERP group {erpgroup_no} -> {api_id}")
                        found = True
                        break

                if not found:
                    print(f"⚠️ ERP group existe peut-être mais introuvable: {erpgroup_no}")
            else:
                print(f"❌ GET ERP groups failed: {g.status_code}")

            continue

        r.raise_for_status()

    print("ERP groups créés/réutilisés :", erp_group_code_to_api_id)
    return erp_group_code_to_api_id


def gen_failure_types_api(
    token: str,
    failure_group_ids: list[int],
    site_ids: list[int],
) -> dict[str, int]:
    """
    Create failure types through the API.
    Returns a mapping: failure_type_code -> API failure_type_id
    """

    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)

    headers = _auth_headers(token)
    now = datetime.now()
    FAILURE_TYPES_URL = f"{BASE_URL}/failure-types/failure-types/"

    failures = [
        ("FT-SB01", "Solder Bridge",           0),
        ("FT-SB02", "Insufficient Solder",     0),
        ("FT-SB03", "Solder Ball",             0),
        ("FT-CM01", "Missing Component",       1),
        ("FT-CM02", "Wrong Component",         1),
        ("FT-CM03", "Tombstoning",             1),
        ("FT-CM04", "Component Rotation Error",1),
        ("FT-PB01", "PCB Scratch",             2),
        ("FT-PB02", "Board Contamination",     2),
        ("FT-PR01", "Paste Insufficiency",     3),
        ("FT-PR02", "Paste Bridging",          3),
        ("FT-PR03", "Placement Offset",        3),
        ("FT-EL01", "Short Circuit",           4),
        ("FT-EL02", "Open Circuit",            4),
        ("FT-EL03", "ESD Damage",              4),
        ("FT-CS01", "Flux Residue",            5),
        ("FT-CS02", "Cosmetic Scratch",        5),
    ]

    failure_type_code_to_id: dict[str, int] = {}

    for i, (base_code, desc, grp) in enumerate(failures):
        failure_type_code = base_code
        payload = {
            "failure_type_code": failure_type_code,
            "failure_type_desc": desc,
            "site_id": site_ids[i % len(site_ids)],
            "failure_group_id": failure_group_ids[grp % len(failure_group_ids)],
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }

        print("POST URL:", FAILURE_TYPES_URL)
        print("PAYLOAD:", payload)

        r = requests.post(
            FAILURE_TYPES_URL,
            json=payload,
            headers=headers,
            timeout=30,
            verify=False,
        )

        print("STATUS:", r.status_code)
        print("RESPONSE:", r.text)

        if r.status_code in (200, 201):
            api_id = int(r.json()["failure_type_id"])
            failure_type_code_to_id[failure_type_code] = api_id
            print(f"✅ created failure type {failure_type_code} -> {api_id}")
            continue

        if r.status_code in (400, 409, 422):
            g = requests.get(
                FAILURE_TYPES_URL,
                headers=headers,
                timeout=30,
                verify=False,
            )

            if g.status_code == 200:
                body = g.json()
                items = body["results"] if isinstance(body, dict) and "results" in body else body

                found = False
                for item in items:
                    if item.get("failure_type_code") == failure_type_code:
                        api_id = int(item["failure_type_id"])
                        failure_type_code_to_id[failure_type_code] = api_id
                        print(f"♻️ reused failure type {failure_type_code} -> {api_id}")
                        found = True
                        break

                if not found:
                    print(f"⚠️ failure type existe peut-être mais introuvable: {failure_type_code}")
            else:
                print(f"❌ GET failure types failed: {g.status_code}")

            continue

        r.raise_for_status()

    print("Failure types créés/réutilisés :", failure_type_code_to_id)
    return failure_type_code_to_id

def gen_machine_conditions_ref_api(
    token: str,
    machine_condition_group_ids: list[int],
) -> dict[str, int]:
    """
    Create/reuse machine conditions through the API.
    Returns a mapping: condition code -> API condition id
    """

    if not machine_condition_group_ids:
        raise RuntimeError("machine_condition_group_ids est vide.")

    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)

    headers = _auth_headers(token)
    now = datetime.now()
    MACHINE_CONDITIONS_URL = f"{BASE_URL}/machine-conditions/machine-conditions/"

    condition_code_to_id: dict[str, int] = {}

    for c in ALLOWED_CONDITIONS:
        group_api_id = machine_condition_group_ids[
            (c["group_id"] - 1) % len(machine_condition_group_ids)
        ]

        payload = {
            "group_id": group_api_id,
            "condition_name": c["code"],
            "condition_description": c["desc"],
            "color_rgb": c["color"],
            "is_active": True,
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }

        print("POST URL:", MACHINE_CONDITIONS_URL)
        print("PAYLOAD:", payload)

        r = requests.post(
            MACHINE_CONDITIONS_URL,
            json=payload,
            headers=headers,
            timeout=30,
            verify=False,
        )

        print("STATUS:", r.status_code)
        print("RESPONSE:", r.text)

        if r.status_code in (200, 201):
            data = _get_json_or_text(r)
            api_id = int(data["id"])
            condition_code_to_id[c["code"]] = api_id
            print(f"✅ created machine condition {c['code']} -> {api_id}")
            continue

        if r.status_code in (400, 409, 422):
            data = _get_json_or_text(r)
            detail = str(data.get("detail", ""))

            existing_id = _extract_id_from_detail(detail)
            if existing_id is not None:
                condition_code_to_id[c["code"]] = existing_id
                print(f"♻️ reused machine condition {c['code']} -> {existing_id}")
                continue

            existing_id = _get_existing_id_by_get(
                "machine-conditions/machine-conditions/",
                token,
                wanted_value=c["code"],
                name_key_candidates=("condition_name", "name"),
            )
            if existing_id is not None:
                condition_code_to_id[c["code"]] = existing_id
                print(f"♻️ reused machine condition {c['code']} -> {existing_id}")
                continue

            raise RuntimeError(
                f"Machine condition '{c['code']}' already exists, but ID could not be resolved."
            )

        r.raise_for_status()

    print("Machine conditions créées/réutilisées :", condition_code_to_id)
    return condition_code_to_id


def gen_part_groups_api(
    token: str,
    part_group_type_ids: list[int],
) -> dict[str, int]:
    """
    Create/reuse part groups through the API.
    Returns a mapping: part group name -> API id
    """

    if not part_group_type_ids:
        raise RuntimeError("part_group_type_ids est vide.")

    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)

    headers = _auth_headers(token)
    now = datetime.now()
    PART_GROUPS_URL = f"{BASE_URL}/part-groups/part-groups/"

    groups = [
        ("PCBA-FG",   "Finished PCB Assemblies",   0, "EA"),
        ("PCBA-WIP",  "WIP PCB Sub-Assemblies",    1, "EA"),
        ("SMD-COMP",  "SMD Components Pool",       2, "EA"),
        ("THT-COMP",  "THT Components Pool",       2, "EA"),
        ("RAW-PCB",   "Bare PCB Boards",           3, "EA"),
        ("CONSM",     "Consumables (solder/flux)", 3, "KG"),
    ]

    part_group_name_to_id: dict[str, int] = {}

    for i, (nm, ds, gti, pt) in enumerate(groups):
        group_name = nm

        payload = {
            "name": group_name,
            "description": ds,
            "user_id": 1,
            "part_type": pt,
            "costs": random.randint(10, 500),
            "is_active": True,
            "circulating_lot": random.randint(50, 500),
            "automatic_emptying": 0,
            "master_workplan": None,
            "comment": None,
            "state": 1,
            "material_transfer": False,
            "created_on": now.isoformat(),
            "edited_on": now.isoformat(),
            "part_group_type_id": part_group_type_ids[gti % len(part_group_type_ids)],
        }

        print("POST URL:", PART_GROUPS_URL)
        print("PAYLOAD:", payload)

        r = requests.post(
            PART_GROUPS_URL,
            json=payload,
            headers=headers,
            timeout=30,
            verify=False,
        )

        print("STATUS:", r.status_code)
        print("RESPONSE:", r.text)

        if r.status_code in (200, 201):
            data = _get_json_or_text(r)
            api_id = int(data["id"])
            part_group_name_to_id[group_name] = api_id
            print(f"✅ created part group {group_name} -> {api_id}")
            continue

        if r.status_code in (400, 409, 422):
            data = _get_json_or_text(r)
            detail = str(data.get("detail", ""))

            existing_id = _extract_id_from_detail(detail)
            if existing_id is not None:
                part_group_name_to_id[group_name] = existing_id
                print(f"♻️ reused part group {group_name} -> {existing_id}")
                continue

            existing_id = _get_existing_id_by_get(
                "part-groups/part-groups/",
                token,
                wanted_value=group_name,
                name_key_candidates=("name",),
            )
            if existing_id is not None:
                part_group_name_to_id[group_name] = existing_id
                print(f"♻️ reused part group {group_name} -> {existing_id}")
                continue

            raise RuntimeError(
                f"Part group '{group_name}' already exists, but ID could not be resolved."
            )

        r.raise_for_status()

    print("Part groups créés/réutilisés :", part_group_name_to_id)
    return part_group_name_to_id


def gen_part_master_api(
    token: str,
    part_type_ids: list[int],
    part_group_map: dict[str, int],
    machine_group_ids: list[int],
    site_ids: list[int],
    unit_id: int,
    n_products: int = 8,
) -> tuple[dict[str, int], list[str]]:
    """
    Create/reuse part master entries through the API.
    Returns:
        - part_number_to_id
        - list of part_numbers
    """

    if not part_type_ids:
        raise RuntimeError("part_type_ids est vide.")
    if not part_group_map:
        raise RuntimeError("part_group_map est vide.")
    if not machine_group_ids:
        raise RuntimeError("machine_group_ids est vide.")
    if not site_ids:
        raise RuntimeError("site_ids est vide.")
    if not unit_id:
        raise RuntimeError("unit_id est vide ou invalide.")

    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)

    headers = _auth_headers(token)
    now = datetime.now()
    PART_MASTER_URL = f"{BASE_URL}/part-masters/part-master/"

    rows: list[dict[str, Any]] = []
    product_names = [
        ("PCB-CTL-001", "Motor Controller Board v1.2"),
        ("PCB-PSU-002", "Power Supply Unit 24V/10A"),
        ("PCB-COM-003", "Communication Gateway PCB"),
        ("PCB-SEN-004", "Sensor Interface Board"),
        ("PCB-DRV-005", "LED Driver Board 3-channel"),
        ("PCB-IOT-006", "IoT Node Board - LoRa"),
        ("PCB-HMI-007", "HMI Touch Controller"),
        ("PCB-AMP-008", "Audio Amplifier Board 2x50W"),
    ]

    # Produits finis
    for i, (pn, desc) in enumerate(product_names[:n_products]):
        payload = {
            "part_number": pn,
            "description": desc,
            "part_status": "active",
            "parttype_id": part_type_ids[0],
            "partgroup_id": part_group_map["PCBA-FG"],
            "case_type": random.choice(["SMT", "MIXED"]),
            "product": True,
            "panel": True,
            "variant": False,
            "machine_group_id": machine_group_ids[1 % len(machine_group_ids)],
            "material_info": "FR4 1.6mm HASL",
            "parts_index": i + 1,
            "edit_order_based_bom": False,
            "site_id": site_ids[i % len(site_ids)],
            "unit_id": unit_id,
            "material_code": f"MAT-{pn}",
            "no_of_panels": random.choice([1, 2, 4]),
            "customer_material_number": f"CUST-{pn}",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }

        # enlève seulement les champs optionnels à None
        payload = {k: v for k, v in payload.items() if v is not None}
        rows.append(payload)

    # Composants
    comp_offset = len(rows)
    for ci, (prefix, ctype, pkg) in enumerate(PCB_COMPONENT_TYPES):
        for vi in range(3):
            pn = f"{prefix}{ci+1:02d}{vi+1:02d}"
            payload = {
                "part_number": pn,
                "description": f"{ctype} {pkg} variant {vi+1}",
                "part_status": "active",
                "parttype_id": part_type_ids[2 % len(part_type_ids)],
                "partgroup_id": (
                    part_group_map["SMD-COMP"]
                    if prefix in {"R", "C", "U", "Q", "D", "L", "F"}
                    else part_group_map["THT-COMP"]
                ),
                "case_type": pkg,
                "product": False,
                "panel": False,
                "variant": vi > 0,
                "machine_group_id": None,
                "material_info": None,
                "parts_index": comp_offset + ci * 3 + vi + 1,
                "edit_order_based_bom": False,
                "site_id": site_ids[0],
                "unit_id": unit_id,
                "material_code": f"MAT-{pn}",
                "no_of_panels": 1,
                "customer_material_number": None,
                "created_at": now.isoformat(),
                "updated_at": now.isoformat(),
            }

            payload = {k: v for k, v in payload.items() if v is not None}
            rows.append(payload)

    part_number_to_id: dict[str, int] = {}
    created_part_numbers: list[str] = []

    for payload in rows:
        print("POST URL:", PART_MASTER_URL)
        print("PAYLOAD:", payload)

        r = requests.post(
            PART_MASTER_URL,
            json=payload,
            headers=headers,
            timeout=30,
            verify=False,
        )

        print("STATUS:", r.status_code)
        print("RESPONSE:", r.text)

        part_number = payload["part_number"]

        if r.status_code in (200, 201):
            data = _get_json_or_text(r)
            api_id = int(data["id"])
            part_number_to_id[part_number] = api_id
            created_part_numbers.append(part_number)
            print(f"✅ created part {part_number} -> {api_id}")
            continue

        if r.status_code == 422:
            raise RuntimeError(
                f"Payload invalide pour part '{part_number}': {r.text}"
            )

        if r.status_code in (400, 409):
            data = _get_json_or_text(r)
            detail = str(data.get("detail", ""))

            existing_id = _extract_id_from_detail(detail)
            if existing_id is not None:
                part_number_to_id[part_number] = existing_id
                created_part_numbers.append(part_number)
                print(f"♻️ reused part {part_number} -> {existing_id}")
                continue

            existing_id = _get_existing_id_by_get(
                "part-masters/part-master/",
                token,
                wanted_value=part_number,
                name_key_candidates=("part_number", "name"),
            )
            if existing_id is not None:
                part_number_to_id[part_number] = existing_id
                created_part_numbers.append(part_number)
                print(f"♻️ reused part {part_number} -> {existing_id}")
                continue

            raise RuntimeError(
                f"Part '{part_number}' already exists, but ID could not be resolved."
            )

        r.raise_for_status()

    print("Part master créés/réutilisés :", part_number_to_id)
    return part_number_to_id, created_part_numbers


# ---------------------------------------------------------------------------
# Layer 2
# ---------------------------------------------------------------------------


def gen_assign_stations_to_erpgrp_api(
    token: str,
    station_ids: list[int],
    erp_group_ids: list[int],
) -> list[tuple[int, int]]:
    """
    Create/reuse station <-> ERP group assignments through the API.
    Returns a list of valid (station_id, erp_group_id) assignments.
    """

    if not station_ids:
        raise RuntimeError("station_ids est vide.")
    if not erp_group_ids:
        raise RuntimeError("erp_group_ids est vide.")

    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)

    headers = _auth_headers(token)
    url = f"{BASE_URL}/assign-stations/assign-stations-to-erpgrp/"

    created_pairs: list[tuple[int, int]] = []

    for i, sid in enumerate(station_ids):
        erp_group_id = erp_group_ids[i % len(erp_group_ids)]

        payload = {
            "station_id": sid,
            "erp_group_id": erp_group_id,
            "station_type": "production",
            "user_id": 1,
        }

        print("POST URL:", url)
        print("PAYLOAD:", payload)

        try:
            r = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=30,
                verify=False,
            )
        except requests.exceptions.RequestException as e:
            raise RuntimeError(
                f"Erreur réseau pendant l'assignation station {sid} -> ERP group {erp_group_id}: {e}"
            ) from e

        print("STATUS:", r.status_code)
        print("RESPONSE:", r.text)

        if r.status_code in (200, 201):
            created_pairs.append((sid, erp_group_id))
            print(f"✅ assigned station {sid} -> ERP group {erp_group_id}")
            continue

        if r.status_code == 422:
            raise RuntimeError(
                f"Payload invalide pour station {sid} -> ERP group {erp_group_id}: {r.text}"
            )

        if r.status_code in (400, 409):
            # Selon le backend, ça peut vouloir dire "already exists"
            # On considère l'association comme existante si le backend refuse pour doublon.
            created_pairs.append((sid, erp_group_id))
            print(f"♻️ assignment déjà existant ou refusé en doublon: station {sid} -> ERP group {erp_group_id}")
            continue

        r.raise_for_status()

    print("Assignations station -> ERP group :", created_pairs[:10])
    return created_pairs

def gen_work_plans_api(
    site_ids: list[int],
    client_ids: list[int],
    company_code_ids: list[int],
    product_part_master_ids: list[str],
    window_start: datetime,
    window_end: datetime,
    token: str
) -> list[dict[str, Any]]:

    headers = _auth_headers(token)
    created = []

    valid_from = window_start - timedelta(days=30)

    for i, pn in enumerate(product_part_master_ids):

        payload = {
            "version": 1,
            "is_current": True,
            "user_id": 1,
            "site_id": site_ids[i % len(site_ids)],
            "client_id": client_ids[i % len(client_ids)],
            "company_id": company_code_ids[i % len(company_code_ids)],
            "source": 1,
            "status": 1,
            "product_vers_id": i + 1,
            "workplan_status": "R",
            "part_no": pn,
            "part_desc": f"Work plan for {pn}",
            "workplan_desc": f"SMT/THT production plan v1 - {pn}",
            "workplan_type": random.choice(["SMT", "MIX", "THT"]),
            "workplan_version_erp": f"WP-{i+1:04d}-V1",
            "created_at": valid_from.isoformat(),
        }

        r = requests.post(
            f"{BASE_URL}/workplans/workplan/",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False
        )

        print("WP STATUS:", r.status_code)
        print("WP RESPONSE:", r.text)

        if r.status_code in (200, 201):
            data = r.json()

            wp_id = data["id"]

            created.append({
                "id": wp_id,
                "part_no": pn,
                "url": f"{BASE_URL}/workplans/workplan/{wp_id}"
            })
            continue

        if r.status_code in (400, 409):
            data = _get_json_or_text(r)
            detail = str(data.get("detail", ""))
            
            existing_id = _extract_id_from_detail(detail)
            if existing_id is not None:
                created.append({
                    "id": existing_id,
                    "part_no": pn,
                    "url": f"{BASE_URL}/workplans/workplan/{existing_id}",
                })
                print(f"Reusing existing workplan '{pn}' (ID {existing_id})")
                continue
            r.raise_for_status()

    print(f"  + work_plans (API): {len(created)}")
    return created


def gen_work_steps_api(
    workplan_ids: list[int],
    erp_group_ids: list[int],
    window_start: datetime,
    token: str
) -> list[dict[str, Any]]:
    headers = _auth_headers(token)
    now = datetime.now()
    created_steps: list[dict[str, Any]] = []

    for wp_id in workplan_ids:
        for step_i, (code, desc, step_type) in enumerate(PCB_PROCESS_STEPS):
            eid = erp_group_ids[step_i % len(erp_group_ids)]

            payload = {
                "workplan_id": wp_id,
                "erp_group_id": eid,
                "workstep_no": (step_i + 1) * 10,
                "step": step_i + 1,
                "setup_time": round(random.uniform(15, 60), 2),
                "te_person": 1,
                "te_machine": round(AVG_CYCLE_TIME_SEC / 60, 2),
                "te_time_base": 60,
                "te_qty_base": 1,
                "transport_time": round(random.uniform(1, 10), 2),
                "wait_time": round(random.uniform(0, 30), 2),
                "status": 1,
                "equ_id": None,
                "msl_relevant": 0,
                "msl_offset": 0,
                "panel_count": random.choice([1, 2, 4]),
                "workstep_desc": desc,
                "erp_grp_no": f"ERP-{code}",
                "erp_grp_desc": desc,
                "time_unit": "MIN",
                "setup_flag": "X" if step_i == 0 else "",
                "workstep_version_erp": f"WS-{wp_id:04d}-{step_i+1:02d}",
                "info": step_type,
                "confirmation": "AUTO",
                "sequentiell": "X",
                "workstep_type": step_type,
                "traceflag": "X",
                "step_type": random.choice(["manuel", "auto", "semiAuto"]),
                "created_at": window_start.isoformat(),
                "stamp": now.isoformat(),
            }

            r = requests.post(
                f"{BASE_URL}/worksteps/worksteps/",
                json=payload,
                headers=headers,
                timeout=30,
                verify=False
            )

            print("WORKSTEP STATUS:", r.status_code)
            print("WORKSTEP RESPONSE:", r.text)

            if r.status_code in (200, 201):
                data = r.json()
                step_id = data["id"]

                created_steps.append({
                    "id": step_id,
                    "workplan_id": wp_id,
                    "url": f"{BASE_URL}/worksteps/worksteps/{step_id}",
                })
                continue
            
            if r.status_code in (400, 422):
                data = _get_json_or_text(r)
                print(f"Skipping workstep for workplan {wp_id}: {data}")
                continue
            
            if r.status_code == 409:
                data = _get_json_or_text(r)
                detail = str(data.get("detail", ""))
                existing_id = _extract_id_from_detail(detail)
                
                if existing_id is not None:
                    created_steps.append({
                        "id": existing_id,
                        "workplan_id": wp_id,
                        "url": f"{BASE_URL}/worksteps/workstep/{existing_id}",
                    })
                    continue
                
                r.raise_for_status()

    print(f"  + work_steps (API): {len(created_steps)}")
    return created_steps

def gen_bom_headers_api(
    product_part_master_ids: list[int],
    window_start: datetime,
    token: str,
) -> list[dict[str, Any]]:

    headers = _auth_headers(token)
    created = []
    now = datetime.now()
    vf = window_start - timedelta(days=60)

    for pm_id in product_part_master_ids:

        payload = {
            "description": f"BOM for {pm_id}",
            "valid_from": vf.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "valid_to": None,
            "part_master_id": pm_id,
            "created_by": "system",
            "updated_by": "system",
            "state": "draft",
            "version": 1,
            "is_current": True,
            "previous_version_id": None,
        }

        r = requests.post(
            f"{BASE_URL}/bom-headers/bom/headers/",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False,
        )

        print("BOM HEADER STATUS:", r.status_code)
        print("BOM HEADER RESPONSE:", r.text)

        if r.status_code in (200, 201):
            data = r.json()
            hid = data["id"]
            created.append({
                "id": hid,
                "part_master_id": pm_id,
                "url": f"{BASE_URL}/bom-headers/bom/headers/{hid}",
            })
            continue

        if r.status_code == 409:
            data = _get_json_or_text(r)
            detail = str(data.get("detail", ""))

            existing_id = _extract_id_from_detail(detail)
            if existing_id is not None:
                created.append({
                    "id": existing_id,
                    "part_master_id": pm_id,
                    "url": f"{BASE_URL}/bom-headers/bom/headers/{existing_id}",
                })
                continue

        print(f"Skipping BOM header for part_master_id={pm_id}")

    return created


def gen_bom_items_api(
    bom_header_rows: list[dict],
    component_ids: list[int],
    token: str
):

    headers = _auth_headers(token)

    for row in bom_header_rows:
        bh_id = row["id"]

        for i in range(random.randint(5, 15)):
            payload = {
                "bom_header_id": bh_id,
                "part_master_id": random.choice(component_ids),
                "quantity": random.randint(1, 10),
                "is_product": False,
                "component_name": f"Comp-{i}",
                "layer": random.choice([1, 2]),
            }

            r = requests.post(
                f"{BASE_URL}/bom-items/bom/items/",
                json=payload,
                headers=headers,
                timeout=30,
                verify=False,
            )

            print("BOM ITEM STATUS:", r.status_code)

def gen_boms_api(
    product_part_master_ids: list[int],
    window_start: datetime,
    token: str,
) -> list[dict[str, Any]]:
    headers = _auth_headers(token)
    created = []
    vf = window_start - timedelta(days=60)

    for pm_id in product_part_master_ids:
        payload = {
            "state": "released",
            "bom_type": random.choice(["PRODUCTION", "ENGINEERING"]),
            "bom_version": 1,
            "bom_version_valid_from": _d_str(vf),
            "bom_version_valid_to": None,
            "user_id": 1,
            "part_number": pm_id,
        }

        r = requests.post(
            f"{BASE_URL}/boms/bom/",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False,
        )

        print("BOM STATUS:", r.status_code)
        print("BOM RESPONSE:", r.text)

        if r.status_code in (200, 201):
            data = r.json()
            created.append({
                "id": int(data["id"]),
                "part_number": pm_id,
                "url": data.get("url") or f"{BASE_URL}/boms/bom/{data['id']}",
            })
            continue

        if r.status_code == 409:
            data = _get_json_or_text(r)
            detail = str(data.get("detail", ""))
            existing_id = _extract_id_from_detail(detail)

            if existing_id is not None:
                created.append({
                    "id": existing_id,
                    "part_number": pm_id,
                    "url": f"{BASE_URL}/boms/bom/{existing_id}",
                })
                continue

        print(f"Skipping BOM for part_master_id={pm_id}: {_get_json_or_text(r)}")

    return created

            
def gen_bom_insertion_api(
    product_part_master_ids: list[str],
    token: str
):

    headers = _auth_headers(token)

    for pn in product_part_master_ids:

        payload = {
            "part_number": pn,
            "bom_master_version": 1,
            "workplan_master_version": 1,
        }

        r = requests.post(
            f"{BASE_URL}/api/merge/bom-insertions",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False,
        )

        print("BOM INSERT STATUS:", r.status_code)
# ---------------------------------------------------------------------------
# Layer 3
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class WOWindow:
    workorder_id: int
    part_number: str
    part_id: int
    company_id: int
    event_start: datetime
    event_end: datetime
    state: str
    qty: int



def gen_work_orders_api(
    client_ids: list[int],
    company_code_ids: list[int],
    site_ids: list[int],
    product_part_master_ids: list[str],
    product_part_ids: list[int],
    workplan_by_part_no: dict[str, dict[str, Any]],
    window_start: datetime,
    window_end: datetime,
    n: int,
    wo_qty_min: int,
    wo_qty_max: int,
    token: str,
) -> tuple[list[int], dict[int, WOWindow]]:
    headers = _auth_headers(token)
    plan = _state_plan(n)
    created_ids: list[int] = []
    windows: dict[int, WOWindow] = {}

    for i in range(n):
        state = plan[i] if i < len(plan) else "active"

        pn_idx = random.randrange(len(product_part_master_ids))
        pn = product_part_master_ids[pn_idx]
        pid = product_part_ids[pn_idx]
        cid = company_code_ids[i % len(company_code_ids)]
        qty = random.randint(wo_qty_min, wo_qty_max)
        dur = _wo_duration(qty)

        if state in {"delivered", "finished"}:
            d_lo = window_start + dur
            d_hi = window_end - timedelta(hours=1)
            delivery_dt = _clamp_dt(_random_between(d_lo, d_hi), d_lo, d_hi)
            delivery_dt = delivery_dt.replace(minute=0, second=0, microsecond=0)
            start_dt = _snap_shift_start(delivery_dt - dur)

        elif state in {"active", "open"}:
            s_lo = max(window_start, window_end - dur * 2)
            s_hi = window_end - timedelta(hours=2)
            start_dt = _snap_shift_start(_random_between(s_lo, s_hi))
            delivery_dt = start_dt + dur

        else:
            start_dt = _snap_shift_start(
                window_end + timedelta(hours=random.randint(1, 720))
            )
            delivery_dt = start_dt + dur

        created_hi = _clamp_dt(start_dt - timedelta(hours=1), window_start, window_end)
        created_dt = (
            window_start
            if created_hi <= window_start
            else _random_between(window_start, created_hi)
        )

        stamp_dt = _clamp_dt(
            _random_between(created_dt + timedelta(minutes=1), start_dt),
            window_start,
            window_end,
        )

        aps_s = start_dt + timedelta(seconds=random.randint(-3600, 3600))
        aps_e = delivery_dt + timedelta(seconds=random.randint(-3600, 3600))
        if aps_e < aps_s:
            aps_e = aps_s + timedelta(hours=1)

        wp = workplan_by_part_no.get(pn)

        payload = {
            "workorder_no": f"WO-{random.randint(100000, 999999)}",
            "workorder_type": random.choice(["P", "R", "T"]),
            "part_number": pn,
            "workorder_qty": qty,
            "startdate": _dt_str(start_dt),
            "deliverydate": _dt_str(delivery_dt),
            "unit": random.choice(["EA", "PCS"]),
            "bom_version": "1",
            "workplan_type": random.choice(["SMT", "MIX", "THT"]),
            "backflush": "",  # <- correction
            "source": 1,
            "workplan_version": "1",
            "workorder_desc": f"{pn} production batch",
            "bom_info": None,
            "workplan_valid_from": _dt_str(window_start - timedelta(days=60)),
            "workorder_no_ext": f"EXT-{random.randint(100000, 999999)}",
            "info1": None,
            "info2": None,
            "info3": None,
            "info4": None,
            "info5": None,
            "ninfo1": None,
            "ninfo2": None,
            "status": "R",
            "created": _dt_str(created_dt),
            "stamp": _dt_str(stamp_dt),
            "site_id": site_ids[i % len(site_ids)],
            "client_id": client_ids[i % len(client_ids)],
            "company_id": cid,
            "drawing_no": f"DWG-{pn}-R1",
            "workorder_state": state[0].upper() if state else "A",
            "parent_workorder": None,
            "controller": "SAP",
            "bareboard_no": f"BB-{random.randint(100000, 999999)}",
            "aps_planning_start_date": _dt_str(aps_s),
            "aps_planning_stamp": _dt_str(aps_e),
            "aps_planning_end_date": _dt_str(aps_e),
            "aps_order_fixation": None,
            "workplan_id": wp["id"] if wp else None,
            "workplan_url": wp["url"] if wp else None,
        }

        r = requests.post(
            f"{BASE_URL}/workorders/workorders/",  # <- correction endpoint
            json=payload,
            headers=headers,
            timeout=30,
            verify=False,
        )

        print("WORKORDER STATUS:", r.status_code)
        print("WORKORDER RESPONSE:", r.text)

        if r.status_code in (200, 201):
            data = r.json()
            wo_id = int(data["id"])
            created_ids.append(wo_id)

            ev_start = max(start_dt, window_start)
            ev_end = min(delivery_dt, window_end)
            if state in {"active", "open"}:
                ev_end = max(ev_start + timedelta(hours=1), window_end)

            if ev_end > ev_start and state != "planned":
                windows[wo_id] = WOWindow(
                    wo_id, pn, pid, cid, ev_start, ev_end, state, qty
                )

            wo_url = data.get("url") or f"{BASE_URL}/workorders/workorders/{wo_id}"
            print(f"Created workorder id={wo_id} url={wo_url}")
            continue

        if r.status_code ==409:
            data = _get_json_or_text(r)
            detail = str(data.get("detail", ""))

            existing_id = _extract_id_from_detail(detail)
            
            if existing_id is not None:
                created_ids.append(existing_id)
                print(f"Reusing existing workorder '{pn}' (ID {existing_id})")
                continue
            
            raise RuntimeError(f"Workorder exists but ID not found for {pn}")

        if r.status_code == 400:
            data = _get_json_or_text(r)
            print(f"Skipping workorder for {pn} because API returned 400: {data}")
            continue
        
        r.raise_for_status()

    print(f"  + work_orders (API): {len(created_ids)}")
    return created_ids, windows


def gen_serial_numbers_api(
    wo_windows: dict[int, WOWindow],
    token: str,
) -> dict[int, list[int]]:
    headers = _auth_headers(token)
    wo_snr_map: dict[int, list[int]] = {}

    for wid, w in wo_windows.items():
        target = min(max(1, w.qty), 2000)
        snr_ids: list[int] = []

        for pos in range(1, target + 1):
            ts = _clamp_dt(
                w.event_start + timedelta(seconds=pos * AVG_CYCLE_TIME_SEC),
                w.event_start,
                w.event_end,
            )

            payload = {
                "serial_number": f"SNR-WO-{wid:06d}-{pos:05d}",
                "serial_number_pos": pos,
                "serial_number_ref_pos": pos,
                "serial_number_active": "Y",
                "serial_number_ref": f"SNR-WO-{wid:06d}-{pos:05d}",
                "splitted": False,
                "workorder_id": wid,
                "part_id": w.part_id,
                "customer_part_number": f"CUST-{w.part_id}",
                "workorder_type": "S",
                "serial_number_type": "S",
                "cluster_name": "ASSEMBLY",
                "cluster_type": "O",
                "created_on": ts.astimezone().isoformat(),
                "created_by": 1,
                "company_code_id": w.company_id,
            }

            r = requests.post(
                f"{BASE_URL}/api/serialnumbers/",
                json=payload,
                headers=headers,
                timeout=30,
                verify=False,
            )

            print("SNR STATUS:", r.status_code)
            print("SNR RESPONSE:", r.text)

            if r.status_code in (200, 201):
                data = r.json()
                snr_ids.append(int(data["id"]))
                continue

            if r.status_code in (400, 409, 422):
                print(f"Skipping serial number for WO {wid}: {_get_json_or_text(r)}")
                continue

            r.raise_for_status()

        wo_snr_map[wid] = snr_ids

    print(f"  + serial_numbers (API): {sum(len(v) for v in wo_snr_map.values())}")
    return wo_snr_map


def gen_active_workorders_api(
    wo_windows: dict[int, WOWindow],
    station_ids: list[int],
    window_end: datetime,
    n: int,
    token: str,
) -> list[dict[str, Any]]:
    headers = _auth_headers(token)

    active_wids = [
        wid for wid, w in wo_windows.items()
        if w.state in {"active", "open"}
    ]

    if not active_wids:
        print("  - active_workorders: no active/open WOs")
        return []

    now = datetime.now()
    n = min(n, len(active_wids))
    created: list[dict[str, Any]] = []

    for i in range(n):
        wid = active_wids[i % len(active_wids)]

        created_at = _clamp_dt(
            now - timedelta(hours=random.uniform(0.5, 8)),
            window_end - timedelta(days=1),
            now,
        )
        updated_at = _clamp_dt(
            created_at + timedelta(minutes=random.randint(5, 60)),
            created_at,
            now,
        )

        payload = {
            "workorder_id": wid,
            "station_id": station_ids[i % len(station_ids)],
            "state": 1,
            "process_layer": 0,
            "created_at": created_at.astimezone().isoformat(),
            "updated_at": updated_at.astimezone().isoformat(),
        }

        r = requests.post(
            f"{BASE_URL}/active-workorders/active-workorders/",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False,
        )

        print("ACTIVE WO STATUS:", r.status_code)
        print("ACTIVE WO RESPONSE:", r.text)

        if r.status_code in (200, 201):
            data = r.json()
            active_id = int(data["id"])
            created.append({
                "id": active_id,
                "workorder_id": wid,
                "url": data.get("url") or f"{BASE_URL}/active-workorders/active-workorders/{active_id}",
            })
            continue

        if r.status_code in (400, 422):
            data = _get_json_or_text(r)
            print(f"Skipping active_workorder for workorder {wid}: {data}")
            continue

        if r.status_code == 409:
            data = _get_json_or_text(r)
            detail = str(data.get("detail", ""))

            existing_id = _extract_id_from_detail(detail)
            if existing_id is not None:
                created.append({
                    "id": existing_id,
                    "workorder_id": wid,
                    "url": f"{BASE_URL}/active-workorders/active-workorders/{existing_id}",
                })
                print(f"Reusing active_workorder for workorder {wid} (ID {existing_id})")
                continue

        r.raise_for_status()

    print(f"  + active_workorders (API): {len(created)}")
    return created
# ---------------------------------------------------------------------------
# Layer 4
# ---------------------------------------------------------------------------


def gen_bookings_api(
    wo_windows: dict[int, WOWindow],
    wo_snr_map: dict[int, list[int]],
    station_ids: list[int],
    failure_type_ids: list[int],
    pass_p: float,
    fail_p: float,
    scrap_p: float,
    target_bookings: int,
    window_start: datetime,
    window_end: datetime,
    token: str,
) -> list[dict[str, Any]]:

    headers = _auth_headers(token)
    created: list[dict[str, Any]] = []

    eligible = {
        wid: w for wid, w in wo_windows.items()
        if w.state in {"open", "active", "finished", "delivered"}
    }

    if not eligible:
        print("  - bookings: no eligible work orders")
        return []

    wo_list = list(eligible.values())

    wo_wins: dict[int, list[tuple[datetime, datetime]]] = {
        w.workorder_id: _shift_windows(w.event_start, w.event_end)
        for w in wo_list
    }

    def _state_for(wo_state: str) -> str:
        bp = min(1.0, pass_p + (0.05 if wo_state in {"delivered", "finished"} else 0))
        r = random.random()
        if r < bp:
            return "pass"
        if r < bp + fail_p:
            return "fail"
        return "scrap"

    n_stations = len(station_ids)
    n_fail_types = len(failure_type_ids)
    n_meas = len(MEASUREMENT_CATALOG)

    print(f"  Generating {target_bookings:,} bookings via API...")

    for i in range(target_bookings):
        w = random.choices(wo_list, weights=[w.qty for w in wo_list], k=1)[0]
        wins = wo_wins[w.workorder_id]

        if wins:
            seg = random.choice(wins)
            ts = _random_between(seg[0], seg[1])
        else:
            ts = _random_between(w.event_start, w.event_end)

        st = _state_for(w.state)

        failed_id = None
        if st in {"fail", "scrap"} and failure_type_ids:
            failed_id = failure_type_ids[random.randint(0, n_fail_types - 1)]

        updated_at = _clamp_dt(
            ts + timedelta(seconds=random.randint(0, 300)),
            ts,
            datetime.now(),
        )

        rct = max(30.0, random.gauss(AVG_CYCLE_TIME_SEC, 30.0))

        snr_ids = wo_snr_map.get(w.workorder_id, [])
        snr_id = snr_ids[random.randint(0, len(snr_ids) - 1)] if snr_ids else None

        payload = {
            "workorder_id": w.workorder_id,
            "station_id": station_ids[i % n_stations],
            "failed_id": failed_id,
            "serial_number_id": snr_id,
            "process_layer": random.randint(0, 3),
            "date_of_booking": ts.astimezone().isoformat(),
            "state": st,
            "mesure_id": None,
            "real_cycle_time": round(rct, 3),
            "type": "SNR" if snr_id else "batch",
            "snr_booking": bool(snr_id),
            "booked_by": "Admin",
            "created_at": ts.astimezone().isoformat(),
            "updated_at": updated_at.astimezone().isoformat(),
        }

        r = requests.post(
            f"{BASE_URL}/bookings/bookings/",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False,
        )

        print("BOOKING STATUS:", r.status_code)
        print("BOOKING RESPONSE:", r.text)

        if r.status_code in (200, 201):
            data = r.json()
            booking_id = int(data["id"])
            created.append({
                "id": booking_id,
                "workorder_id": w.workorder_id,
                "url": data.get("url") or f"{BASE_URL}/bookings/bookings/{booking_id}",
            })
            continue

        if r.status_code in (400, 422):
            data = _get_json_or_text(r)
            print(f"Skipping booking for WO {w.workorder_id}: {data}")
            continue

        if r.status_code == 409:
            data = _get_json_or_text(r)
            detail = str(data.get("detail", ""))
            existing_id = _extract_id_from_detail(detail)

            if existing_id is not None:
                created.append({
                    "id": existing_id,
                    "workorder_id": w.workorder_id,
                    "url": f"{BASE_URL}/bookings/bookings/{existing_id}",
                })
                print(f"Reusing booking for WO {w.workorder_id} (ID {existing_id})")
                continue

        r.raise_for_status()

        if len(created) % 100 == 0 and created:
            print(f"    ... {len(created):,} bookings created")

    print(f"  + bookings (API): {len(created):,}")
    return created

def gen_measurement_data_api(
    wo_windows: dict[int, WOWindow],
    station_ids: list[int],
    n: int,
    token: str,
) -> list[dict[str, Any]]:

    headers = _auth_headers(token)
    created = []
    eligible = list(wo_windows.values())

    if not eligible:
        print("  - measurement_data: no eligible work orders")
        return []

    for i in range(n):
        w = random.choice(eligible)
        wins = _shift_windows(w.event_start, w.event_end)

        ts = (
            _random_between(*random.choice(wins))
            if wins else _random_between(w.event_start, w.event_end)
        )

        m = random.choice(MEASUREMENT_CATALOG)
        lo, hi = float(m["lower_limit"]), float(m["upper_limit"])

        val = round(
            random.uniform(lo, hi)
            if random.random() < 0.95
            else random.choice([
                random.uniform(lo * 0.85, lo),
                random.uniform(hi, hi * 1.15),
            ]),
            4,
        )

        fin = ts + timedelta(seconds=random.choice([1.0, 2.0, 3.0]))

        payload = {
            "STATION_ID": station_ids[i % len(station_ids)],
            "WORKORDER_ID": w.workorder_id,
            "BOOK_DATE": ts.isoformat(),
            "MEASURE_NAME": m["measure_name"],
            "MEASURE_VALUE": str(val),
            "LOWER_LIMIT": str(lo),
            "UPPER_LIMIT": str(hi),
            "NOMINAL": str(float(m["nominal"])),
            "TOLERANCE": str(float(m["tolerance"])),
            "MEASURE_FAIL_CODE": 0,
            "MEASURE_TYPE": m["measure_type"],
            "created_at": ts.isoformat(),
            "updated_at": fin.isoformat(),
        }

        r = requests.post(
            f"{BASE_URL}/measurement-data/measurement-data/",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False,
        )

        print("MEASUREMENT STATUS:", r.status_code)
        print("MEASUREMENT RESPONSE:", r.text)

        if r.status_code in (200, 201):
            data = r.json()
            mid = data.get("ID") or data.get("id")
            created.append({
                "id": mid,
                "workorder_id": w.workorder_id,
                "url": data.get("url") or f"{BASE_URL}/measurement-data/measurement-data/{mid}",
            })
            continue

        if r.status_code in (400, 409, 422):
            print(f"Skipping measurement for WO {w.workorder_id}: {_get_json_or_text(r)}")
            continue

        r.raise_for_status()

    print(f"  + measurement_data (API): {len(created)}")
    return created

def gen_machine_condition_data_api(
    station_ids: list[int],
    machine_condition_ids: list[int],
    window_start: datetime,
    window_end: datetime,
    n_max: int,
    downtime_target: float,
    run_min_h: float,
    run_max_h: float,
    down_min_h: float,
    down_max_h: float,
    token: str,
) -> list[dict[str, Any]]:

    headers = _auth_headers(token)
    created = []

    run_min_s = run_min_h * 3600.0
    run_max_s = run_max_h * 3600.0
    down_min_s = down_min_h * 3600.0
    down_max_s = down_max_h * 3600.0

    running_mc_id = machine_condition_ids[-1]
    non_running_ids = machine_condition_ids[:-1]

    if not station_ids or not machine_condition_ids:
        print("  - machine_condition_data: missing station_ids or machine_condition_ids")
        return []

    produced = 0
    total_run = 0.0
    total_down = 0.0

    per_station_max = max(50, n_max // max(1, len(station_ids)))

    for station_id in station_ids:
        tot_s = (window_end - window_start).total_seconds()
        run_left = tot_s * (1.0 - downtime_target)
        down_left = tot_s * downtime_target

        station_produced = 0

        for sw_start, sw_end in _shift_windows(window_start, window_end):
            if station_produced >= per_station_max or produced >= n_max:
                break

            t = sw_start

            while t < sw_end and station_produced < per_station_max and produced < n_max:
                rem = (sw_end - t).total_seconds()
                if rem <= 0:
                    break

                is_down = (
                    random.random() < (down_left / max(1, down_left + run_left))
                    if down_left > 0 and run_left > 0
                    else down_left > 0
                )

                if is_down:
                    dur = min(rem, down_left, random.uniform(down_min_s, down_max_s))
                    mc_id = random.choice(non_running_ids)
                else:
                    dur = min(rem, run_left, random.uniform(run_min_s, run_max_s))
                    mc_id = running_mc_id

                s_dt = t
                e_dt = _clamp_dt(
                    s_dt + timedelta(seconds=float(dur)),
                    sw_start,
                    sw_end,
                )

                if e_dt <= s_dt:
                    t = sw_end
                    continue

                seg = (e_dt - s_dt).total_seconds()
                if is_down:
                    down_left -= seg
                    total_down += seg
                else:
                    run_left -= seg
                    total_run += seg

                payload = {
                    "date_from": s_dt.isoformat(),
                    "date_to": e_dt.isoformat(),
                    "station_id": int(station_id),
                    "condition_id": mc_id,
                    "level": "A" if is_down else "P",
                    "condition_stamp": e_dt.isoformat(),
                    "condition_type": "s",
                    "color_rgb": "#8b1818" if is_down else "#13be1e",
                    "updated_at": e_dt.isoformat(),
                    "condition_created": s_dt.isoformat()
                }

                r = requests.post(
                    f"{BASE_URL}/machine-condition-data/machine-condition-data/",
                    json=payload,
                    headers=headers,
                    timeout=30,
                    verify=False,
                )

                print("MACHINE CONDITION DATA STATUS:", r.status_code)
                print("MACHINE CONDITION DATA RESPONSE:", r.text)

                if r.status_code in (200, 201):
                    data = r.json()
                    cid = data.get("id")
                    created.append({
                        "id": cid,
                        "station_id": station_id,
                        "url": data.get("url") or f"{BASE_URL}/machine-condition-data/machine-condition-data/{cid}",
                    })
                elif r.status_code in (400, 409, 422):
                    print(f"Skipping machine_condition_data for station {station_id}: {_get_json_or_text(r)}")
                else:
                    r.raise_for_status()

                produced += 1
                station_produced += 1
                t = e_dt

    total = total_run + total_down
    print(f"  + machine_condition_data (API): {len(created)}")

    if total > 0:
        print(f"    downtime: {total_down / total * 100:.1f}%  (target {downtime_target * 100:.0f}%)")

    return created



# ---------------------------------------------------------------------------
# GENERATION OF DATA ON NEW BD 
# ---------------------------------------------------------------------------


def api_get(endpoint: str, token=None, params=None):
    r = requests.get(
        f"{BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}",
        headers=_auth_headers(token),
        params=params or {},
        timeout=30,
        verify=False
    )
    r.raise_for_status()
    return r.json()


def api_post_newBD(endpoint: str, payload: dict, token=None):
    r = requests.post(
        f"{POST_BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}",
        json=payload,
        headers=_auth_headers(token),
        timeout=30
    )

    print("POST:", r.url)
    print("STATUS:", r.status_code)
    print("RESPONSE:", r.text)

    r.raise_for_status()
    return r.json()

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    #API_TOKEN = get_api_token()
    
    p = argparse.ArgumentParser(
        description="Generate ALL PCB industrial tables – 1-month window")

    p.add_argument("--months",            type=int,   default=1)
    p.add_argument("--work-orders",       type=int,   default=30)
    p.add_argument("--active-workorders", type=int,   default=10)
    p.add_argument("--wo-qty-min",        type=int,   default=200)
    p.add_argument("--wo-qty-max",        type=int,   default=800)
    p.add_argument("--bookings",          type=int,   default=20000)
    p.add_argument("--bookings-match-qty",action="store_true")
    p.add_argument("--booking-pass-p",    type=float, default=0.90)
    p.add_argument("--booking-fail-p",    type=float, default=0.07)
    p.add_argument("--booking-scrap-p",   type=float, default=0.03)
    p.add_argument("--measurements",      type=int,   default=20000)
    p.add_argument("--conditions",             type=int,   default=1000)
    p.add_argument("--downtime-target",        type=float, default=0.20)
    p.add_argument("--cond-running-hours-min", type=float, default=4.0)
    p.add_argument("--cond-running-hours-max", type=float, default=8.0)
    p.add_argument("--cond-downtime-hours-min",type=float, default=0.25)
    p.add_argument("--cond-downtime-hours-max",type=float, default=4.0)
    p.add_argument("--n-products",  type=int, default=8,
                   help="Number of distinct PCB product types (max 8)")
    p.add_argument("--n-sites",     type=int, default=2)
    p.add_argument("--n-clients",   type=int, default=3)
    p.add_argument("--seed",        type=int, default=0)
    args = p.parse_args()

    if int(args.seed):
        random.seed(int(args.seed))

    pass_p, fail_p, scrap_p = (args.booking_pass_p,
                                args.booking_fail_p,
                                args.booking_scrap_p)
    if abs(pass_p + fail_p + scrap_p - 1.0) > 1e-6:
        raise SystemExit("Booking probabilities must sum to 1.0")

    now          = datetime.now().replace(second=0, microsecond=0)
    window_end   = now
    window_start = _add_months(now, -int(args.months))

    print(f"\n{'='*60}")
    print("PCB Industrial Data Generator  –  Full Schema")
    print(f"Window : {window_start}  ->  {window_end}")
    print(f"{'='*60}")

    pg   = _pg_config_from_env()
    writer = PostgresWriter(pg)
    conn   = writer.connect()

# ── Layer 0 ──────────────────────────────────────────────────────────────

    print("\n[Layer 0] Reference / catalogue tables")
    
    cc_ids = gen_company_codes_api(n=args.n_clients, token=API_TOKEN)
    print("Company créés :", cc_ids)
    
    cl_ids = gen_clients_api(company_code_ids=cc_ids, n=args.n_clients, token=API_TOKEN )
    print("Clients Company créés :", cl_ids)
    
    site_ids = gen_sites_api(company_code_ids=cc_ids, n=5, token=API_TOKEN )
    print("Sites créés :", site_ids)
    
    part_types_ids = gen_part_types_api( n=6, token=API_TOKEN)
    print("Part Types créés :", part_types_ids)

    part_group_types_ids = gen_part_group_types_api( n=4, token=API_TOKEN)
    print("Part Group Types créés :", part_group_types_ids)

    workplan_types_ids = gen_workplan_types_api( n=5, token=API_TOKEN)
    print("Workplan Types créés :", workplan_types_ids)
    
    failure_group_types_ids = gen_failure_group_types_api( n=6, token=API_TOKEN)
    print("Failure Group Types créés :", failure_group_types_ids)
    
    machine_condition_groups_ids = gen_machine_condition_groups_api(n=3,token=API_TOKEN)
    print("Machine condition groups créés :", machine_condition_groups_ids)
    
    cell_ids = gen_cells_api(site_ids=site_ids, n=5, token=API_TOKEN)
    if not cell_ids:
        raise RuntimeError("Cells vides → STOP")

    machine_group_ids = gen_machine_groups_api(cell_ids=cell_ids, n=10, token=API_TOKEN)
    print("machine groups créés : ", machine_group_ids )
    
    machine_group_id_map  = seed_machine_groups_api(cell_ids=cell_ids, token=API_TOKEN)
    print("Machine Groups map :", machine_group_id_map)
    
    station_legacy_to_api_id = seed_fixed_stations_api(token=API_TOKEN,machine_group_name_to_id=machine_group_name_to_id)
    print("station map", station_legacy_to_api_id )
     
    line_legacy_to_api_id = seed_fixed_lines_api(token=API_TOKEN, station_legacy_to_api_id=station_legacy_to_api_id)
    print("Lines créés/réutilisés", line_legacy_to_api_id) 
        
    valid_associations = build_line_station_associations(
    line_legacy_to_api_id=line_legacy_to_api_id,
    station_legacy_to_api_id=station_legacy_to_api_id
    )
    print("Associations valides :", valid_associations[:10])
    
    erp_group_code_to_api_id = gen_erp_groups_api( token=API_TOKEN, window_start=window_start)
    print("ERP group map", erp_group_code_to_api_id)
        
    failure_type_map = gen_failure_types_api(token=API_TOKEN, failure_group_ids=failure_group_types_ids, site_ids=site_ids)
    print("Failure types map :", failure_type_map)
    
    if not machine_condition_groups_ids:
        raise RuntimeError("Aucun machine condition group ID récupéré.")
    
    machine_condition_map = gen_machine_conditions_ref_api(token=API_TOKEN,machine_condition_group_ids=machine_condition_groups_ids)
    print("Machine conditions map :", machine_condition_map)
    
    part_group_map = gen_part_groups_api(token=API_TOKEN, part_group_type_ids=part_group_types_ids )
    print("Part groups map :", part_group_map)
    
    part_master_map, part_numbers = gen_part_master_api(
    token=API_TOKEN,
    part_type_ids=part_types_ids,
    part_group_map=part_group_map,
    machine_group_ids=list(machine_group_name_to_id.values()),
    site_ids=site_ids,
    unit_id=1,   # remplace par un vrai unit_id valide de ton système
    n_products=8,
    )
    
    product_part_numbers = part_numbers[:args.n_products]
    product_part_master_ids = [part_master_map[pn] for pn in product_part_numbers]
    product_part_ids = product_part_master_ids
    
    erp_group_ids = list(erp_group_code_to_api_id.values())
    station_ids = list(station_legacy_to_api_id.values())
    
    station_erp_assignments = gen_assign_stations_to_erpgrp_api(
        token=API_TOKEN, station_ids=station_ids, erp_group_ids=erp_group_ids )
    
    print("Assignations station/ERP :", station_erp_assignments[:10])
    
    workplan_rows = gen_work_plans_api(
    site_ids,
    cl_ids,
    cc_ids,
    product_part_numbers,
    window_start,
    window_end,
    token=API_TOKEN)
    
    print ("workplan", workplan_rows)
    
    workplan_by_part_no = {
    row["part_no"]: {"id": row["id"], "url": row["url"]}
    for row in workplan_rows}
    
    print("workplan by part", workplan_by_part_no)
    
    workstep_rows = gen_work_steps_api(
    workplan_ids = [wp["id"] for wp in workplan_rows],
    erp_group_ids=erp_group_ids,
    window_start=window_start,
    token=API_TOKEN)
    
    print("workstep", workstep_rows)

    
    # 3. BOM headers
    
    bom_header_rows = gen_bom_headers_api(
    product_part_master_ids,
    window_start,
    API_TOKEN)
    
    print("bom headers", bom_header_rows)
    
    component_ids = [
    pm_id for pn, pm_id in part_master_map.items()
    if pn not in product_part_numbers]
    
    # 4. BOM items
    
    gen_bom_items_api(
    bom_header_rows,
    component_ids,
    API_TOKEN)
    
    #bom_rows = gen_boms_api(
    #product_part_master_ids,
    #window_start,
    #API_TOKEN)
    
    #print("boms", bom_rows)
    
    gen_bom_insertion_api(
    product_part_master_ids,
    API_TOKEN)
    
    print("bom insertion")
    
    work_order_ids, wo_windows = gen_work_orders_api(
    client_ids=cl_ids,
    company_code_ids=cc_ids,
    site_ids=site_ids,
    product_part_master_ids=product_part_numbers,
    product_part_ids=product_part_ids,
    workplan_by_part_no=workplan_by_part_no,
    window_start=window_start,
    window_end=window_end,
    n=50,
    wo_qty_min=10,
    wo_qty_max=100,
    token=API_TOKEN)
    
    print("workorders", work_order_ids)
    
    if not wo_windows:
        print("No workorders created → skipping serial_numbers, active_workorders and bookings")
        return

    wo_snr_map = gen_serial_numbers_api(
    wo_windows=wo_windows,
    token=API_TOKEN)
    
    print("serial numbers", wo_snr_map)
    
    active_workorder_rows = gen_active_workorders_api(
    wo_windows=wo_windows,
    station_ids=station_ids,
    window_end=window_end,
    n=20,
    token=API_TOKEN)
    
    print("active workorders", active_workorder_rows)
    
    
    booking_rows = gen_bookings_api(
    wo_windows=wo_windows,
    wo_snr_map=wo_snr_map,
    station_ids=station_ids,
    failure_type_ids=list(failure_type_map.values()) if isinstance(failure_type_map, dict) else failure_type_map,
    pass_p=0.90,
    fail_p=0.08,
    scrap_p=0.02,
    target_bookings=100,
    window_start=window_start,
    window_end=window_end,
    token=API_TOKEN)
    
    print("bookings", booking_rows)
    
    measurement_rows = gen_measurement_data_api(
    wo_windows=wo_windows,
    station_ids=station_ids,
    n=200,
    token=API_TOKEN)
    
    print("measurement data", measurement_rows)
    
    machine_condition_rows = gen_machine_condition_data_api(
    station_ids=station_ids,
    machine_condition_ids=list(machine_condition_map.values()),
    window_start=window_start,
    window_end=window_end,
    n_max=200,
    downtime_target=0.20,
    run_min_h=1,
    run_max_h=4,
    down_min_h=0.2,
    down_max_h=1,
    token=API_TOKEN)
    
    print("machine condition data", machine_condition_rows)

    conn.commit()
    
if __name__ == "__main__":
    main()