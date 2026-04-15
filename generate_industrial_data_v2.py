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
from admin.postgres_writer_v2 import PgConfig, PostgresWriter
from dotenv import load_dotenv
import re

BASE_URL ="https://core_demo.momes-solutions.com"
API_TOKEN = os.getenv("API_TOKEN")

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

STATIONS_FIXED: list[dict[str, Any]] = [
    {"id": 4,   "machine_group_id": 4,  "name": "FRA-PP-L01-01",  "description": "Pick & Place 1"},
    {"id": 5,   "machine_group_id": 4,  "name": "FRA-PP-L01-02",  "description": "Pick & Place 2"},
    {"id": 6,   "machine_group_id": 4,  "name": "FRA-PP-L01-03",  "description": "Pick & Place 3"},
    {"id": 7,   "machine_group_id": 5,  "name": "FRA-OVE-L01-01", "description": "Reflow Oven"},
    {"id": 8,   "machine_group_id": 6,  "name": "FRA-AOI-L01-01", "description": "AOI"},
    {"id": 12,  "machine_group_id": 5,  "name": "FRA-OVE-L01-02", "description": ""},
    {"id": 1,   "machine_group_id": 1,  "name": "FRA-LAB-L01-01", "description": "Labeling"},
    {"id": 2,   "machine_group_id": 1,  "name": "ST-02",           "description": "Auto seeded for FK integrity"},
    {"id": 3,   "machine_group_id": 1,  "name": "ST-03",           "description": "Auto seeded for FK integrity"},
    {"id": 9,   "machine_group_id": 1,  "name": "ST-09",           "description": "Auto seeded for FK integrity"},
    {"id": 10,  "machine_group_id": 1,  "name": "ST-10",           "description": "Auto seeded for FK integrity"},
    {"id": 27,  "machine_group_id": 6,  "name": "FRA-AOI-L02-01", "description": None},
    {"id": 28,  "machine_group_id": 6,  "name": "FRA-AOI-L03-01", "description": None},
    {"id": 29,  "machine_group_id": 6,  "name": "FRA-AOI-L04-01", "description": None},
    {"id": 30,  "machine_group_id": 6,  "name": "FRA-AOI-L05-01", "description": None},
    {"id": 31,  "machine_group_id": 6,  "name": "FRA-AOI-L06-01", "description": None},
    {"id": 63,  "machine_group_id": 5,  "name": "FRA-OVE-L02-01", "description": None},
    {"id": 64,  "machine_group_id": 5,  "name": "FRA-OVE-L03-01", "description": None},
    {"id": 65,  "machine_group_id": 5,  "name": "FRA-OVE-L04-01", "description": None},
    {"id": 66,  "machine_group_id": 5,  "name": "FRA-OVE-L05-01", "description": None},
    {"id": 67,  "machine_group_id": 5,  "name": "FRA-OVE-L06-01", "description": None},
    {"id": 75,  "machine_group_id": 4,  "name": "FRA-PP-L02-01",  "description": None},
    {"id": 76,  "machine_group_id": 4,  "name": "FRA-PP-L02-02",  "description": None},
    {"id": 77,  "machine_group_id": 4,  "name": "FRA-PP-L03-01",  "description": None},
    {"id": 78,  "machine_group_id": 4,  "name": "FRA-PP-L03-02",  "description": None},
    {"id": 79,  "machine_group_id": 4,  "name": "FRA-PP-L04-01",  "description": None},
    {"id": 80,  "machine_group_id": 4,  "name": "FRA-PP-L04-02",  "description": None},
    {"id": 81,  "machine_group_id": 4,  "name": "FRA-PP-L05-01",  "description": None},
    {"id": 82,  "machine_group_id": 4,  "name": "FRA-PP-L05-02",  "description": None},
    {"id": 83,  "machine_group_id": 4,  "name": "FRA-PP-L06-01",  "description": None},
    {"id": 84,  "machine_group_id": 4,  "name": "FRA-PP-L06-02",  "description": None},
    {"id": 24,  "machine_group_id": 10, "name": "THT-AIM-L04-01", "description": None},
    {"id": 25,  "machine_group_id": 10, "name": "THT-AIM-L05-01", "description": None},
    {"id": 35,  "machine_group_id": 6,  "name": "THT-AOI-L04-01", "description": None},
    {"id": 36,  "machine_group_id": 6,  "name": "THT-AOI-L05-01", "description": None},
    {"id": 48,  "machine_group_id": 21, "name": "THT-CC-L04-01",  "description": None},
    {"id": 49,  "machine_group_id": 21, "name": "THT-CC-L05-01",  "description": None},
    {"id": 71,  "machine_group_id": 11, "name": "THT-PO-L04-01",  "description": None},
    {"id": 72,  "machine_group_id": 11, "name": "THT-PO-L05-01",  "description": None},
    {"id": 100, "machine_group_id": 12, "name": "THT-WSM-L04-01", "description": None},
    {"id": 101, "machine_group_id": 12, "name": "THT-WSM-L05-01", "description": None},
    {"id": 21,  "machine_group_id": 10, "name": "THT-AIM-L01-01", "description": None},
    {"id": 22,  "machine_group_id": 10, "name": "THT-AIM-L02-01", "description": None},
    {"id": 23,  "machine_group_id": 10, "name": "THT-AIM-L03-01", "description": None},
    {"id": 32,  "machine_group_id": 6,  "name": "THT-AOI-L01-01", "description": None},
    {"id": 33,  "machine_group_id": 6,  "name": "THT-AOI-L02-01", "description": None},
    {"id": 34,  "machine_group_id": 6,  "name": "THT-AOI-L03-01", "description": None},
    {"id": 45,  "machine_group_id": 21, "name": "THT-CC-L01-01",  "description": None},
    {"id": 46,  "machine_group_id": 21, "name": "THT-CC-L02-01",  "description": None},
    {"id": 47,  "machine_group_id": 21, "name": "THT-CC-L03-01",  "description": None},
    {"id": 68,  "machine_group_id": 11, "name": "THT-PO-L01-01",  "description": None},
    {"id": 69,  "machine_group_id": 11, "name": "THT-PO-L02-01",  "description": None},
    {"id": 70,  "machine_group_id": 11, "name": "THT-PO-L03-01",  "description": None},
    {"id": 37,  "machine_group_id": 6,  "name": "TL-AOI-L01-01",  "description": None},
    {"id": 38,  "machine_group_id": 6,  "name": "TL-AOI-L02-01",  "description": None},
    {"id": 39,  "machine_group_id": 6,  "name": "TL-AOI-L03-01",  "description": None},
    {"id": 40,  "machine_group_id": 6,  "name": "TL-AOI-L04-01",  "description": None},
    {"id": 41,  "machine_group_id": 17, "name": "TL-BCT-L01-01",  "description": None},
    {"id": 42,  "machine_group_id": 17, "name": "TL-BCT-L02-01",  "description": None},
    {"id": 43,  "machine_group_id": 17, "name": "TL-BCT-L03-01",  "description": None},
    {"id": 44,  "machine_group_id": 17, "name": "TL-BCT-L04-01",  "description": None},
    {"id": 50,  "machine_group_id": 14, "name": "TL-FPT-L01-01",  "description": None},
    {"id": 51,  "machine_group_id": 14, "name": "TL-FPT-L02-01",  "description": None},
    {"id": 52,  "machine_group_id": 14, "name": "TL-FPT-L03-01",  "description": None},
    {"id": 53,  "machine_group_id": 14, "name": "TL-FPT-L04-01",  "description": None},
    {"id": 54,  "machine_group_id": 15, "name": "TL-FTS-L01-01",  "description": None},
    {"id": 55,  "machine_group_id": 15, "name": "TL-FTS-L02-01",  "description": None},
    {"id": 56,  "machine_group_id": 15, "name": "TL-FTS-L03-01",  "description": None},
    {"id": 57,  "machine_group_id": 15, "name": "TL-FTS-L04-01",  "description": None},
    {"id": 58,  "machine_group_id": 13, "name": "TL-ICT-L01-01",  "description": None},
    {"id": 59,  "machine_group_id": 13, "name": "TL-ICT-L02-01",  "description": None},
    {"id": 60,  "machine_group_id": 13, "name": "TL-ICT-L03-01",  "description": None},
    {"id": 61,  "machine_group_id": 13, "name": "TL-ICT-L04-01",  "description": None},
    {"id": 85,  "machine_group_id": 3,  "name": "FRA-SPI-L01-01", "description": None},
    {"id": 86,  "machine_group_id": 3,  "name": "FRA-SPI-L02-01", "description": None},
    {"id": 87,  "machine_group_id": 3,  "name": "FRA-SPI-L03-01", "description": None},
    {"id": 88,  "machine_group_id": 3,  "name": "FRA-SPI-L04-01", "description": None},
    {"id": 89,  "machine_group_id": 3,  "name": "FRA-SPI-L05-01", "description": None},
    {"id": 90,  "machine_group_id": 3,  "name": "FRA-SPI-L06-01", "description": None},
    {"id": 91,  "machine_group_id": 2,  "name": "FRA-SPP-L01-01", "description": None},
    {"id": 92,  "machine_group_id": 2,  "name": "FRA-SPP-L02-01", "description": None},
    {"id": 93,  "machine_group_id": 2,  "name": "FRA-SPP-L03-01", "description": None},
    {"id": 94,  "machine_group_id": 2,  "name": "FRA-SPP-L04-01", "description": None},
    {"id": 95,  "machine_group_id": 2,  "name": "FRA-SPP-L05-01", "description": None},
    {"id": 96,  "machine_group_id": 2,  "name": "FRA-SPP-L06-01", "description": None},
    {"id": 97,  "machine_group_id": 12, "name": "THT-WSM-L01-01", "description": None},
    {"id": 98,  "machine_group_id": 12, "name": "THT-WSM-L02-01", "description": None},
    {"id": 99,  "machine_group_id": 12, "name": "THT-WSM-L03-01", "description": None},
    {"id": 102, "machine_group_id": 16, "name": "ASS-ASM-L01-01", "description": None},
]

# Exact machine groups from production DB export – covers every machine_group_id
# referenced by STATIONS_FIXED so the FK constraint is always satisfied.
MACHINE_GROUPS_FIXED: list[dict[str, Any]] = [
    {"id": 1,  "name": "LAB-GROUP",   "description": "Labeling Machines"},
    {"id": 2,  "name": "SPP-GROUP",   "description": "Solder Paste Printer"},
    {"id": 3,  "name": "SPI-GROUP",   "description": "Solder Paste Inspection"},
    {"id": 4,  "name": "PNP-GROUP",   "description": "Pick & Place Machines"},
    {"id": 5,  "name": "OVE-GROUP",   "description": "Reflow Ovens"},
    {"id": 6,  "name": "AOI-GROUP",   "description": "Automated Optical Inspection"},
    {"id": 10, "name": "AIM-GROUP",   "description": "Auto Insertion Machines"},
    {"id": 11, "name": "PO-GROUP",    "description": "Pin-through-hole Ovens"},
    {"id": 12, "name": "WSM-GROUP",   "description": "Wave Solder Machines"},
    {"id": 13, "name": "ICT-GROUP",   "description": "In-Circuit Testers"},
    {"id": 14, "name": "FPT-GROUP",   "description": "Flying Probe Testers"},
    {"id": 15, "name": "FTS-GROUP",   "description": "Functional Test Systems"},
    {"id": 16, "name": "ASM-GROUP",   "description": "Assembly Machines"},
    {"id": 17, "name": "BCT-GROUP",   "description": "Box Build & Cable Testers"},
    {"id": 21, "name": "CC-GROUP",    "description": "Conformal Coating Machines"},
]

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
        host=os.getenv("PGHOST", "host.docker.internal"),
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
    
    created_ids: list[int] = []
    
    headers = _auth_headers(token)

    for i in range(n):
        name = f"CC-{i+1:03d}"

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

        if r.status_code == 409:
            # le company code existe déjà → on récupère son id depuis le message
            try:
                data = r.json()
                detail = data.get("detail", "")
                # exemple: "Company code with name 'CC-001' already exists (ID: 3)"
                import re
                match = re.search(r"ID:\s*(\d+)", detail)
                if match:
                    existing_id = int(match.group(1))
                    created_ids.append(existing_id)
                    print(f"Reusing existing company code '{name}' with ID {existing_id}")
                    continue
            except Exception:
                pass

            raise RuntimeError(f"Company code '{name}' already exists, but ID could not be parsed.")

        r.raise_for_status()

    print(f"  + company_codes (API): {len(created_ids)} rows")
    return created_ids


def gen_clients_api(company_code_ids: list[int], n: int = 3, token=None) -> list[int]:
      
    if not token:
        raise ValueError("Token API manquant.")
    
    names = ["PCB-Corp", "ElektraTech", "CircuitMasters", "NexaPCB", "SolderPro"]
    created_ids: list[int] = []
    
    headers = _auth_headers(token)

    for i in range(n):
        client_name = f"{names[i % len(names)][:14]} {i+1}"

        payload = {
            "user_id": 1,
            "company_code": f"CC{i+1:03d}",
            "name": client_name,
            "description": f"PCB manufacturing client {i+1}"
        }

        r = requests.post(
            f"{BASE_URL}/clients/clients",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False
        )

        print("CLIENT STATUS:", r.status_code)
        print("CLIENT RESPONSE:", r.text)

        if r.status_code in (200, 201):
            data = r.json()
            client_id = data.get("id")
            if not client_id:
                raise RuntimeError(f"Client API response has no id: {data}")
            created_ids.append(int(client_id))
            continue

        if r.status_code == 409:
            try:
                data = r.json()
                detail = data.get("detail", "")
                match = re.search(r"ID[:\s]*([0-9]+)", detail)
                #match = re.search(r"\(ID:\s*(\d+)\)", detail)
                if match:
                    existing_id = int(match.group(1))
                    created_ids.append(existing_id)
                    print(f"Reusing existing client '{client_name}' with ID {existing_id}")
                    continue
            except Exception:
                pass

            raise RuntimeError(f"Client '{client_name}' already exists, but ID could not be parsed.")

        r.raise_for_status()

    # Mise à jour des company codes avec le client_id
    for i, cc_id in enumerate(company_code_ids):
        client_id = created_ids[i % len(created_ids)]

        update_payload = {
            "user_id": 1,
            "client_id": client_id,
            "name": f"CC-{i+1:03d}",
            "description": f"Company Code {i+1}"
        }

        r = requests.put(
            f"{BASE_URL}/company-codes/{cc_id}",
            json=update_payload,
            headers=headers,
            timeout=30,
            verify=False
        )

        print("COMPANY_CODE UPDATE STATUS:", r.status_code)
        print("COMPANY_CODE UPDATE RESPONSE:", r.text)

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

def gen_machine_condition_groups_api(n: int = 3, token=None):

    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)

    headers = _auth_headers(token)

    items = [
        ("Unplanned Downtime", "Breakdowns, stoppages, deviations"),
        ("Planned Downtime", "Changeover, setup, maintenance"),
        ("Operational", "Running, breaks, meetings"),
    ]

    created_ids = []

    for i in range(n):
        name, desc = items[i % len(items)]

        payload = {
            "group_name": f"{name[:14]} {i+1}",
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
            created_ids.append(r.json()["id"])
            continue

        if r.status_code in (400, 409):
            print(f"⚠️ exists → {name}")
            continue

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


def seed_fixed_machine_groups_api(cell_ids: list[int], token: str) -> dict[int, int]:
    """
    Returns:
        {export_machine_group_id: api_machine_group_id}
    """
    if not cell_ids:
        raise ValueError("cell_ids est vide — impossible de créer les machine groups")

    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)

    headers = _auth_headers(token)
    MACHINE_GROUPS_URL = f"{BASE_URL}/machine-groups/machine_groups/"
    id_map = {}

    for mg in MACHINE_GROUPS_FIXED:
        payload = {
            "name": mg["name"],
            "description": mg["description"],
            "user_id": 1,
            "cell_id": cell_ids[mg["id"] % len(cell_ids)],
            "is_active": True,
            "failure": False,
        }

        r = requests.post(
            MACHINE_GROUPS_URL,
            json=payload,
            headers=headers,
            timeout=30,
            verify=False
        )

        print("STATUS:", r.status_code)
        print("RESPONSE:", r.text)

        if r.status_code in (200, 201):
            id_map[mg["id"]] = r.json()["id"]
            continue

        if r.status_code == 409:
            g = requests.get(
                MACHINE_GROUPS_URL,
                headers=headers,
                timeout=30,
                verify=False
            )

            if g.status_code == 200:
                body = g.json()
                items = body["results"] if isinstance(body, dict) and "results" in body else body

                for item in items:
                    if item.get("name") == payload["name"]:
                        id_map[mg["id"]] = item["id"]
                        break
                else:
                    print(f"⚠️ exists but not found: {payload['name']}")
            continue

        r.raise_for_status()

    print("Machine group id map:", id_map)
    return id_map

def seed_fixed_stations_api(token: str, machine_group_id_map: dict[int, int]) -> dict[int, int]:
    """
    Returns:
        {export_station_id: api_station_id}
    """
    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)

    headers = _auth_headers(token)
    STATIONS_URL = f"{BASE_URL}/stations/stations/"
    id_map = {}

    for s in STATIONS_FIXED:
        export_mg_id = s["machine_group_id"]

        if export_mg_id not in machine_group_id_map:
            raise ValueError(
                f"machine_group_id export introuvable: {export_mg_id} pour station {s['name']}"
            )

        payload = {
            "name": s["name"],
            "description": s.get("description") or "",
            "machine_group_id": machine_group_id_map[export_mg_id],
            "is_active": True,
            "user_id": 8,
            "info": "",
        }

        r = requests.post(
            STATIONS_URL,
            json=payload,
            headers=headers,
            timeout=30,
            verify=False
        )

        print("STATUS:", r.status_code)
        print("RESPONSE:", r.text)

        if r.status_code in (200, 201):
            id_map[s["id"]] = r.json()["id"]
            continue

        if r.status_code in (400, 409):
            g = requests.get(
                STATIONS_URL,
                headers=headers,
                timeout=30,
                verify=False
            )

            if g.status_code == 200:
                body = g.json()
                items = body["results"] if isinstance(body, dict) and "results" in body else body

                for item in items:
                    if item.get("name") == payload["name"]:
                        id_map[s["id"]] = item["id"]
                        break
                else:
                    print(f"⚠️ station exists but not found: {payload['name']}")
            continue

        r.raise_for_status()

    print("station_id_map:", id_map)
    return id_map

def seed_fixed_lines_api(token: str, station_id_map: dict[int, int]) -> list[int]:
    """
    Create fixed lines through the API.
    station_id_map maps export station IDs -> API station IDs.
    Returns created/reused line IDs.
    """

    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)

    headers = _auth_headers(token)
    created_ids = []
    LINES_URL = f"{BASE_URL}/lines/lines/"

    for ln in LINES_FIXED:
        export_station_ids = [
            sid for lid, sid in LINE_STATION_ASSOCIATIONS_FIXED
            if lid == ln["id"]
        ]

        api_station_ids = []
        for export_sid in export_station_ids:
            api_sid = station_id_map.get(export_sid)
            if api_sid is not None:
                api_station_ids.append(api_sid)
            else:
                print(f"⚠️ station export id introuvable dans station_id_map: {export_sid}")

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
            created_ids.append(r.json()["id"])
            print(f"✅ created line {ln['name']} -> {r.json()['id']}")
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

                for item in items:
                    if item.get("name") == ln["name"]:
                        created_ids.append(item["id"])
                        print(f"♻️ reused line {ln['name']} -> {item['id']}")
                        break
                else:
                    print(f"⚠️ line existe peut-être mais introuvable: {ln['name']}")
            else:
                print(f"❌ GET lines failed: {g.status_code}")

            continue

        r.raise_for_status()

    print("Lines créées/réutilisées :", created_ids)
    return created_ids


def seed_fixed_line_station_association_api(
    token: str,
    line_id_map: dict[int, int],
    station_id_map: dict[int, int],
) -> None:
    if isinstance(token, str) and token.startswith("Bearer "):
        token = token.replace("Bearer ", "", 1)

    headers = _auth_headers(token)
    
    ASSOC_URL = f"{BASE_URL}/lines/lines/"
    now_str = _dt_str(datetime.now())

    for export_line_id, export_station_id in LINE_STATION_ASSOCIATIONS_FIXED:
        api_line_id = line_id_map.get(export_line_id)
        api_station_id = station_id_map.get(export_station_id)

        if api_line_id is None:
            print(f"⚠️ line export id introuvable: {export_line_id}")
            continue

        if api_station_id is None:
            print(f"⚠️ station export id introuvable: {export_station_id}")
            continue

        payload = {
            "line_id": api_line_id,
            "station_id": api_station_id,
            "created_at": now_str,
        }

        r = requests.post(
            ASSOC_URL,
            json=payload,
            headers=headers,
            timeout=30,
            verify=False
        )

        print("POST URL:", ASSOC_URL)
        print("PAYLOAD:", payload)
        print("STATUS:", r.status_code)
        print("RESPONSE:", r.text)

        if r.status_code in (200, 201):
            print(f"✅ association créée {api_line_id} -> {api_station_id}")
            continue

        if r.status_code in (400, 409):
            print(f"♻️ association déjà existante ou ignorée {api_line_id} -> {api_station_id}")
            continue

        r.raise_for_status()  

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
    
    machine_condition_groups_ids = gen_machine_condition_groups_api( n=3, token=API_TOKEN)
    print("Machine condition groups créés :", machine_condition_groups_ids)

    cell_ids = gen_cells_api(site_ids=site_ids, n=5, token=API_TOKEN)
    if not cell_ids:
        raise RuntimeError("Cells vides → STOP")

    machine_group_ids = gen_machine_groups_api(cell_ids=cell_ids, n=10, token=API_TOKEN)
    print("machine groups créés : ", machine_group_ids )
    
    machine_group_id_map  = seed_fixed_machine_groups_api(cell_ids=cell_ids, token=API_TOKEN)
    print("Machine Groups map :", machine_group_id_map)
    
    
    station_id_map = seed_fixed_stations_api(token=API_TOKEN, machine_group_id_map=machine_group_id_map) 
    print("Stations créés/réutilisés : ", station_id_map)
 
    lines_ids = seed_fixed_lines_api(token=API_TOKEN, station_id_map=station_id_map)
    print("Lines créés/réutilisés", lines_ids) 
    
    
    conn.commit()

if __name__ == "__main__":
    main()