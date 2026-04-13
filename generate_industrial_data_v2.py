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

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = os.getenv("BASE_URL", "https://core_demo.momes-solutions.com")
API_TOKEN = os.getenv("API_TOKEN")


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
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

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
    created_ids: list[int] = []

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

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

import re
import requests

def gen_clients_api(company_code_ids: list[int], n: int = 3, token=None) -> list[int]:
    names = ["PCB-Corp", "ElektraTech", "CircuitMasters", "NexaPCB", "SolderPro"]
    created_ids: list[int] = []

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

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
                match = re.search(r"ID:\s*(\d+)", detail)
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


"""
def gen_sites(conn, company_code_ids: list[int], n: int = 2) -> list[int]:
    nxt = _get_next_id(conn, "public.sites")
    locs = [("SITE-TN-01", "36.8065,10.1815", "Tunis Plant"),
            ("SITE-TN-02", "36.7370,10.2320", "Ariana Plant"),
            ("SITE-EU-01", "48.8566,2.3522",  "Paris Plant")]
    rows = []
    for i in range(n):
        loc = locs[i % len(locs)]
        rows.append({"id": nxt + i, "user_id": 1,
                      "company_code_id": company_code_ids[i % len(company_code_ids)],
                      "site_number": f"{loc[0]}-{nxt+i}", "site_external_number": f"EXT-{nxt+i:04d}",
                      "deletion_priority": 0, "geo_coordinates": loc[1],
                      "description": loc[2]})
    sql = ("INSERT INTO public.sites "
           "(id,user_id,company_code_id,site_number,site_external_number,"
           "deletion_priority,geo_coordinates,description) "
           "VALUES (%(id)s,%(user_id)s,%(company_code_id)s,%(site_number)s,"
           "%(site_external_number)s,%(deletion_priority)s,%(geo_coordinates)s,%(description)s) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="sites")
    return [r["id"] for r in rows]


def gen_part_types(conn) -> list[int]:
    nxt = _get_next_id(conn, "public.part_types")
    now = datetime.now()
    items = [("PCB Assembly", "Finished PCB assembly"),
             ("Raw PCB", "Bare printed circuit board"),
             ("SMD Component", "Surface-mount device"),
             ("THT Component", "Through-hole component"),
             ("Mechanical", "Mechanical / hardware part"),
             ("Consumable", "Solder, flux, cleaning agent")]
    rows = [{"id": nxt + i, "name": f"{nm[:14]} {nxt+i}", "description": ds, "user_id": 1,
              "is_active": True, "date_of_creation": _dt_str(now),
              "date_of_change": _dt_str(now)}
             for i, (nm, ds) in enumerate(items)]
    sql = ("INSERT INTO public.part_types "
           "(id,name,description,user_id,is_active,date_of_creation,date_of_change) "
           "VALUES (%(id)s,%(name)s,%(description)s,%(user_id)s,%(is_active)s,"
           "CAST(%(date_of_creation)s AS timestamp),CAST(%(date_of_change)s AS timestamp)) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="part_types")
    return [r["id"] for r in rows]


def gen_part_group_types(conn) -> list[int]:
    nxt = _get_next_id(conn, "public.part_group_types")
    now = datetime.now()
    items = [("Finished Goods", "Shipped customer products"),
             ("WIP", "Work-in-progress assemblies"),
             ("Components", "Purchased electronic components"),
             ("Raw Material", "PCB substrates and bare boards")]
    rows = [{"id": nxt + i, "name": f"{nm[:14]} {nxt+i}", "description": ds,
              "created_at": _dt_str(now), "updated_at": _dt_str(now)}
             for i, (nm, ds) in enumerate(items)]
    sql = ("INSERT INTO public.part_group_types (id,name,description,created_at,updated_at) "
           "VALUES (%(id)s,%(name)s,%(description)s,"
           "CAST(%(created_at)s AS timestamp),CAST(%(updated_at)s AS timestamp)) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="part_group_types")
    return [r["id"] for r in rows]


def gen_workplan_types(conn) -> list[int]:
    nxt = _get_next_id(conn, "public.workplan_types")
    now = datetime.now()
    items = [("SMT", "Surface Mount Technology line"),
             ("THT", "Through-Hole Technology line"),
             ("MIXED", "Mixed SMT + THT line"),
             ("TEST", "Test & inspection only"),
             ("REWORK", "Rework / repair process")]
    rows = [{"id": nxt + i, "name": f"{nm[:14]} {nxt+i}", "description": ds, "is_active": True,
              "created_at": _dt_str(now), "updated_at": _dt_str(now)}
             for i, (nm, ds) in enumerate(items)]
    sql = ("INSERT INTO public.workplan_types (id,name,description,is_active,created_at,updated_at) "
           "VALUES (%(id)s,%(name)s,%(description)s,%(is_active)s,"
           "CAST(%(created_at)s AS timestamp),CAST(%(updated_at)s AS timestamp)) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="workplan_types")
    return [r["id"] for r in rows]


def gen_failure_group_types(conn) -> list[int]:
    nxt = _get_next_id(conn, "public.failure_group_types")
    now = datetime.now()
    items = [
        ("Solder Defects",      "Bridging, opens, insufficient solder"),
        ("Component Defects",   "Missing, wrong value, wrong orientation"),
        ("PCB Defects",         "Board damage, contamination, delamination"),
        ("Process Defects",     "Paste, placement, reflow process issues"),
        ("Electrical Failures", "Short, open circuit, ESD damage"),
        ("Cosmetic Defects",    "Scratches, marks, flux residue"),
    ]
    rows = [{"id": nxt + i, "failure_group_name": f"{nm[:14]} {nxt+i}", "failure_group_desc": ds,
              "created_at": _dt_str(now), "updated_at": _dt_str(now)}
             for i, (nm, ds) in enumerate(items)]
    sql = ("INSERT INTO public.failure_group_types "
           "(id,failure_group_name,failure_group_desc,created_at,updated_at) "
           "VALUES (%(id)s,%(failure_group_name)s,%(failure_group_desc)s,"
           "CAST(%(created_at)s AS timestamp),CAST(%(updated_at)s AS timestamp)) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="failure_group_types")
    return [r["id"] for r in rows]


def gen_machine_condition_groups(conn) -> list[int]:
    nxt = _get_next_id(conn, "public.machine_condition_groups")
    now = datetime.now()
    items = [("Unplanned Downtime", "Breakdowns, stoppages, deviations"),
             ("Planned Downtime",   "Changeover, setup, maintenance"),
             ("Operational",        "Running, breaks, meetings")]
    rows = [{"id": nxt + i, "group_name": f"{nm[:14]} {nxt+i}", "group_description": ds,
              "is_active": True, "created_at": _dt_str(now), "updated_at": _dt_str(now)}
             for i, (nm, ds) in enumerate(items)]
    sql = ("INSERT INTO public.machine_condition_groups "
           "(id,group_name,group_description,is_active,created_at,updated_at) "
           "VALUES (%(id)s,%(group_name)s,%(group_description)s,%(is_active)s,"
           "CAST(%(created_at)s AS timestamp),CAST(%(updated_at)s AS timestamp)) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="machine_condition_groups")
    return [r["id"] for r in rows]

# ---------------------------------------------------------------------------
# Layer 1
# ---------------------------------------------------------------------------

def gen_cells(conn, site_ids: list[int]) -> list[int]:
    nxt = _get_next_id(conn, "public.cells")
    cells_def = [
        ("SMT-LINE-A", "SMT Production Line A"),
        ("SMT-LINE-B", "SMT Production Line B"),
        ("THT-LINE",   "THT Production Line"),
        ("TEST-CELL",  "Test & Inspection Cell"),
        ("REWORK",     "Rework Station"),
    ]
    rows = [{"id": nxt + i, "name": f"{nm[:14]} {nxt+i}", "description": ds,
              "site_id": site_ids[i % len(site_ids)], "user_id": 1,
              "info": f"Cell capacity: {random.randint(500,2000)} panels/day",
              "is_active": True}
             for i, (nm, ds) in enumerate(cells_def)]
    sql = ("INSERT INTO public.cells (id,name,description,site_id,user_id,info,is_active) "
           "VALUES (%(id)s,%(name)s,%(description)s,%(site_id)s,%(user_id)s,%(info)s,%(is_active)s) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="cells")
    return [r["id"] for r in rows]


def gen_machine_groups(conn, cell_ids: list[int]) -> list[int]:
    nxt = _get_next_id(conn, "public.machine_groups")
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
    rows = [{"id": nxt + i, "name": f"{nm[:14]} {nxt+i}", "description": ds,
              "user_id": 1, "cell_id": cell_ids[i % len(cell_ids)],
              "is_active": True, "failure": False}
             for i, (nm, ds) in enumerate(mg_def)]
    sql = ("INSERT INTO public.machine_groups "
           "(id,name,description,user_id,cell_id,is_active,failure) "
           "VALUES (%(id)s,%(name)s,%(description)s,%(user_id)s,%(cell_id)s,"
           "%(is_active)s,%(failure)s) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="machine_groups")
    return [r["id"] for r in rows]


def gen_stations(conn, machine_group_ids: list[int]) -> list[int]:
    nxt = _get_next_id(conn, "public.stations")
    station_templates = [
        ("SPI-01",      "Koh Young KY8030-3"),
        ("SPI-02",      "Koh Young KY8030-3 (backup)"),
        ("PNP-01",      "FUJI NXT III - 1"),
        ("PNP-02",      "FUJI NXT III - 2"),
        ("PNP-03",      "Yamaha YSM40R"),
        ("REFLOW-01",   "Heller 1913 MkIII"),
        ("REFLOW-02",   "Rehm Convection Plus 7"),
        ("AOI-TOP-01",  "Omron VT-S730"),
        ("AOI-BOT-01",  "Omron VT-S530"),
        ("ICT-01",      "Teradyne i1000D"),
        ("ICT-02",      "Keysight 3070"),
        ("FUNC-01",     "Custom Fixture #1"),
        ("FUNC-02",     "Custom Fixture #2"),
        ("WAVE-01",     "Ersa POWERFLOW e N2"),
        ("XRAY-01",     "Saki BF-X1800F"),
        ("PROG-01",     "Data IO FlashCORE III"),
        ("CLEAN-01",    "Zestron Aqua Kleenr"),
    ]
    rows = [{"id": nxt + i, "name": f"{nm[:14]}-{nxt+i}", "description": desc,
              "machine_group_id": machine_group_ids[i % len(machine_group_ids)],
              "is_active": True, "user_id": 1,
              "info": f"Serial: SN{random.randint(10000,99999)}"}
             for i, (nm, desc) in enumerate(station_templates)]
    sql = ("INSERT INTO public.stations "
           "(id,name,description,machine_group_id,is_active,user_id,info) "
           "VALUES (%(id)s,%(name)s,%(description)s,%(machine_group_id)s,"
           "%(is_active)s,%(user_id)s,%(info)s) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="stations")
    return [r["id"] for r in rows]


def gen_lines(conn, n: int = 4) -> list[int]:
    nxt = _get_next_id(conn, "public.lines")
    now = datetime.now()
    defs = [("SMT-A", "SMT Production Line A - Double-sided"),
            ("SMT-B", "SMT Production Line B - Single-sided"),
            ("THT-1", "Through-Hole Assembly Line 1"),
            ("TEST-1","Test & Inspection Line 1")]
    rows = [{"id": nxt + i, "name": f"{nm[:14]} {nxt+i}", "description": ds,
              "date": _dt_str(now - timedelta(days=365)), "user_id": 1}
             for i, (nm, ds) in enumerate(defs[:n])]
    sql = ("INSERT INTO public.lines (id,name,description,date,user_id) "
           "VALUES (%(id)s,%(name)s,%(description)s,CAST(%(date)s AS timestamp),%(user_id)s) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="lines")
    return [r["id"] for r in rows]


def gen_erp_groups(conn, window_start: datetime) -> list[int]:
    nxt = _get_next_id(conn, "public.erp_groups")
    now = datetime.now()
    rows = []
    for i, (code, desc, _) in enumerate(PCB_PROCESS_STEPS):
        rows.append({
            "id": nxt + i, "state": 1,
            "erpgroup_no": f"ERP-{code}-{nxt+i}",
            "erp_group_description": desc,
            "erpsystem": "SAP",
            "sequential": True, "separate_station": False, "fixed_layer": False,
            "created_on": _dt_str(window_start), "edited_on": _dt_str(now),
            "modified_by": 1, "user_id": 1, "cst_id": None, "valid": True,
        })
    sql = ("INSERT INTO public.erp_groups "
           "(id,state,erpgroup_no,erp_group_description,erpsystem,sequential,"
           "separate_station,fixed_layer,created_on,edited_on,modified_by,user_id,cst_id,valid) "
           "VALUES (%(id)s,%(state)s,%(erpgroup_no)s,%(erp_group_description)s,%(erpsystem)s,"
           "%(sequential)s,%(separate_station)s,%(fixed_layer)s,"
           "CAST(%(created_on)s AS timestamp),CAST(%(edited_on)s AS timestamp),"
           "%(modified_by)s,%(user_id)s,%(cst_id)s,%(valid)s) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="erp_groups")
    return [r["id"] for r in rows]


def gen_failure_types(conn, failure_group_ids: list[int], site_ids: list[int]) -> list[int]:
    nxt = _get_next_id(conn, "public.failure_types", id_col="failure_type_id")
    now = datetime.now()
    failures = [
        ("FT-SB01", "Solder Bridge",           0),
        ("FT-SB02", "Insufficient Solder",      0),
        ("FT-SB03", "Solder Ball",              0),
        ("FT-CM01", "Missing Component",        1),
        ("FT-CM02", "Wrong Component",          1),
        ("FT-CM03", "Tombstoning",              1),
        ("FT-CM04", "Component Rotation Error", 1),
        ("FT-PB01", "PCB Scratch",              2),
        ("FT-PB02", "Board Contamination",      2),
        ("FT-PR01", "Paste Insufficiency",      3),
        ("FT-PR02", "Paste Bridging",           3),
        ("FT-PR03", "Placement Offset",         3),
        ("FT-EL01", "Short Circuit",            4),
        ("FT-EL02", "Open Circuit",             4),
        ("FT-EL03", "ESD Damage",               4),
        ("FT-CS01", "Flux Residue",             5),
        ("FT-CS02", "Cosmetic Scratch",         5),
    ]
    rows = [{"failure_type_id": nxt + i, "failure_type_code": f"{code}-{nxt+i}",
              "failure_type_desc": desc,
              "site_id": site_ids[i % len(site_ids)],
              "failure_group_id": failure_group_ids[grp % len(failure_group_ids)],
              "created_at": _dt_str(now), "updated_at": _dt_str(now)}
             for i, (code, desc, grp) in enumerate(failures)]
    sql = ("INSERT INTO public.failure_types "
           "(failure_type_id,failure_type_code,failure_type_desc,site_id,failure_group_id,"
           "created_at,updated_at) "
           "VALUES (%(failure_type_id)s,%(failure_type_code)s,%(failure_type_desc)s,"
           "%(site_id)s,%(failure_group_id)s,"
           "CAST(%(created_at)s AS timestamp),CAST(%(updated_at)s AS timestamp)) "
           "ON CONFLICT (failure_type_id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="failure_types")
    
    # Fetch the actual IDs from the DB to ensure they exist (especially if conflicts occurred)
    with conn.cursor() as cur:
        cur.execute("SELECT failure_type_id FROM public.failure_types ORDER BY failure_type_id;")
        return [r[0] for r in cur.fetchall()]


def gen_machine_conditions_ref(conn, machine_condition_group_ids: list[int]) -> list[int]:
    nxt = _get_next_id(conn, "public.machine_conditions")
    now = datetime.now()
    rows = []
    for i, c in enumerate(ALLOWED_CONDITIONS):
        g_id = machine_condition_group_ids[(c["group_id"] - 1) % len(machine_condition_group_ids)]
        rows.append({
            "id": nxt + i, "group_id": g_id,
            "condition_name": c["code"], "condition_description": c["desc"],
            "color_rgb": c["color"], "is_active": True,
            "created_at": _dt_str(now), "updated_at": _dt_str(now),
        })
    
    # Change from ON CONFLICT DO NOTHING to ON CONFLICT DO UPDATE
    sql = ("INSERT INTO public.machine_conditions "
           "(id,group_id,condition_name,condition_description,color_rgb,is_active,"
           "created_at,updated_at) "
           "VALUES (%(id)s,%(group_id)s,%(condition_name)s,%(condition_description)s,"
           "%(color_rgb)s,%(is_active)s,"
           "CAST(%(created_at)s AS timestamp),CAST(%(updated_at)s AS timestamp)) "
           "ON CONFLICT (condition_name) DO UPDATE SET "
           "group_id = EXCLUDED.group_id, "
           "condition_description = EXCLUDED.condition_description, "
           "color_rgb = EXCLUDED.color_rgb, "
           "updated_at = EXCLUDED.updated_at "
           "RETURNING id;")
    
    # Need to modify _bulk_insert to handle RETURNING
    # Or simpler: just skip if exists
    _bulk_insert(conn, sql, rows, label="machine_conditions (reference)")
    
    # Fetch the actual IDs from the DB to ensure they exist (especially if conflicts occurred)
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM public.machine_conditions ORDER BY id;")
        return [r[0] for r in cur.fetchall()]


def gen_part_groups(conn, part_group_type_ids: list[int]) -> list[int]:
    nxt = _get_next_id(conn, "public.part_groups")
    now = datetime.now()
    groups = [
        ("PCBA-FG",   "Finished PCB Assemblies",    0, "EA"),
        ("PCBA-WIP",  "WIP PCB Sub-Assemblies",      1, "EA"),
        ("SMD-COMP",  "SMD Components Pool",         2, "EA"),
        ("THT-COMP",  "THT Components Pool",         2, "EA"),
        ("RAW-PCB",   "Bare PCB Boards",             3, "EA"),
        ("CONSM",     "Consumables (solder/flux)",   3, "KG"),
    ]
    rows = []
    for i, (nm, ds, gti, pt) in enumerate(groups):
        rows.append({
            "id": nxt + i, "name": f"{nm[:14]} {nxt+i}", "description": ds, "user_id": 1,
            "part_type": pt, "costs": random.randint(10, 500),
            "is_active": True, "circulating_lot": random.randint(50, 500),
            "automatic_emptying": 0, "master_workplan": None, "comment": None,
            "state": 1, "material_transfer": False,
            "created_on": _dt_str(now), "edited_on": _dt_str(now),
            "part_group_type_id": part_group_type_ids[gti % len(part_group_type_ids)],
        })
    sql = ("INSERT INTO public.part_groups "
           "(id,name,description,user_id,part_type,costs,is_active,circulating_lot,"
           "automatic_emptying,master_workplan,comment,state,material_transfer,"
           "created_on,edited_on,part_group_type_id) "
           "VALUES (%(id)s,%(name)s,%(description)s,%(user_id)s,%(part_type)s,%(costs)s,"
           "%(is_active)s,%(circulating_lot)s,%(automatic_emptying)s,%(master_workplan)s,"
           "%(comment)s,%(state)s,%(material_transfer)s,"
           "CAST(%(created_on)s AS timestamp),CAST(%(edited_on)s AS timestamp),"
           "%(part_group_type_id)s) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="part_groups")
    return [r["id"] for r in rows]


def gen_part_master(conn, part_type_ids: list[int], part_group_ids: list[int],
                    machine_group_ids: list[int], site_ids: list[int],
                    n_products: int = 8) -> tuple[list[int], list[str]]:
    nxt = _get_next_id(conn, "public.part_master")
    now = datetime.now()
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
    for i, (pn, desc) in enumerate(product_names[:n_products]):
        rows.append({
            "id": nxt + i, "part_number": f"{pn}-{nxt+i}", "description": desc,
            "part_status": "active",
            "parttype_id": part_type_ids[0],
            "partgroup_id": part_group_ids[0],
            "case_type": random.choice(["SMT", "MIXED"]),
            "product": True, "panel": True, "variant": False,
            "machine_group_id": machine_group_ids[1 % len(machine_group_ids)],
            "material_info": "FR4 1.6mm HASL",
            "parts_index": nxt + i, "edit_order_based_bom": False,
            "site_id": site_ids[i % len(site_ids)],
            "unit_id": None, "material_code": f"MAT-{pn}-{nxt+i}",
            "no_of_panels": random.choice([1, 2, 4]),
            "customer_material_number": f"CUST-{pn}-{nxt+i}",
            "created_at": _dt_str(now), "updated_at": _dt_str(now),
        })
    comp_offset = len(rows)
    for ci, (prefix, ctype, pkg) in enumerate(PCB_COMPONENT_TYPES):
        for vi in range(3):
            pn = f"{prefix}{ci+1:02d}{vi+1:02d}"
            rows.append({
                "id": nxt + comp_offset + ci * 3 + vi,
                "part_number": f"{pn}-{nxt+comp_offset+ci*3+vi}",
                "description": f"{ctype} {pkg} variant {vi+1}",
                "part_status": "active",
                "parttype_id": part_type_ids[2 % len(part_type_ids)],
                "partgroup_id": part_group_ids[2 % len(part_group_ids)],
                "case_type": pkg, "product": False, "panel": False, "variant": vi > 0,
                "machine_group_id": None,
                "material_info": None,
                "parts_index": comp_offset + ci * 3 + vi + 1,
                "edit_order_based_bom": False,
                "site_id": site_ids[0],
                "unit_id": None, "material_code": f"MAT-{pn}",
                "no_of_panels": 1, "customer_material_number": None,
                "created_at": _dt_str(now), "updated_at": _dt_str(now),
            })
    sql = ("INSERT INTO public.part_master "
           "(id,part_number,description,part_status,parttype_id,partgroup_id,case_type,"
           "product,panel,variant,machine_group_id,material_info,parts_index,"
           "edit_order_based_bom,site_id,unit_id,material_code,no_of_panels,"
           "customer_material_number,created_at,updated_at) "
           "VALUES (%(id)s,%(part_number)s,%(description)s,%(part_status)s,%(parttype_id)s,"
           "%(partgroup_id)s,%(case_type)s,%(product)s,%(panel)s,%(variant)s,"
           "%(machine_group_id)s,%(material_info)s,%(parts_index)s,%(edit_order_based_bom)s,"
           "%(site_id)s,%(unit_id)s,%(material_code)s,%(no_of_panels)s,"
           "%(customer_material_number)s,"
           "CAST(%(created_at)s AS timestamp),CAST(%(updated_at)s AS timestamp)) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="part_master")
    return [r["id"] for r in rows], [r["part_number"] for r in rows]

# ---------------------------------------------------------------------------
# Layer 2
# ---------------------------------------------------------------------------

def gen_line_station_association(conn, line_ids: list[int],
                                 station_ids: list[int]) -> None:
    now_str = _dt_str(datetime.now())
    rows: list[dict[str, Any]] = []
    seen: set[tuple[int, int]] = set()
    spl = max(3, len(station_ids) // max(1, len(line_ids)))
    for li, lid in enumerate(line_ids):
        for sid in station_ids[li * spl:(li + 1) * spl]:
            key = (lid, sid)
            if key not in seen:
                seen.add(key)
                rows.append({"line_id": lid, "station_id": sid, "created_at": now_str})
    sql = ("INSERT INTO public.line_station_association (line_id,station_id,created_at) "
           "VALUES (%(line_id)s,%(station_id)s,CAST(%(created_at)s AS timestamp)) "
           "ON CONFLICT DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="line_station_association")


def gen_assign_stations_to_erpgrp(conn, station_ids: list[int],
                                   erp_group_ids: list[int]) -> None:
    nxt = _get_next_id(conn, "public.assign_stations_to_erpgrp")
    rows = [{"id": nxt + i, "station_id": sid,
              "erp_group_id": erp_group_ids[i % len(erp_group_ids)],
              "station_type": "production", "user_id": 1}
             for i, sid in enumerate(station_ids)]
    sql = ("INSERT INTO public.assign_stations_to_erpgrp "
           "(id,station_id,erp_group_id,station_type,user_id) "
           "VALUES (%(id)s,%(station_id)s,%(erp_group_id)s,%(station_type)s,%(user_id)s) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="assign_stations_to_erpgrp")


def gen_work_plans(conn, site_ids: list[int], client_ids: list[int],
                   company_code_ids: list[int], product_part_numbers: list[str],
                   window_start: datetime, window_end: datetime) -> list[int]:
    nxt = _get_next_id(conn, "public.work_plans")
    now = datetime.now()
    rows = []
    valid_from = window_start - timedelta(days=30)
    for i, pn in enumerate(product_part_numbers):
        rows.append({
            "id": nxt + i, 
            "version": 1, 
            "is_current": "Y",
            "previous_version_id": None, 
            "deleted_at": None,
            "user_id": 1,
            "site_id": site_ids[i % len(site_ids)],
            "client_id": client_ids[i % len(client_ids)],
            "company_id": company_code_ids[i % len(company_code_ids)],
            "source": 1,  # numeric, not string
            "status": 1,  # numeric, not string
            "product_vers_id": i + 1,  # numeric
            "workplan_status": "R",  # "R" for released
            "part_no": pn,  # varchar
            "part_desc": f"Work plan for {pn}",  # varchar
            "workplan_desc": f"SMT/THT production plan v1 - {pn}",  # varchar
            "workplan_type": random.choice(["SMT", "MIX", "THT"]),  # varchar
            "workplan_info": None,  # varchar
            "workplan_version_erp": f"WP-{nxt+i:04d}-V1",  # varchar
            "aps_info1": None,  # varchar
            "aps_info2": None,  # varchar
            "created_at": _dt_str(valid_from),  # timestamp
            "updated_at": _dt_str(now),  # timestamp
            "workplan_valid_from": _dt_str(valid_from),  # timestamp
            "workplan_valid_to": None,  # timestamp
        })
    
    sql = ("INSERT INTO public.work_plans "
           "(id,version,is_current,previous_version_id,deleted_at,user_id,site_id,client_id,"
           "company_id,source,status,product_vers_id,workplan_status,part_no,part_desc,"
           "workplan_desc,workplan_type,workplan_info,workplan_version_erp,aps_info1,aps_info2,"
           "created_at,updated_at,workplan_valid_from,workplan_valid_to) "
           "VALUES (%(id)s,%(version)s,%(is_current)s,%(previous_version_id)s,%(deleted_at)s,"
           "%(user_id)s,%(site_id)s,%(client_id)s,%(company_id)s,%(source)s,%(status)s,"
           "%(product_vers_id)s,%(workplan_status)s,%(part_no)s,%(part_desc)s,"
           "%(workplan_desc)s,%(workplan_type)s,%(workplan_info)s,%(workplan_version_erp)s,"
           "%(aps_info1)s,%(aps_info2)s,"
           "CAST(%(created_at)s AS timestamp),CAST(%(updated_at)s AS timestamp),"
           "CAST(%(workplan_valid_from)s AS timestamp),%(workplan_valid_to)s) "
           "ON CONFLICT (id) DO NOTHING;")
    
    _bulk_insert(conn, sql, rows, label="work_plans")
    return [r["id"] for r in rows]

def gen_work_steps(conn, workplan_ids: list[int], erp_group_ids: list[int],
                   window_start: datetime) -> None:
    nxt = _get_next_id(conn, "public.work_steps")
    now = datetime.now()
    rows: list[dict[str, Any]] = []
    step_no = 0
    for wp_id in workplan_ids:
        for step_i, (code, desc, step_type) in enumerate(PCB_PROCESS_STEPS):
            eid = erp_group_ids[step_i % len(erp_group_ids)]
            rows.append({
                "id": nxt + step_no, "workplan_id": wp_id, "erp_group_id": eid,
                "workstep_no": (step_i + 1) * 10, "step": step_i + 1,
                "setup_time": round(random.uniform(15, 60), 2),
                "te_person": 1,
                "te_machine": round(AVG_CYCLE_TIME_SEC / 60, 2),
                "te_time_base": 60, "te_qty_base": 1,
                "transport_time": round(random.uniform(1, 10), 2),
                "wait_time": round(random.uniform(0, 30), 2),
                "status": 1, "equ_id": None, "msl_relevant": 0, "msl_offset": 0,
                "panel_count": random.choice([1, 2, 4]),
                "workstep_desc": desc, "erp_grp_no": f"ERP-{code}",
                "erp_grp_desc": desc, "time_unit": "MIN",
                "setup_flag": "X" if step_i == 0 else "",
                "workstep_version_erp": f"WS-{wp_id:04d}-{step_i+1:02d}",
                "info": step_type, "confirmation": "AUTO",
                "sequentiell": "X", "workstep_type": step_type,
                "traceflag": "X", "step_type": "PRODUCTION",
                "created_at": _dt_str(window_start), "stamp": _dt_str(now),
            })
            step_no += 1
    sql = ("INSERT INTO public.work_steps "
           "(id,workplan_id,erp_group_id,workstep_no,step,setup_time,te_person,te_machine,"
           "te_time_base,te_qty_base,transport_time,wait_time,status,equ_id,msl_relevant,"
           "msl_offset,panel_count,workstep_desc,erp_grp_no,erp_grp_desc,time_unit,"
           "setup_flag,workstep_version_erp,info,confirmation,sequentiell,workstep_type,"
           "traceflag,step_type,created_at,stamp) "
           "VALUES (%(id)s,%(workplan_id)s,%(erp_group_id)s,%(workstep_no)s,%(step)s,"
           "%(setup_time)s,%(te_person)s,%(te_machine)s,%(te_time_base)s,%(te_qty_base)s,"
           "%(transport_time)s,%(wait_time)s,%(status)s,%(equ_id)s,%(msl_relevant)s,"
           "%(msl_offset)s,%(panel_count)s,%(workstep_desc)s,%(erp_grp_no)s,"
           "%(erp_grp_desc)s,%(time_unit)s,%(setup_flag)s,%(workstep_version_erp)s,"
           "%(info)s,%(confirmation)s,%(sequentiell)s,%(workstep_type)s,%(traceflag)s,"
           "%(step_type)s,CAST(%(created_at)s AS timestamp),CAST(%(stamp)s AS timestamp)) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="work_steps")


def gen_bom_headers(conn, product_part_master_ids: list[int],
                    window_start: datetime) -> list[int]:
    nxt = _get_next_id(conn, "public.bom_headers")
    now = datetime.now()
    vf = window_start - timedelta(days=60)
    rows = [{"id": nxt + i, "description": f"BOM for part {pm_id} - Rev A",
              "valid_from": _dt_str(vf), "valid_to": None,
              "created_at": _dt_str(vf), "last_updated": _dt_str(now),
              "part_master_id": pm_id, "state": "released",
              "version": 1, "is_current": "Y",
              "previous_version_id": None, "deleted_at": None,
              "created_by": "system", "updated_by": "system"}
             for i, pm_id in enumerate(product_part_master_ids)]
    sql = ("INSERT INTO public.bom_headers "
           "(id,description,valid_from,valid_to,created_at,last_updated,part_master_id,"
           "state,version,is_current,previous_version_id,deleted_at,created_by,updated_by) "
           "VALUES (%(id)s,%(description)s,"
           "CAST(%(valid_from)s AS timestamp),%(valid_to)s,"
           "CAST(%(created_at)s AS timestamp),CAST(%(last_updated)s AS timestamp),"
           "%(part_master_id)s,%(state)s,%(version)s,%(is_current)s,"
           "%(previous_version_id)s,%(deleted_at)s,%(created_by)s,%(updated_by)s) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="bom_headers")
    return [r["id"] for r in rows]


def gen_bom_items(conn, bom_header_ids: list[int], all_part_master_ids: list[int],
                  product_part_ids: list[int], station_ids: list[int],
                  window_start: datetime) -> None:
    nxt = _get_next_id(conn, "public.bom_items")
    now = datetime.now()
    component_ids = [p for p in all_part_master_ids if p not in set(product_part_ids)]
    if not component_ids:
        component_ids = all_part_master_ids
    rows: list[dict[str, Any]] = []
    item_no = 0
    for bh_id in bom_header_ids:
        n_comps = random.randint(8, 25)
        comps = random.sample(component_ids, min(n_comps, len(component_ids)))
        for layer_i, comp_id in enumerate(comps):
            rows.append({
                "id": nxt + item_no,
                "bom_header_id": bh_id, "part_master_id": comp_id,
                "quantity": random.randint(1, 20), "is_product": False,
                "component_name": f"Comp-{comp_id:04d}",
                "setup": layer_i == 0,
                "station_id": station_ids[item_no % len(station_ids)] if random.random() < 0.6 else None,
                "manual_test": random.random() < 0.05,
                "layer": (layer_i % 2) + 1,
                "created_at": _dt_str(window_start), "updated_at": _dt_str(now),
            })
            item_no += 1
    sql = ("INSERT INTO public.bom_items "
           "(id,bom_header_id,part_master_id,quantity,is_product,component_name,"
           "setup,station_id,manual_test,layer,created_at,updated_at) "
           "VALUES (%(id)s,%(bom_header_id)s,%(part_master_id)s,%(quantity)s,"
           "%(is_product)s,%(component_name)s,%(setup)s,%(station_id)s,%(manual_test)s,"
           "%(layer)s,CAST(%(created_at)s AS timestamp),CAST(%(updated_at)s AS timestamp)) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="bom_items")


def gen_boms(conn, product_part_master_ids: list[int],
             window_start: datetime) -> list[int]:
    nxt = _get_next_id(conn, "public.boms")
    vf = window_start - timedelta(days=60)
    rows = [{"id": nxt + i, "state": "released",
              "bom_type": random.choice(["PRODUCTION", "ENGINEERING"]),
              "bom_version": 1,
              "bom_version_valid_from": _d_str(vf),
              "bom_version_valid_to": None,
              "user_id": 1, 
              "part_number": pm_id}  # pm_id is integer, not string!
             for i, pm_id in enumerate(product_part_master_ids)]
    sql = ("INSERT INTO public.boms "
           "(id,state,bom_type,bom_version,bom_version_valid_from,bom_version_valid_to,"
           "user_id,part_number) "
           "VALUES (%(id)s,%(state)s,%(bom_type)s,%(bom_version)s,"
           "CAST(%(bom_version_valid_from)s AS date),%(bom_version_valid_to)s,"
           "%(user_id)s,%(part_number)s) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="boms")
    return [r["id"] for r in rows]


def gen_bom_insertion(conn, product_pns: list[str], bom_ids: list[int], bom_header_ids: list[int]) -> None:
    try:
        nxt = _get_next_id(conn, "public.bom_insertion")
    except Exception:
        print("  - bom_insertion: table not found, skipping")
        return
    n = min(len(product_pns), len(bom_ids), len(bom_header_ids))
    now = datetime.now()
    rows = [{"id": nxt + i, 
              "part_number": product_pns[i],
              "bom_master_version": 1, 
              "bom_slave_version": 1,
              "workplan_master_version": 1,
              "workplan_slave_version": 1,
              "created_at": _dt_str(now)}
             for i in range(n)]
    sql = ("INSERT INTO public.bom_insertion "
           "(id,part_number,bom_master_version,bom_slave_version,"
           "workplan_master_version,workplan_slave_version,created_at) "
           "VALUES (%(id)s,%(part_number)s,%(bom_master_version)s,%(bom_slave_version)s,"
           "%(workplan_master_version)s,%(workplan_slave_version)s,"
           "CAST(%(created_at)s AS timestamp)) ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="bom_insertion")

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


def gen_work_orders(conn, client_ids: list[int], company_code_ids: list[int],
                    site_ids: list[int], product_part_numbers: list[str],
                    product_part_ids: list[int],
                    window_start: datetime, window_end: datetime,
                    n: int, wo_qty_min: int, wo_qty_max: int,
                    ) -> tuple[list[int], dict[int, WOWindow]]:
    nxt = _get_next_id(conn, "public.work_orders")
    plan = _state_plan(n)
    rows: list[dict[str, Any]] = []
    windows: dict[int, WOWindow] = {}

    for i in range(n):
        rid   = nxt + i
        state = plan[i] if i < len(plan) else "active"
        pn_idx = random.randrange(len(product_part_numbers))
        pn     = product_part_numbers[pn_idx]
        pid    = product_part_ids[pn_idx]
        cid    = company_code_ids[i % len(company_code_ids)]
        qty    = random.randint(wo_qty_min, wo_qty_max)
        dur    = _wo_duration(qty)

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
                window_end + timedelta(hours=random.randint(1, 720)))
            delivery_dt = start_dt + dur

        created_hi = _clamp_dt(start_dt - timedelta(hours=1), window_start, window_end)
        created_dt = (window_start if created_hi <= window_start
                      else _random_between(window_start, created_hi))
        stamp_dt = _clamp_dt(
            _random_between(created_dt + timedelta(minutes=1), start_dt),
            window_start, window_end)

        aps_s = start_dt + timedelta(seconds=random.randint(-3600, 3600))
        aps_e = delivery_dt + timedelta(seconds=random.randint(-3600, 3600))
        if aps_e < aps_s:
            aps_e = aps_s + timedelta(hours=1)

        rows.append({
            "id": rid, "workorder_no": f"WO-{rid:06d}",
            "workorder_type": random.choice(["P", "R", "T"]), # P=Prod, R=Rework, T=Proto
            "part_number": pn, "workorder_qty": qty,
            "startdate": _dt_str(start_dt), "deliverydate": _dt_str(delivery_dt),
            "unit": random.choice(["EA", "PCS"]),
            "bom_version": "1", "workplan_type": random.choice(["SMT", "MIX", "THT"]),
            "backflush": None, "source": 1, "workplan_version": "1",
            "workorder_desc": f"{pn} production batch",
            "bom_info": None,
            "workplan_valid_from": _dt_str(window_start - timedelta(days=60)),
            "workorder_no_ext": f"EXT-{rid:06d}",
            "info1": None, "info2": None, "info3": None, "info4": None, "info5": None,
            "ninfo1": None, "ninfo2": None,
            "status": "R", # R=Released
            "created": _dt_str(created_dt), "stamp": _dt_str(stamp_dt),
            "site_id": site_ids[i % len(site_ids)],
            "client_id": client_ids[i % len(client_ids)],
            "company_id": cid,
            "drawing_no": f"DWG-{pn}-R1",
            "workorder_state": state[0].upper() if state else "A", # Use first letter
            "parent_workorder": None, "controller": "SAP",
            "bareboard_no": f"BB-{rid:06d}",
            "aps_planning_start_date": _dt_str(aps_s),
            "aps_planning_stamp": _dt_str(aps_e),
            "aps_planning_end_date": _dt_str(aps_e),
            "aps_order_fixation": None,
        })

        ev_start = max(start_dt, window_start)
        ev_end   = min(delivery_dt, window_end)
        if state in {"active", "open"}:
            ev_end = max(ev_start + timedelta(hours=1), window_end)
        if ev_end > ev_start and state != "planned":
            windows[rid] = WOWindow(rid, pn, pid, cid, ev_start, ev_end, state, qty)

    sql = ("INSERT INTO public.work_orders "
           "(id,workorder_no,workorder_type,part_number,workorder_qty,startdate,deliverydate,"
           "unit,bom_version,workplan_type,backflush,source,workplan_version,workorder_desc,"
           "bom_info,workplan_valid_from,workorder_no_ext,info1,info2,info3,info4,info5,"
           "ninfo1,ninfo2,status,created,stamp,site_id,client_id,company_id,drawing_no,"
           "workorder_state,parent_workorder,controller,bareboard_no,"
           "aps_planning_start_date,aps_planning_stamp,aps_planning_end_date,aps_order_fixation) "
           "VALUES (%(id)s,%(workorder_no)s,%(workorder_type)s,%(part_number)s,%(workorder_qty)s,"
           "CAST(%(startdate)s AS timestamp),CAST(%(deliverydate)s AS timestamp),"
           "%(unit)s,%(bom_version)s,%(workplan_type)s,%(backflush)s,%(source)s,"
           "%(workplan_version)s,%(workorder_desc)s,%(bom_info)s,"
           "CAST(%(workplan_valid_from)s AS timestamp),%(workorder_no_ext)s,"
           "%(info1)s,%(info2)s,%(info3)s,%(info4)s,%(info5)s,%(ninfo1)s,%(ninfo2)s,"
           "%(status)s,CAST(%(created)s AS timestamp),CAST(%(stamp)s AS timestamp),"
           "%(site_id)s,%(client_id)s,%(company_id)s,%(drawing_no)s,%(workorder_state)s,"
           "%(parent_workorder)s,%(controller)s,%(bareboard_no)s,"
           "CAST(%(aps_planning_start_date)s AS timestamp),"
           "CAST(%(aps_planning_stamp)s AS timestamp),"
           "CAST(%(aps_planning_end_date)s AS timestamp),%(aps_order_fixation)s) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="work_orders")
    return [r["id"] for r in rows], windows


def gen_serial_numbers(conn, wo_windows: dict[int, WOWindow]) -> dict[int, list[int]]:
    nxt = _get_next_id(conn, "public.serial_numbers")
    now = datetime.now()
    rows: list[dict[str, Any]] = []
    wo_snr_map: dict[int, list[int]] = {}
    snr_no = 0

    for wid, w in wo_windows.items():
        is_partial = w.state in {"open", "active"}
        target = max(1, int(w.qty * random.uniform(0.40, 0.85)) if is_partial else w.qty)
        target = min(target, 2000)
        snr_ids: list[int] = []
        for pos in range(1, target + 1):
            rid = nxt + snr_no
            snr_ids.append(rid)
            snr_no += 1
            ts = _clamp_dt(
                w.event_start + timedelta(seconds=pos * AVG_CYCLE_TIME_SEC),
                w.event_start, w.event_end)
            rows.append({
                "id": rid,
                "serial_number": f"SN-{wid:06d}-{pos:05d}",
                "serial_number_pos": pos,
                "serial_number_ref_pos": None,
                "serial_number_active": "Y",
                "workorder_id": wid,
                "part_id": w.part_id,
                "created_by": 1,
                "company_code_id": w.company_id,
                "workorder_type": "P", # P=Prod
                "customer_part_number": None,
                "serial_number_type": "S",
                "cluster_name": None,
                "cluster_type": None,
                "serial_number_ref": None,
                "created_on": _dt_str(ts),
                "updated_on": _dt_str(_clamp_dt(ts + timedelta(minutes=5), ts, now)),
            })
        wo_snr_map[wid] = snr_ids

    sql = ("INSERT INTO public.serial_numbers "
           "(id,serial_number,serial_number_pos,serial_number_ref_pos,serial_number_active,"
           "workorder_id,part_id,created_by,company_code_id,workorder_type,"
           "customer_part_number,serial_number_type,cluster_name,cluster_type,"
           "serial_number_ref,created_on,updated_on) "
           "VALUES (%(id)s,%(serial_number)s,%(serial_number_pos)s,%(serial_number_ref_pos)s,"
           "%(serial_number_active)s,%(workorder_id)s,%(part_id)s,%(created_by)s,"
           "%(company_code_id)s,%(workorder_type)s,%(customer_part_number)s,"
           "%(serial_number_type)s,%(cluster_name)s,%(cluster_type)s,%(serial_number_ref)s,"
           "CAST(%(created_on)s AS timestamp),CAST(%(updated_on)s AS timestamp)) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label=f"serial_numbers ({len(rows)} rows)")
    return wo_snr_map


def gen_active_workorders(conn, wo_windows: dict[int, WOWindow],
                           station_ids: list[int], window_end: datetime,
                           n: int) -> None:
    active_wids = [wid for wid, w in wo_windows.items()
                   if w.state in {"active", "open"}]
    if not active_wids:
        print("  - active_workorders: no active/open WOs")
        return
    nxt = _get_next_id(conn, "public.active_workorders")
    now = datetime.now()
    n   = min(n, len(active_wids))
    rows = []
    for i in range(n):
        wid = active_wids[i % len(active_wids)]
        t = _clamp_dt(now - timedelta(hours=random.uniform(0.5, 8)),
                      window_end - timedelta(days=1), now)
        rows.append({
            "id": nxt + i, "workorder_id": wid,
            "station_id": station_ids[i % len(station_ids)],
            "state": 1, "process_layer": 0,
            "created_at": _dt_str(t),
            "updated_at": _dt_str(_clamp_dt(t + timedelta(minutes=random.randint(5, 60)), t, now)),
        })
    sql = ("INSERT INTO public.active_workorders "
           "(id,workorder_id,station_id,state,process_layer,created_at,updated_at) "
           "VALUES (%(id)s,%(workorder_id)s,%(station_id)s,%(state)s,%(process_layer)s,"
           "CAST(%(created_at)s AS timestamp),CAST(%(updated_at)s AS timestamp)) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label="active_workorders")

# ---------------------------------------------------------------------------
# Layer 4
# ---------------------------------------------------------------------------

def gen_bookings(conn, wo_windows: dict[int, WOWindow],
                 wo_snr_map: dict[int, list[int]],
                 station_ids: list[int], failure_type_ids: list[int],
                 pass_p: float, fail_p: float, scrap_p: float,
                 match_qty: bool, n_random: int,
                 window_end: datetime) -> None:
    nxt = _get_next_id(conn, "public.bookings")
    now = datetime.now()

    def _state(wo_state: str) -> str:
        bp = min(1.0, pass_p + (0.05 if wo_state in {"delivered", "finished"} else 0))
        r  = random.random()
        if r < bp:           return "Pass" # Pass
        if r < bp + fail_p:  return "Fail" # Fail
        return "Scrap" # Scrap

    eligible = {wid: w for wid, w in wo_windows.items()
                if w.state in {"open", "active", "finished", "delivered"}}
    rows: list[dict[str, Any]] = []
    bid = nxt

    if match_qty:
        for wid, w in eligible.items():
            is_partial = w.state in {"open", "active"}
            timestamps = _booking_timestamps(w.event_start, w.event_end, w.qty, is_partial)
            snr_ids    = wo_snr_map.get(wid, [])
            for pos, ts in enumerate(timestamps):
                st = _state(w.state)
                fi = None if st == "P" else int(random.choice(failure_type_ids))
                ud = _clamp_dt(ts + timedelta(seconds=random.randint(0, 300)), ts, now)
                rct = max(30.0, random.gauss(AVG_CYCLE_TIME_SEC, 30.0))
                snr_id = snr_ids[pos] if pos < len(snr_ids) else None
                rows.append({
                    "id": bid, "workorder_id": wid,
                    "station_id": station_ids[bid % len(station_ids)],
                    "failed_id": fi, "serial_number_id": snr_id,
                    "process_layer": random.randint(0, 3),
                    "date_of_booking": _dt_str(ts), "state": st,
                    "mesure_id": random.randint(1, len(MEASUREMENT_CATALOG)),
                    "real_cycle_time": round(rct, 3), "type": "SNR",
                    "created_at": _dt_str(ts), "updated_at": _dt_str(ud),
                })
                bid += 1
    else:
        eligible_list = list(eligible.values())
        for _ in range(n_random):
            w    = random.choice(eligible_list)
            wins = _shift_windows(w.event_start, w.event_end)
            ts   = (_random_between(*random.choice(wins)) if wins
                    else _random_between(w.event_start, w.event_end))
            st   = _state(w.state)
            fi   = None if st == "P" else int(random.choice(failure_type_ids))
            ud   = _clamp_dt(ts + timedelta(seconds=random.randint(0, 300)), ts, now)
            rct  = max(30.0, random.gauss(AVG_CYCLE_TIME_SEC, 30.0))
            snr_ids = wo_snr_map.get(w.workorder_id, [])
            snr_id  = random.choice(snr_ids) if snr_ids else None
            rows.append({
                "id": bid, "workorder_id": w.workorder_id,
                "station_id": station_ids[bid % len(station_ids)],
                "failed_id": fi, "serial_number_id": snr_id,
                "process_layer": random.randint(0, 3),
                "date_of_booking": _dt_str(ts), "state": st,
                "mesure_id": random.randint(1, len(MEASUREMENT_CATALOG)),
                "real_cycle_time": round(rct, 3),
                "type": "SNR" if random.random() < 0.7 else "batch",
                "created_at": _dt_str(ts), "updated_at": _dt_str(ud),
            })
            bid += 1

    sql = ("INSERT INTO public.bookings "
           "(id,workorder_id,station_id,failed_id,serial_number_id,process_layer,"
           "date_of_booking,state,mesure_id,real_cycle_time,\"type\",created_at,updated_at) "
           "VALUES (%(id)s,%(workorder_id)s,%(station_id)s,%(failed_id)s,%(serial_number_id)s,"
           "%(process_layer)s,CAST(%(date_of_booking)s AS timestamp),%(state)s,"
           "%(mesure_id)s,%(real_cycle_time)s,%(type)s,"
           "CAST(%(created_at)s AS timestamp),CAST(%(updated_at)s AS timestamp)) "
           "ON CONFLICT (id) DO NOTHING;")
    _bulk_insert(conn, sql, rows, label=f"bookings ({len(rows)} rows)")


def gen_measurement_data(conn, wo_windows: dict[int, WOWindow],
                          station_ids: list[int], n: int) -> None:
    nxt = _get_next_id(conn, "public.measurement_data", id_col='"ID"')
    now = datetime.now()
    eligible = list(wo_windows.values())
    rows: list[dict[str, Any]] = []
    for i in range(n):
        w    = random.choice(eligible)
        wins = _shift_windows(w.event_start, w.event_end)
        ts   = (_random_between(*random.choice(wins)) if wins
                else _random_between(w.event_start, w.event_end))
        m    = random.choice(MEASUREMENT_CATALOG)
        lo, hi = float(m["lower_limit"]), float(m["upper_limit"])
        val  = round((random.uniform(lo, hi) if random.random() < 0.95
                      else random.choice([random.uniform(lo * 0.85, lo),
                                          random.uniform(hi, hi * 1.15)])), 4)
        fin  = ts + timedelta(seconds=random.choice([1.0, 2.0, 3.0]))
        rows.append({
            "ID": nxt + i,
            "STATION_ID": station_ids[i % len(station_ids)],
            "WORKORDER_ID": w.workorder_id,
            "BOOK_DATE": _dt_str(ts),
            "MEASURE_NAME": m["measure_name"],
            "MEASURE_VALUE": str(val),
            "LOWER_LIMIT": str(lo),
            "UPPER_LIMIT": str(hi),
            "NOMINAL": str(float(m["nominal"])),
            "TOLERANCE": str(float(m["tolerance"])),
            "MEASURE_FAIL_CODE": 0,
            "MEASURE_TYPE": m["measure_type"],
            "created_at": _dt_str(ts), "updated_at": _dt_str(fin),
        })
    sql = ("INSERT INTO public.measurement_data "
           "(\"ID\",\"STATION_ID\",\"WORKORDER_ID\",\"BOOK_DATE\",\"MEASURE_NAME\","
           "\"MEASURE_VALUE\",\"LOWER_LIMIT\",\"UPPER_LIMIT\",\"NOMINAL\",\"TOLERANCE\","
           "\"MEASURE_FAIL_CODE\",\"MEASURE_TYPE\",created_at,updated_at) "
           "VALUES (%(ID)s,%(STATION_ID)s,%(WORKORDER_ID)s,"
           "CAST(%(BOOK_DATE)s AS timestamp),%(MEASURE_NAME)s,%(MEASURE_VALUE)s,"
           "%(LOWER_LIMIT)s,%(UPPER_LIMIT)s,%(NOMINAL)s,%(TOLERANCE)s,"
           "%(MEASURE_FAIL_CODE)s,%(MEASURE_TYPE)s,"
           "CAST(%(created_at)s AS timestamp),CAST(%(updated_at)s AS timestamp)) "
           "ON CONFLICT (\"ID\") DO NOTHING;")
    _bulk_insert(conn, sql, rows, label=f"measurement_data ({n} rows)")


def gen_machine_condition_data(conn, station_ids: list[int],
                                machine_condition_ids: list[int],
                                window_start: datetime, window_end: datetime,
                                n_max: int, downtime_target: float,
                                run_min_h: float, run_max_h: float,
                                down_min_h: float, down_max_h: float) -> None:
    nxt = _get_next_id(conn, "public.machine_condition_data")
    run_min_s  = run_min_h  * 3600.0
    run_max_s  = run_max_h  * 3600.0
    down_min_s = down_min_h * 3600.0
    down_max_s = down_max_h * 3600.0
    running_mc_id   = machine_condition_ids[-1]   # "Running" = last inserted
    non_running_ids = machine_condition_ids[:-1]

    rows: list[dict[str, Any]] = []
    produced = total_run = total_down = 0.0

    for station_id in station_ids:
        if produced >= n_max:
            break
        tot_s     = (window_end - window_start).total_seconds()
        run_left  = tot_s * (1.0 - downtime_target)
        down_left = tot_s * downtime_target

        for sw_start, sw_end in _shift_windows(window_start, window_end):
            if produced >= n_max:
                break
            t = sw_start
            while t < sw_end and produced < n_max:
                rem     = (sw_end - t).total_seconds()
                if rem <= 0:
                    break
                is_down = (random.random() < (down_left / max(1, down_left + run_left))
                           if down_left > 0 and run_left > 0 else (down_left > 0))
                if is_down:
                    dur   = min(rem, down_left, random.uniform(down_min_s, down_max_s))
                    mc_id = random.choice(non_running_ids)
                else:
                    dur   = min(rem, run_left, random.uniform(run_min_s, run_max_s))
                    mc_id = running_mc_id

                s_dt = t
                e_dt = _clamp_dt(s_dt + timedelta(seconds=float(dur)), sw_start, sw_end)
                if e_dt <= s_dt:
                    t = sw_end
                    continue

                seg = (e_dt - s_dt).total_seconds()
                if is_down: down_left -= seg; total_down += seg
                else:       run_left  -= seg; total_run  += seg

                rows.append({
                    "id": nxt + int(produced),
                    "date_from": _dt_str(s_dt), "date_to": _dt_str(e_dt),
                    "station_id": int(station_id), "condition_id": mc_id,
                    "level": "A" if is_down else "P",
                    "condition_created": _dt_str(s_dt),
                    "condition_stamp":   _dt_str(e_dt),
                    "condition_type": "s",
                    "color_rgb": "#8b1818" if is_down else "#13be1e",
                    "updated_at": _dt_str(e_dt),
                })
                produced += 1
                t = e_dt

    sql = ("INSERT INTO public.machine_condition_data "
           "(id,date_from,date_to,station_id,condition_id,level,condition_created,"
           "condition_stamp,condition_type,color_rgb,updated_at) "
           "VALUES (%(id)s,CAST(%(date_from)s AS timestamp),CAST(%(date_to)s AS timestamp),"
           "%(station_id)s,%(condition_id)s,%(level)s,"
           "CAST(%(condition_created)s AS timestamp),CAST(%(condition_stamp)s AS timestamp),"
           "%(condition_type)s,%(color_rgb)s,CAST(%(updated_at)s AS timestamp)) "
           "ON CONFLICT (id) DO NOTHING;")
    tot = total_run + total_down
    _bulk_insert(conn, sql, rows, label=f"machine_condition_data ({len(rows)} rows)")
    if tot > 0:
        print(f"    downtime: {total_down/tot*100:.1f}%  (target {downtime_target*100:.0f}%)")

"""""
# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
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
    cl_ids = gen_clients_api(company_code_ids=cc_ids, n=args.n_clients, token=API_TOKEN)
    conn.commit()
      


if __name__ == "__main__":
    main()