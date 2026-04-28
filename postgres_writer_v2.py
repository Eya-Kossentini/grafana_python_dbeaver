from __future__ import annotations

import os
import requests
import psycopg2
from dataclasses import dataclass
from dotenv import load_dotenv
from datetime import datetime
from constants import *
from generate_industrial_data_v2 import *
from datetime import datetime, timedelta


load_dotenv()

SOURCE_API_URL = "https://core_demo.momes-solutions.com"
API_TOKEN = os.getenv("API_TOKEN")

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}




@dataclass(frozen=True)
class PgConfig:
    host: str = "127.0.0.1"
    port: int = 5435
    dbname: str = "postgres"
    user: str = "postgres"
    password: str = "admin123"
    schema: str = "staging" 



def pg_table_qualified(pg: PgConfig) -> str:
    return f'{pg.schema}.{pg.table}'


def pg_connect(pg: PgConfig):
    conn = psycopg2.connect(
        host=pg.host,
        port=pg.port,
        dbname=pg.dbname,
        user=pg.user,
        password=pg.password
    )
    return conn


class PostgresWriter:
    def __init__(self, pg: PgConfig):
        self.pg = pg
        self.conn = None

    def connect(self):
        if self.conn is None:
            self.conn = pg_connect(self.pg)
        return self.conn

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

# ---------------------------------------------------------------------------
# TABLE DE BASE :  
# gen_company_codes_api
# gen_clients_api
# gen_sites_api
# ---------------------------------------------------------------------------

def load_and_generate_company_codes(writer: PostgresWriter, extra_n: int = 3) -> list[int]:
    conn = writer.connect()
    ids = []

    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id),0) FROM staging.company_codes")
        max_id = int(cur.fetchone()[0])

        for i in range(extra_n):
            new_id = max_id + i + 1

            cur.execute("""
                INSERT INTO staging.company_codes
                (id, user_id, client_id, name, description)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    client_id = EXCLUDED.client_id,
                    name = EXCLUDED.name,
                    description = EXCLUDED.description;
            """, (
                new_id,
                1,
                1,  # client_id temporaire obligatoire
                f"COMP-{new_id}",
                "Generated company"
            ))

            ids.append(new_id)

    conn.commit()
    print(f"✅ generated {len(ids)} company_codes")
    return ids

def load_and_generate_clients(writer: PostgresWriter, company_code_ids: list[int], extra_n=3) -> list[int]:
    conn = writer.connect()
    ids = []

    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id),0) FROM staging.clients")
        max_id = cur.fetchone()[0]

        for i in range(extra_n):
            new_id = max_id + i + 1

            cur.execute("""
                INSERT INTO staging.clients
                (id, user_id, name, description)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT DO NOTHING;
            """, (
                new_id,
                1,
                f"CLIENT-{new_id}",
                "Generated client"
            ))

            ids.append(new_id)

    conn.commit()
    print(f"✅ generated {len(ids)} clients")
    return ids


def load_and_generate_sites(writer: PostgresWriter, company_code_ids: list[int], extra_n: int = 5) -> list[int]:
    conn = writer.connect()
    site_ids = []

    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.sites;")
        max_id = int(cur.fetchone()[0])

        locs = [
            ("SITE-TN-01", "36.8065,10.1815", "Tunis Plant"),
            ("SITE-TN-02", "36.7370,10.2320", "Ariana Plant"),
            ("SITE-EU-01", "48.8566,2.3522", "Paris Plant"),
        ]

        for i in range(extra_n):
            new_id = max_id + i + 1
            site_number, geo, desc = locs[i % len(locs)]

            cur.execute("""
                INSERT INTO staging.sites
                (id, user_id, company_code_id, site_number, site_external_number,
                 deletion_priority, geo_coordinates, description)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                new_id,
                1,
                company_code_ids[i % len(company_code_ids)],
                f"{site_number}-{new_id}",
                f"EXT-{new_id:04d}",
                "0",
                geo,
                desc
            ))

            site_ids.append(new_id)

    conn.commit()
    print(f"✅ generated {len(site_ids)} sites")
    return site_ids



# ---------------------------------------------------------------------------
# Structure industrielle
# cells → sites
# machine_groups → cells
# stations → machine_groups
# lines → stations
# ---------------------------------------------------------------------------

def load_and_generate_cells(writer: PostgresWriter, site_ids: list[int], extra_n: int = 20) -> list[int]:
    if not site_ids:
        raise RuntimeError("site_ids est vide.")

    conn = writer.connect()
    cells_ids = []

    cell_defs = [
        ("GEN-LAB", "Generated Labelling Cell"),
        ("GEN-SPP", "Generated Solder Paste Printing Cell"),
        ("GEN-SMT", "Generated SMT Cell"),
        ("GEN-AOI", "Generated AOI Cell"),
        ("GEN-TEST", "Generated Test Cell"),
    ]

    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.cells;")
        max_id = int(cur.fetchone()[0])

        for i in range(extra_n):
            new_id = max_id + i + 1
            name, desc = cell_defs[i % len(cell_defs)]

            cur.execute("""
                INSERT INTO staging.cells
                (id, name, description, site_id, user_id, info, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                new_id,
                f"{name}-{new_id}",
                f"{desc} {new_id}",
                site_ids[i % len(site_ids)],
                1,
                "generated",
                True
            ))

            cells_ids.append(new_id)

    conn.commit()
    print(f"✅ generated {len(cells_ids)} cells")
    return cells_ids

def load_cells(writer: PostgresWriter):
    r = requests.get(
        f"{SOURCE_API_URL}/cells/",
        headers=headers,
        timeout=30,
        verify=False
    )

    print("GET CELLS STATUS:", r.status_code)
    print("GET CELLS RESPONSE:", r.text[:300])

    r.raise_for_status()

    data = r.json()
    items = data if isinstance(data, list) else data.get("results", [])

    conn = writer.connect()

    with conn.cursor() as cur:
        for item in items:
            cur.execute("""
                INSERT INTO staging.cells
                (id, name, description, site_id, user_id, info, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    site_id = EXCLUDED.site_id,
                    user_id = EXCLUDED.user_id,
                    info = EXCLUDED.info,
                    is_active = EXCLUDED.is_active;
            """, (
                int(item["id"]),
                item.get("name"),
                item.get("description"),
                int(item.get("site_id") or 1),
                int(item.get("user_id") or 1),
                item.get("info"),
                bool(item.get("is_active", True))
            ))

    conn.commit()
    print(f"✅ {len(items)} cells loaded into staging")

def load_and_generate_machine_groups(writer: PostgresWriter, extra_n: int = 30):
    r = requests.get(
        f"{SOURCE_API_URL}/machine-groups/machine_groups/",
        headers=headers,
        timeout=30,
        verify=False
    )

    print("GET MACHINE GROUPS STATUS:", r.status_code)
    print("GET MACHINE GROUPS RESPONSE:", r.text[:300])

    r.raise_for_status()

    data = r.json()
    items = data if isinstance(data, list) else data.get("results", [])

    conn = writer.connect()

    with conn.cursor() as cur:
        # 1) COPY API mère
        for item in items:
            cur.execute("""
                INSERT INTO staging.machine_groups
                (id, name, description, user_id, cell_id, is_active, failure)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    user_id = EXCLUDED.user_id,
                    cell_id = EXCLUDED.cell_id,
                    is_active = EXCLUDED.is_active,
                    failure = EXCLUDED.failure;
            """, (
                int(item["id"]),
                item.get("name"),
                item.get("description"),
                int(item.get("user_id") or 1),
                item.get("cell_id"),
                bool(item.get("is_active", True)),
                bool(item.get("failure", False))
            ))

        # 2) GENERATE extra rows
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.machine_groups;")
        max_id = int(cur.fetchone()[0])

        cur.execute("SELECT id FROM staging.cells ORDER BY id;")
        cell_ids = [int(row[0]) for row in cur.fetchall()]

        if not cell_ids:
            raise RuntimeError("Aucune cell dans staging.cells → lance load_cells(writer) avant.")

        mg_def = [
            ("GEN-SPI-GROUP", "Generated Solder Paste Inspection"),
            ("GEN-PNP-GROUP", "Generated Pick & Place Machines"),
            ("GEN-OVE-GROUP", "Generated Reflow Ovens"),
            ("GEN-AOI-GROUP", "Generated AOI Machines"),
            ("GEN-ICT-GROUP", "Generated ICT Machines"),
        ]

        generated_ids = []

        for i in range(extra_n):
            new_id = max_id + i + 1
            name, desc = mg_def[i % len(mg_def)]

            cur.execute("""
                INSERT INTO staging.machine_groups
                (id, name, description, user_id, cell_id, is_active, failure)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                new_id,
                f"{name}-{new_id}",
                f"{desc} {new_id}",
                1,
                cell_ids[i % len(cell_ids)],
                True,
                False
            ))

            generated_ids.append(new_id)

    conn.commit()

    print(f"✅ copied {len(items)} machine_groups from source")
    print(f"✅ generated {len(generated_ids)} extra machine_groups into staging")
    return generated_ids

def load_and_generate_stations(writer: PostgresWriter, extra_n: int = 20) -> dict[int, int]:
    r = requests.get(
        f"{SOURCE_API_URL}/stations/stations/",
        headers=headers,
        timeout=30,
        verify=False
    )

    print("GET STATIONS STATUS:", r.status_code)
    print("GET STATIONS RESPONSE:", r.text[:300])

    r.raise_for_status()

    data = r.json()
    items = data if isinstance(data, list) else data.get("results", [])

    conn = writer.connect()
    station_legacy_to_api_id: dict[int, int] = {}

    with conn.cursor() as cur:
        # 1) COPY source API
        for item in items:
            cur.execute("""
                INSERT INTO staging.stations
                (id, machine_group_id, name, description, is_active, user_id, info)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    machine_group_id = EXCLUDED.machine_group_id,
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    is_active = EXCLUDED.is_active,
                    user_id = EXCLUDED.user_id,
                    info = EXCLUDED.info;
            """, (
                int(item["id"]),
                item.get("machine_group_id"),
                item.get("name"),
                item.get("description") or "",
                bool(item.get("is_active", True)),
                int(item.get("user_id") or 1),
                item.get("info")
            ))

        # 2) SEED fixed stations
        for station in STATIONS_FIXED:
            cur.execute("""
                SELECT id
                FROM staging.machine_groups
                WHERE name = %s
                LIMIT 1;
            """, (station["machine_group"],))

            row = cur.fetchone()
            if not row:
                print(f"⚠️ machine group introuvable: {station['machine_group']}")
                continue

            machine_group_id = int(row[0])

            cur.execute("""
                INSERT INTO staging.stations
                (id, machine_group_id, name, description, is_active, user_id, info)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    machine_group_id = EXCLUDED.machine_group_id,
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    is_active = EXCLUDED.is_active,
                    user_id = EXCLUDED.user_id,
                    info = EXCLUDED.info;
            """, (
                int(station["legacy_id"]),
                machine_group_id,
                station["name"],
                station.get("description") or "",
                True,
                5,
                station.get("info")
            ))

            station_legacy_to_api_id[int(station["legacy_id"])] = int(station["legacy_id"])

        # 3) GENERATE extra stations
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.stations;")
        max_id = int(cur.fetchone()[0])

        cur.execute("SELECT id FROM staging.machine_groups ORDER BY id;")
        machine_group_ids = [int(row[0]) for row in cur.fetchall()]

        if not machine_group_ids:
            raise RuntimeError("Aucun machine_group dans staging.machine_groups.")

        for i in range(extra_n):
            new_id = max_id + i + 1

            cur.execute("""
                INSERT INTO staging.stations
                (id, machine_group_id, name, description, is_active, user_id, info)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                new_id,
                machine_group_ids[i % len(machine_group_ids)],
                f"GEN-STATION-{new_id}",
                f"Generated station {new_id}",
                True,
                1,
                "generated"
            ))

    conn.commit()

    print(f"✅ copied {len(items)} stations")
    print(f"✅ seeded {len(station_legacy_to_api_id)} fixed stations")
    print(f"✅ generated {extra_n} extra stations")

    return station_legacy_to_api_id

def load_and_generate_lines(
    writer: PostgresWriter,
    station_legacy_to_api_id: dict[int, int],
    extra_n: int = 5
) -> dict[int, int]:

    r = requests.get(
        f"{SOURCE_API_URL}/lines/lines/",
        headers=headers,
        timeout=30,
        verify=False
    )

    print("GET LINES STATUS:", r.status_code)
    print("GET LINES RESPONSE:", r.text[:300])

    r.raise_for_status()

    data = r.json()
    items = data if isinstance(data, list) else data.get("results", [])

    conn = writer.connect()
    line_legacy_to_api_id: dict[int, int] = {}

    with conn.cursor() as cur:
        # 1) COPY lines from source
        for item in items:
            line_id = int(item["id"])
            station_ids = item.get("station_ids") or []

            cur.execute("""
                INSERT INTO staging.lines
                (id, name, description, date, user_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    date = EXCLUDED.date,
                    user_id = EXCLUDED.user_id;
            """, (
                line_id,
                item.get("name"),
                item.get("description"),
                item.get("date") or datetime.now(),
                int(item.get("user_id") or 1)
            ))

            line_legacy_to_api_id[line_id] = line_id

            for station_id in station_ids:
                cur.execute("""
                    INSERT INTO staging.line_stations
                    (line_id, station_id)
                    VALUES (%s, %s)
                    ON CONFLICT (line_id, station_id) DO NOTHING;
                """, (
                    line_id,
                    int(station_id)
                ))

        # 2) SEED fixed lines
        for ln in LINES_FIXED:
            line_id = int(ln["id"])

            cur.execute("""
                INSERT INTO staging.lines
                (id, name, description, date, user_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    date = EXCLUDED.date,
                    user_id = EXCLUDED.user_id;
            """, (
                line_id,
                ln["name"],
                "",
                datetime.now(),
                8
            ))

            line_legacy_to_api_id[line_id] = line_id

            export_station_ids = [
                sid for lid, sid in LINE_STATION_ASSOCIATIONS_FIXED
                if lid == line_id
            ]

            for export_sid in export_station_ids:
                station_id = station_legacy_to_api_id.get(export_sid)

                if station_id is None:
                    print(f"⚠️ station introuvable pour line {ln['name']}: {export_sid}")
                    continue

                cur.execute("""
                    INSERT INTO staging.line_stations
                    (line_id, station_id)
                    VALUES (%s, %s)
                    ON CONFLICT (line_id, station_id) DO NOTHING;
                """, (
                    line_id,
                    station_id
                ))

        # 3) GENERATE extra lines
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.lines;")
        max_id = int(cur.fetchone()[0])

        cur.execute("SELECT id FROM staging.stations ORDER BY id;")
        station_ids = [int(row[0]) for row in cur.fetchall()]

        if not station_ids:
            raise RuntimeError("Aucune station dans staging.stations.")

        for i in range(extra_n):
            new_id = max_id + i + 1

            cur.execute("""
                INSERT INTO staging.lines
                (id, name, description, date, user_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                new_id,
                f"GEN-LINE-{new_id}",
                f"Generated line {new_id}",
                datetime.now(),
                1
            ))

            # associer 3 stations à chaque ligne générée
            for sid in station_ids[i:i+3]:
                cur.execute("""
                    INSERT INTO staging.line_stations
                    (line_id, station_id)
                    VALUES (%s, %s)
                    ON CONFLICT (line_id, station_id) DO NOTHING;
                """, (
                    new_id,
                    sid
                ))

    conn.commit()

    print(f"✅ copied {len(items)} lines")
    print(f"✅ seeded {len(LINES_FIXED)} fixed lines")
    print(f"✅ generated {extra_n} extra lines")

    return line_legacy_to_api_id

def load_line_station_associations(
    writer: PostgresWriter,
    associations: list[tuple[int, int]]
):
    conn = writer.connect()

    with conn.cursor() as cur:
        for line_id, station_id in associations:
            cur.execute("""
                INSERT INTO staging.line_stations
                (line_id, station_id)
                VALUES (%s, %s)
                ON CONFLICT (line_id, station_id) DO NOTHING;
            """, (line_id, station_id))

    conn.commit()
    print(f"✅ {len(associations)} line_station associations loaded")
    
# ---------------------------------------------------------------------------
# REFERENTIELS 
# part_groups → part_group_types
# part_number_map → part_types + part_groups
# ---------------------------------------------------------------------------

def load_and_generate_part_groups(writer: PostgresWriter, extra_n: int = 10) -> dict[str, int]:
    r = requests.get(
        f"{SOURCE_API_URL}/part-groups/part-groups/",
        headers=headers,
        timeout=30,
        verify=False
    )

    print("GET PART GROUPS STATUS:", r.status_code)
    print("GET PART GROUPS RESPONSE:", r.text[:300])

    r.raise_for_status()

    data = r.json()
    items = data if isinstance(data, list) else data.get("results", [])

    conn = writer.connect()
    part_group_name_to_id: dict[str, int] = {}

    with conn.cursor() as cur:
        # 1) COPY API mère
        for item in items:
            cur.execute("""
                INSERT INTO staging.part_groups
                (id, name, description, user_id, part_type, part_group_type_id,
                 costs, circulating_lot, is_active, state, automatic_emptying,
                 master_workplan, comment, material_transfer, created_on, edited_on)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    user_id = EXCLUDED.user_id,
                    part_type = EXCLUDED.part_type,
                    part_group_type_id = EXCLUDED.part_group_type_id,
                    costs = EXCLUDED.costs,
                    circulating_lot = EXCLUDED.circulating_lot,
                    is_active = EXCLUDED.is_active,
                    state = EXCLUDED.state,
                    automatic_emptying = EXCLUDED.automatic_emptying,
                    master_workplan = EXCLUDED.master_workplan,
                    comment = EXCLUDED.comment,
                    material_transfer = EXCLUDED.material_transfer,
                    created_on = EXCLUDED.created_on,
                    edited_on = EXCLUDED.edited_on;
            """, (
                int(item["id"]),
                item.get("name"),
                item.get("description"),
                int(item.get("user_id") or 1),
                item.get("part_type"),
                item.get("part_group_type_id"),
                item.get("costs") or 0,
                item.get("circulating_lot") or 0,
                bool(item.get("is_active", True)),
                item.get("state") or 1,
                item.get("automatic_emptying") or 0,
                item.get("master_workplan"),
                item.get("comment"),
                bool(item.get("material_transfer", False)),
                item.get("created_on") or datetime.now(),
                item.get("edited_on") or datetime.now(),
            ))

            if item.get("name"):
                part_group_name_to_id[item["name"]] = int(item["id"])

        # 2) GENERATE extra part groups
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.part_groups;")
        max_id = int(cur.fetchone()[0])

        cur.execute("SELECT id FROM staging.part_group_types ORDER BY id;")
        part_group_type_ids = [int(row[0]) for row in cur.fetchall()]

        if not part_group_type_ids:
            raise RuntimeError("Aucun part_group_type dans staging.part_group_types.")

        generated_defs = [
            ("GEN-PCBA-FG", "Generated Finished PCB Assemblies", "EA"),
            ("GEN-PCBA-WIP", "Generated WIP PCB Sub-Assemblies", "EA"),
            ("GEN-SMD-COMP", "Generated SMD Components Pool", "EA"),
            ("GEN-THT-COMP", "Generated THT Components Pool", "EA"),
            ("GEN-RAW-PCB", "Generated Bare PCB Boards", "EA"),
            ("GEN-CONSM", "Generated Consumables", "KG"),
        ]

        for i in range(extra_n):
            new_id = max_id + i + 1
            base_name, desc, part_type = generated_defs[i % len(generated_defs)]
            name = f"{base_name}-{new_id}"

            cur.execute("""
                INSERT INTO staging.part_groups
                (id, name, description, user_id, part_type, part_group_type_id,
                 costs, circulating_lot, is_active, state, automatic_emptying,
                 master_workplan, comment, material_transfer, created_on, edited_on)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                new_id,
                name,
                f"{desc} {new_id}",
                1,
                part_type,
                part_group_type_ids[i % len(part_group_type_ids)],
                random.randint(10, 500),
                random.randint(50, 500),
                True,
                1,
                0,
                None,
                None,
                False,
                datetime.now(),
                datetime.now(),
            ))

            part_group_name_to_id[name] = new_id

    conn.commit()

    print(f"✅ copied {len(items)} part_groups")
    print(f"✅ generated {extra_n} extra part_groups")

    return part_group_name_to_id

def load_and_generate_part_group_types(writer: PostgresWriter, extra_n: int = 6) -> list[int]:
    r = requests.get(
        f"{SOURCE_API_URL}/part-group-types/part-group-types/",
        headers=headers,
        timeout=30,
        verify=False
    )

    print("GET PART GROUP TYPES STATUS:", r.status_code)
    print("GET PART GROUP TYPES RESPONSE:", r.text[:300])

    r.raise_for_status()

    data = r.json()
    items = data if isinstance(data, list) else data.get("results", [])

    conn = writer.connect()
    ids: list[int] = []

    with conn.cursor() as cur:
        # 1) COPY API mère
        for item in items:
            cur.execute("""
                INSERT INTO staging.part_group_types
                (id, name, description)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description;
            """, (
                int(item["id"]),
                item.get("name"),
                item.get("description")
            ))

            ids.append(int(item["id"]))

        # 2) GENERATE extra rows
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.part_group_types;")
        max_id = int(cur.fetchone()[0])

        generated_items = [
            ("GEN-Finished Goods", "Generated shipped customer products"),
            ("GEN-WIP", "Generated work-in-progress assemblies"),
            ("GEN-Components", "Generated purchased electronic components"),
            ("GEN-Raw Material", "Generated PCB substrates and bare boards"),
        ]

        for i in range(extra_n):
            new_id = max_id + i + 1
            name, desc = generated_items[i % len(generated_items)]

            cur.execute("""
                INSERT INTO staging.part_group_types
                (id, name, description)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                new_id,
                f"{name}-{new_id}",
                f"{desc} {new_id}"
            ))

            ids.append(new_id)

    conn.commit()

    print(f"✅ copied {len(items)} part_group_types")
    print(f"✅ generated {extra_n} extra part_group_types")

    return ids

def load_and_generate_part_number_map(
    writer: PostgresWriter,
    part_type_ids: list[int],
    part_group_map: dict[str, int],
    machine_group_ids: list[int],
    site_ids: list[int],
    unit_id: int,
    n_products: int = 8,
):
    if not part_type_ids:
        raise RuntimeError("part_type_ids est vide.")
    if not part_group_map:
        raise RuntimeError("part_group_map est vide.")
    if not site_ids:
        raise RuntimeError("site_ids est vide.")

    conn = writer.connect()
    part_number_to_id = {}

    pcba_fg_id = next((v for k, v in part_group_map.items() if "PCBA-FG" in k), None)
    comp_group_id = next(
        (v for k, v in part_group_map.items() if any(x in k for x in ["SMD-COMP", "THT-COMP", "RAW-PCB", "CONSM"])),
        pcba_fg_id
    )

    if pcba_fg_id is None:
        raise RuntimeError(f"Aucun groupe PCBA-FG trouvé. Clés disponibles: {list(part_group_map.keys())}")

    machine_group_id = machine_group_ids[0] if machine_group_ids else None

    product_names = [
        ("PCB-CTL-001", "Motor Controller"),
        ("PCB-PSU-002", "Power Supply"),
        ("PCB-COM-003", "Communication Board"),
    ]

    component_names = [
        ("SMD-RES-001", "SMD Resistor"),
        ("SMD-CAP-001", "SMD Capacitor"),
        ("THT-CON-001", "THT Connector"),
        ("RAW-PCB-001", "Raw PCB Board"),
        ("COMP-IC-001", "Integrated Circuit"),
    ]

    with conn.cursor() as cur:
        r = requests.get(
            f"{SOURCE_API_URL}/part-masters/part-master/",
            headers=headers,
            timeout=30,
            verify=False
        )

        print("GET PART MASTER STATUS:", r.status_code)
        print("GET PART MASTER RESPONSE:", r.text[:300])

        r.raise_for_status()

        data = r.json()
        items = data if isinstance(data, list) else data.get("results", [])

        for item in items:
            part_id = int(item["id"])
            part_number = item.get("part_number")

            cur.execute("""
                INSERT INTO staging.part_number_map
                (id, part_number, description, part_type_id,
                 part_group_id, machine_group_id, site_id, unit_id,
                 customer_material_number)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    part_number = EXCLUDED.part_number,
                    description = EXCLUDED.description,
                    part_type_id = EXCLUDED.part_type_id,
                    part_group_id = EXCLUDED.part_group_id,
                    machine_group_id = EXCLUDED.machine_group_id,
                    site_id = EXCLUDED.site_id,
                    unit_id = EXCLUDED.unit_id,
                    customer_material_number = EXCLUDED.customer_material_number;
            """, (
                part_id,
                part_number,
                item.get("description"),
                item.get("parttype_id") or item.get("part_type_id") or part_type_ids[0],
                item.get("partgroup_id") or item.get("part_group_id") or pcba_fg_id,
                item.get("machine_group_id") or machine_group_id,
                site_ids[0],
                item.get("unit_id") or unit_id,
                item.get("customer_material_number"),
            ))

            cur.execute("""
                INSERT INTO staging.part_master
                (id, part_number, description, part_status, parttype_id, partgroup_id,
                 case_type, product, panel, variant, machine_group_id, material_info,
                 parts_index, edit_order_based_bom, site_id, unit_id, material_code,
                 no_of_panels, customer_material_number)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    part_number = EXCLUDED.part_number,
                    description = EXCLUDED.description,
                    part_status = EXCLUDED.part_status,
                    parttype_id = EXCLUDED.parttype_id,
                    partgroup_id = EXCLUDED.partgroup_id,
                    machine_group_id = EXCLUDED.machine_group_id,
                    site_id = EXCLUDED.site_id,
                    unit_id = EXCLUDED.unit_id,
                    customer_material_number = EXCLUDED.customer_material_number;
            """, (
                part_id,
                part_number,
                item.get("description"),
                item.get("part_status") or "active",
                item.get("parttype_id") or item.get("part_type_id") or part_type_ids[0],
                item.get("partgroup_id") or item.get("part_group_id") or pcba_fg_id,
                item.get("case_type"),
                bool(item.get("product", False)),
                bool(item.get("panel", False)),
                bool(item.get("variant", False)),
                item.get("machine_group_id") or machine_group_id,
                item.get("material_info"),
                item.get("parts_index"),
                bool(item.get("edit_order_based_bom", False)),
                site_ids[0],
                item.get("unit_id") or unit_id,
                item.get("material_code") or f"MAT-{part_number}",
                item.get("no_of_panels") or 1,
                item.get("customer_material_number"),
            ))

            if part_number:
                part_number_to_id[part_number] = part_id

        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.part_master;")
        max_id = int(cur.fetchone()[0])

        generated_rows = []

        for i in range(n_products):
            new_id = max_id + len(generated_rows) + 1
            pn, desc = product_names[i % len(product_names)]
            generated_rows.append((new_id, f"{pn}-{new_id}", desc, pcba_fg_id, True))

        for pn, desc in component_names:
            new_id = max_id + len(generated_rows) + 1
            generated_rows.append((new_id, f"{pn}-{new_id}", desc, comp_group_id, False))

        for new_id, part_number, desc, group_id, is_product in generated_rows:
            cur.execute("""
                INSERT INTO staging.part_number_map
                (id, part_number, description,
                 part_type_id, part_group_id, machine_group_id,
                 site_id, unit_id, customer_material_number)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    part_number = EXCLUDED.part_number,
                    description = EXCLUDED.description,
                    part_type_id = EXCLUDED.part_type_id,
                    part_group_id = EXCLUDED.part_group_id,
                    machine_group_id = EXCLUDED.machine_group_id,
                    site_id = EXCLUDED.site_id,
                    unit_id = EXCLUDED.unit_id,
                    customer_material_number = EXCLUDED.customer_material_number;
            """, (
                new_id,
                part_number,
                desc,
                part_type_ids[0],
                group_id,
                machine_group_id,
                site_ids[0],
                unit_id,
                None
            ))

            cur.execute("""
                INSERT INTO staging.part_master
                (id, part_number, description, part_status, parttype_id, partgroup_id,
                 case_type, product, panel, variant, machine_group_id, material_info,
                 parts_index, edit_order_based_bom, site_id, unit_id, material_code,
                 no_of_panels, customer_material_number)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    part_number = EXCLUDED.part_number,
                    description = EXCLUDED.description,
                    part_status = EXCLUDED.part_status,
                    parttype_id = EXCLUDED.parttype_id,
                    partgroup_id = EXCLUDED.partgroup_id,
                    product = EXCLUDED.product,
                    machine_group_id = EXCLUDED.machine_group_id,
                    site_id = EXCLUDED.site_id,
                    unit_id = EXCLUDED.unit_id,
                    material_code = EXCLUDED.material_code;
            """, (
                new_id,
                part_number,
                desc,
                "active",
                part_type_ids[0],
                group_id,
                "SMT",
                is_product,
                False,
                False,
                machine_group_id,
                "Generated material",
                new_id,
                False,
                site_ids[0],
                unit_id,
                f"MAT-{part_number}",
                1,
                None
            ))

            part_number_to_id[part_number] = new_id

    conn.commit()

    print(f"✅ copied {len(items)} part_master/part_number_map")
    print(f"✅ generated {len(generated_rows)} part_master/part_number_map")

    return part_number_to_id

def load_and_generate_part_types(writer: PostgresWriter, extra_n: int = 6) -> list[int]:
    r = requests.get(
        f"{SOURCE_API_URL}/part-types/part-types/",
        headers=headers,
        timeout=30,
        verify=False
    )

    print("GET PART TYPES STATUS:", r.status_code)
    print("GET PART TYPES RESPONSE:", r.text[:300])

    r.raise_for_status()

    data = r.json()
    items = data if isinstance(data, list) else data.get("results", [])

    conn = writer.connect()
    ids: list[int] = []

    with conn.cursor() as cur:
        # 1) COPY API mère
        for item in items:
            cur.execute("""
                INSERT INTO staging.part_types
                (id, name, description, user_id, is_active)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    user_id = EXCLUDED.user_id,
                    is_active = EXCLUDED.is_active;
            """, (
                int(item["id"]),
                item.get("name"),
                item.get("description"),
                int(item.get("user_id") or 1),
                bool(item.get("is_active", True))
            ))

            ids.append(int(item["id"]))

        # 2) GENERATE extra rows
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.part_types;")
        max_id = int(cur.fetchone()[0])

        generated_items = [
            ("GEN-PCB Assembly", "Generated finished PCB assembly"),
            ("GEN-Raw PCB", "Generated bare printed circuit board"),
            ("GEN-SMD Component", "Generated surface-mount device"),
            ("GEN-THT Component", "Generated through-hole component"),
            ("GEN-Mechanical", "Generated mechanical part"),
            ("GEN-Consumable", "Generated consumable"),
        ]

        for i in range(extra_n):
            new_id = max_id + i + 1
            name, desc = generated_items[i % len(generated_items)]

            cur.execute("""
                INSERT INTO staging.part_types
                (id, name, description, user_id, is_active)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                new_id,
                f"{name}-{new_id}",
                f"{desc} {new_id}",
                1,
                True
            ))

            ids.append(new_id)

    conn.commit()

    print(f"✅ copied {len(items)} part_types")
    print(f"✅ generated {extra_n} extra part_types")

    return ids


# ---------------------------------------------------------------------------
# QUALITES ET CONDITIONS
# gen_failure_group_types_api / gen_failure_types_api
# gen_machine_condition_groups_api / gen_machine_conditions_ref_api
# ---------------------------------------------------------------------------

def load_and_generate_failure_group_types(
    writer: PostgresWriter,
    extra_n: int = 6
) -> list[int]:

    r = requests.get(
        f"{SOURCE_API_URL}/failure-group-types/failure-group-types/",
        headers=headers,
        timeout=30,
        verify=False
    )

    print("GET FAILURE GROUP TYPES STATUS:", r.status_code)
    print("GET FAILURE GROUP TYPES RESPONSE:", r.text[:300])

    r.raise_for_status()

    data = r.json()
    items = data if isinstance(data, list) else data.get("results", [])

    conn = writer.connect()
    failure_group_types_ids: list[int] = []

    with conn.cursor() as cur:
        # 1) COPY API mère
        for item in items:
            failure_group_id = int(item["id"])
            failure_group_types_ids.append(failure_group_id)

            cur.execute("""
                INSERT INTO staging.failure_group_types
                (id, failure_group_name, failure_group_desc, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    failure_group_name = EXCLUDED.failure_group_name,
                    failure_group_desc = EXCLUDED.failure_group_desc,
                    updated_at = EXCLUDED.updated_at;
            """, (
                failure_group_id,
                item.get("failure_group_name"),
                item.get("failure_group_desc"),
                item.get("created_at") or datetime.now(),
                item.get("updated_at") or datetime.now()
            ))

        # 2) GENERATE extra rows
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.failure_group_types;")
        max_id = int(cur.fetchone()[0])

        generated_items = [
            ("GEN-Solder Defects", "Generated solder defects"),
            ("GEN-Component Defects", "Generated component defects"),
            ("GEN-PCB Defects", "Generated PCB defects"),
            ("GEN-Process Defects", "Generated process defects"),
            ("GEN-Electrical Failures", "Generated electrical failures"),
            ("GEN-Cosmetic Defects", "Generated cosmetic defects"),
        ]

        for i in range(extra_n):
            new_id = max_id + i + 1
            name, desc = generated_items[i % len(generated_items)]

            cur.execute("""
                INSERT INTO staging.failure_group_types
                (id, failure_group_name, failure_group_desc, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                new_id,
                f"{name}-{new_id}",
                f"{desc} {new_id}",
                datetime.now(),
                datetime.now()
            ))

            failure_group_types_ids.append(new_id)

    conn.commit()

    print(f"✅ copied {len(items)} failure_group_types")
    print(f"✅ generated {extra_n} extra failure_group_types")

    return failure_group_types_ids

def load_and_generate_failure_types(
    writer: PostgresWriter,
    failure_group_ids: list[int],
    site_ids: list[int],
    extra_n: int = 10
) -> dict[str, int]:

    if not failure_group_ids:
        raise RuntimeError("failure_group_ids est vide.")
    if not site_ids:
        raise RuntimeError("site_ids est vide.")

    r = requests.get(
        f"{SOURCE_API_URL}/failure-types/failure-types/",
        headers=headers,
        timeout=30,
        verify=False
    )

    print("GET FAILURE TYPES STATUS:", r.status_code)
    print("GET FAILURE TYPES RESPONSE:", r.text[:300])

    r.raise_for_status()

    data = r.json()
    items = data if isinstance(data, list) else data.get("results", [])

    conn = writer.connect()
    failure_type_code_to_id: dict[str, int] = {}

    with conn.cursor() as cur:
        # 1) COPY API mère
        for item in items:
            failure_type_id = int(item["failure_type_id"])

            cur.execute("""
                INSERT INTO staging.failure_types
                (failure_type_id, failure_type_code, failure_type_desc,
                 site_id, failure_group_id, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (failure_type_id) DO UPDATE SET
                    failure_type_code = EXCLUDED.failure_type_code,
                    failure_type_desc = EXCLUDED.failure_type_desc,
                    site_id = EXCLUDED.site_id,
                    failure_group_id = EXCLUDED.failure_group_id,
                    updated_at = EXCLUDED.updated_at;
            """, (
                failure_type_id,
                item.get("failure_type_code"),
                item.get("failure_type_desc"),
                site_ids[0],
                item.get("failure_group_id"),
                item.get("created_at") or datetime.now(),
                item.get("updated_at") or datetime.now()
            ))

            if item.get("failure_type_code"):
                failure_type_code_to_id[item["failure_type_code"]] = failure_type_id

        # 2) GENERATE extra rows
        cur.execute("SELECT COALESCE(MAX(failure_type_id), 0) FROM staging.failure_types;")
        max_id = int(cur.fetchone()[0])

        generated_failures = [
            ("GEN-FT-SB", "Generated Solder Bridge"),
            ("GEN-FT-IS", "Generated Insufficient Solder"),
            ("GEN-FT-MC", "Generated Missing Component"),
            ("GEN-FT-WC", "Generated Wrong Component"),
            ("GEN-FT-SC", "Generated Short Circuit"),
            ("GEN-FT-OC", "Generated Open Circuit"),
        ]

        for i in range(extra_n):
            new_id = max_id + i + 1
            code, desc = generated_failures[i % len(generated_failures)]
            failure_code = f"{code}-{new_id}"

            cur.execute("""
                INSERT INTO staging.failure_types
                (failure_type_id, failure_type_code, failure_type_desc,
                 site_id, failure_group_id, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (failure_type_id) DO NOTHING;
            """, (
                new_id,
                failure_code,
                f"{desc} {new_id}",
                site_ids[i % len(site_ids)],
                failure_group_ids[i % len(failure_group_ids)],
                datetime.now(),
                datetime.now()
            ))

            failure_type_code_to_id[failure_code] = new_id

    conn.commit()

    print(f"✅ copied {len(items)} failure_types")
    print(f"✅ generated {extra_n} extra failure_types")

    return failure_type_code_to_id

def load_and_generate_machine_condition_groups(
    writer: PostgresWriter,
    extra_n: int = 3
) -> list[int]:

    r = requests.get(
        f"{SOURCE_API_URL}/machine-condition-groups/machine-condition-groups/",
        headers=headers,
        timeout=30,
        verify=False
    )

    print("GET MACHINE CONDITION GROUPS STATUS:", r.status_code)
    print("GET MACHINE CONDITION GROUPS RESPONSE:", r.text[:300])

    r.raise_for_status()

    data = r.json()
    items = data if isinstance(data, list) else data.get("results", [])

    conn = writer.connect()
    machine_condition_groups_ids: list[int] = []

    with conn.cursor() as cur:
        for item in items:
            group_id = int(item["id"])
            machine_condition_groups_ids.append(group_id)

            cur.execute("""
                INSERT INTO staging.machine_condition_groups
                (id, group_name, group_description, is_active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    group_name = EXCLUDED.group_name,
                    group_description = EXCLUDED.group_description,
                    is_active = EXCLUDED.is_active,
                    updated_at = EXCLUDED.updated_at;
            """, (
                group_id,
                item.get("group_name"),
                item.get("group_description"),
                bool(item.get("is_active", True)),
                item.get("created_at") or datetime.now(),
                item.get("updated_at") or datetime.now()
            ))

        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.machine_condition_groups;")
        max_id = int(cur.fetchone()[0])

        generated_items = [
            ("GEN-Unplanned Downtime", "Generated breakdowns and stoppages"),
            ("GEN-Planned Downtime", "Generated setup and maintenance"),
            ("GEN-Operational", "Generated running and breaks"),
        ]

        for i in range(extra_n):
            new_id = max_id + i + 1
            name, desc = generated_items[i % len(generated_items)]

            cur.execute("""
                INSERT INTO staging.machine_condition_groups
                (id, group_name, group_description, is_active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                new_id,
                f"{name}-{new_id}",
                f"{desc} {new_id}",
                True,
                datetime.now(),
                datetime.now()
            ))

            machine_condition_groups_ids.append(new_id)

    conn.commit()

    print(f"✅ copied {len(items)} machine_condition_groups")
    print(f"✅ generated {extra_n} extra machine_condition_groups")

    return machine_condition_groups_ids

def load_and_generate_machine_conditions(
    writer: PostgresWriter,
    machine_condition_group_ids: list[int],
    extra_n: int = 6
) -> dict[str, int]:

    if not machine_condition_group_ids:
        raise RuntimeError("machine_condition_group_ids est vide.")

    r = requests.get(
        f"{SOURCE_API_URL}/machine-conditions/machine-conditions/",
        headers=headers,
        timeout=30,
        verify=False
    )

    print("GET MACHINE CONDITIONS STATUS:", r.status_code)
    print("GET MACHINE CONDITIONS RESPONSE:", r.text[:300])

    r.raise_for_status()

    data = r.json()
    items = data if isinstance(data, list) else data.get("results", [])

    conn = writer.connect()
    condition_code_to_id: dict[str, int] = {}

    with conn.cursor() as cur:
        for item in items:
            condition_id = int(item["id"])

            cur.execute("""
                INSERT INTO staging.machine_conditions
                (id, group_id, condition_name, condition_description,
                 color_rgb, is_active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    group_id = EXCLUDED.group_id,
                    condition_name = EXCLUDED.condition_name,
                    condition_description = EXCLUDED.condition_description,
                    color_rgb = EXCLUDED.color_rgb,
                    is_active = EXCLUDED.is_active,
                    updated_at = EXCLUDED.updated_at;
            """, (
                condition_id,
                item.get("group_id"),
                item.get("condition_name"),
                item.get("condition_description"),
                item.get("color_rgb"),
                bool(item.get("is_active", True)),
                item.get("created_at") or datetime.now(),
                item.get("updated_at") or datetime.now()
            ))

            if item.get("condition_name"):
                condition_code_to_id[item["condition_name"]] = condition_id

        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.machine_conditions;")
        max_id = int(cur.fetchone()[0])

        generated_conditions = [
            ("GEN-1000", "Generated Minor Stoppages", "#d6a624"),
            ("GEN-1001", "Generated Cleaning", "#099f95"),
            ("GEN-2000", "Generated Change Over", "#4940c9"),
            ("GEN-2002", "Generated Machine Breakdown", "#8b1818"),
            ("GEN-3000", "Generated Preventive Maintenance", "#e6d628"),
            ("GEN-3006", "Generated Running", "#13be1e"),
        ]

        for i in range(extra_n):
            new_id = max_id + i + 1
            code, desc, color = generated_conditions[i % len(generated_conditions)]

            condition_name = f"{code}-{new_id}"

            cur.execute("""
                INSERT INTO staging.machine_conditions
                (id, group_id, condition_name, condition_description,
                 color_rgb, is_active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                new_id,
                machine_condition_group_ids[i % len(machine_condition_group_ids)],
                condition_name,
                f"{desc} {new_id}",
                color,
                True,
                datetime.now(),
                datetime.now()
            ))

            condition_code_to_id[condition_name] = new_id

    conn.commit()

    print(f"✅ copied {len(items)} machine_conditions")
    print(f"✅ generated {extra_n} extra machine_conditions")

    return condition_code_to_id
# ---------------------------------------------------------------------------
# ERP / process
# gen_erp_groups_api
# gen_assign_stations_to_erpgrp_api
# ---------------------------------------------------------------------------

def load_and_generate_erp_groups(
    writer: PostgresWriter,
    extra_n: int = 5
) -> dict[str, int]:

    r = requests.get(
        f"{SOURCE_API_URL}/erp-groups/erp-groups/",
        headers=headers,
        timeout=30,
        verify=False
    )

    print("GET ERP GROUPS STATUS:", r.status_code)
    print("GET ERP GROUPS RESPONSE:", r.text[:300])

    r.raise_for_status()

    data = r.json()
    items = data if isinstance(data, list) else data.get("results", [])

    conn = writer.connect()
    erp_group_code_to_id: dict[str, int] = {}

    with conn.cursor() as cur:
        for item in items:
            erp_id = int(item["id"])

            cur.execute("""
                INSERT INTO staging.erp_groups
                (id, state, erpgroup_no, erp_group_description, erpsystem,
                 sequential, separate_station, fixed_layer, created_on, edited_on,
                 modified_by, user_id, cst_id, valid)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    state = EXCLUDED.state,
                    erpgroup_no = EXCLUDED.erpgroup_no,
                    erp_group_description = EXCLUDED.erp_group_description,
                    erpsystem = EXCLUDED.erpsystem,
                    sequential = EXCLUDED.sequential,
                    separate_station = EXCLUDED.separate_station,
                    fixed_layer = EXCLUDED.fixed_layer,
                    edited_on = EXCLUDED.edited_on,
                    modified_by = EXCLUDED.modified_by,
                    user_id = EXCLUDED.user_id,
                    cst_id = EXCLUDED.cst_id,
                    valid = EXCLUDED.valid;
            """, (
                erp_id,
                item.get("state"),
                item.get("erpgroup_no"),
                item.get("erp_group_description"),
                item.get("erpsystem"),
                bool(item.get("sequential", True)),
                bool(item.get("separate_station", False)),
                bool(item.get("fixed_layer", False)),
                item.get("created_on") or datetime.now(),
                item.get("edited_on") or datetime.now(),
                item.get("modified_by"),
                item.get("user_id") or 1,
                item.get("cst_id"),
                bool(item.get("valid", True))
            ))

            if item.get("erpgroup_no"):
                erp_group_code_to_id[item["erpgroup_no"]] = erp_id

        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.erp_groups;")
        max_id = int(cur.fetchone()[0])

        for i in range(extra_n):
            new_id = max_id + i + 1
            code, desc, _ = PCB_PROCESS_STEPS[i % len(PCB_PROCESS_STEPS)]
            erpgroup_no = f"GEN-ERP-{code}-{new_id}"

            cur.execute("""
                INSERT INTO staging.erp_groups
                (id, state, erpgroup_no, erp_group_description, erpsystem,
                 sequential, separate_station, fixed_layer, created_on, edited_on,
                 modified_by, user_id, cst_id, valid)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                new_id,
                1,
                erpgroup_no,
                f"Generated {desc}",
                "SAP",
                True,
                False,
                False,
                datetime.now(),
                datetime.now(),
                1,
                1,
                None,
                True
            ))

            erp_group_code_to_id[erpgroup_no] = new_id

    conn.commit()

    print(f"✅ copied {len(items)} erp_groups")
    print(f"✅ generated {extra_n} extra erp_groups")

    return erp_group_code_to_id

def load_and_generate_assign_stations_to_erpgrp(
    writer: PostgresWriter,
    extra_n: int = 100
) -> list[tuple[int, int]]:

    r = requests.get(
        f"{SOURCE_API_URL}/assign-stations/assign-stations-to-erpgrp/",
        headers=headers,
        timeout=30,
        verify=False
    )

    print("GET ASSIGN STATIONS STATUS:", r.status_code)
    print("GET ASSIGN STATIONS RESPONSE:", r.text[:300])

    r.raise_for_status()

    data = r.json()
    items = data if isinstance(data, list) else data.get("results", [])

    conn = writer.connect()
    pairs: list[tuple[int, int]] = []

    with conn.cursor() as cur:
        for item in items:
            station_id = int(item["station_id"])
            erp_group_id = int(item["erp_group_id"])

            cur.execute("""
                INSERT INTO staging.assign_stations_to_erpgrp
                (station_id, erp_group_id, station_type, user_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (station_id, erp_group_id) DO UPDATE SET
                    station_type = EXCLUDED.station_type,
                    user_id = EXCLUDED.user_id;
            """, (
                station_id,
                erp_group_id,
                item.get("station_type") or "production",
                item.get("user_id") or 1
            ))

            pairs.append((station_id, erp_group_id))

        cur.execute("SELECT id FROM staging.stations ORDER BY id;")
        station_ids = [int(row[0]) for row in cur.fetchall()]

        cur.execute("SELECT id FROM staging.erp_groups ORDER BY id;")
        erp_group_ids = [int(row[0]) for row in cur.fetchall()]

        if not station_ids:
            raise RuntimeError("Aucune station dans staging.stations.")
        if not erp_group_ids:
            raise RuntimeError("Aucun erp_group dans staging.erp_groups.")

        n = min(extra_n, len(station_ids))

        for i in range(n):
            station_id = station_ids[i]
            erp_group_id = erp_group_ids[i % len(erp_group_ids)]

            cur.execute("""
                INSERT INTO staging.assign_stations_to_erpgrp
                (station_id, erp_group_id, station_type, user_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (station_id, erp_group_id) DO NOTHING;
            """, (
                station_id,
                erp_group_id,
                "production",
                1
            ))

            pairs.append((station_id, erp_group_id))

    conn.commit()

    print(f"✅ copied {len(items)} station/erp assignments")
    print(f"✅ generated {n} extra station/erp assignments")

    return pairs

# ---------------------------------------------------------------------------
# Production (CORE métier)
# gen_work_plans_api
# gen_work_steps_api
# gen_bom_headers_api
# gen_bom_items_api
# gen_bom_insertion_api
# ---------------------------------------------------------------------------

def load_and_generate_workplans(
    writer: PostgresWriter,
    site_ids: list[int],
    client_ids: list[int],
    company_code_ids: list[int],
    product_part_numbers: list[str],
    extra_n: int = 10
) -> list[dict[str, Any]]:

    if not site_ids:
        raise RuntimeError("site_ids est vide.")
    if not client_ids:
        raise RuntimeError("client_ids est vide.")
    if not company_code_ids:
        raise RuntimeError("company_code_ids est vide.")
    if not product_part_numbers:
        raise RuntimeError("product_part_numbers est vide.")

    r = requests.get(
        f"{SOURCE_API_URL}/workplans/workplan/",
        headers=headers,
        timeout=30,
        verify=False
    )

    print("GET WORKPLANS STATUS:", r.status_code)
    print("GET WORKPLANS RESPONSE:", r.text[:300])

    r.raise_for_status()

    data = r.json()
    items = data if isinstance(data, list) else data.get("results", [])

    conn = writer.connect()
    workplans: list[dict[str, Any]] = []

    with conn.cursor() as cur:
        # 1) COPY API mère
        for item in items:
            wp_id = int(item["id"])

            cur.execute("""
                INSERT INTO staging.workplans
                (id, version, is_current, user_id, site_id, client_id, company_id,
                 source, status, product_vers_id, workplan_status, part_no,
                 part_desc, workplan_desc, workplan_type, workplan_version_erp,
                 created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    version = EXCLUDED.version,
                    is_current = EXCLUDED.is_current,
                    user_id = EXCLUDED.user_id,
                    site_id = EXCLUDED.site_id,
                    client_id = EXCLUDED.client_id,
                    company_id = EXCLUDED.company_id,
                    source = EXCLUDED.source,
                    status = EXCLUDED.status,
                    product_vers_id = EXCLUDED.product_vers_id,
                    workplan_status = EXCLUDED.workplan_status,
                    part_no = EXCLUDED.part_no,
                    part_desc = EXCLUDED.part_desc,
                    workplan_desc = EXCLUDED.workplan_desc,
                    workplan_type = EXCLUDED.workplan_type,
                    workplan_version_erp = EXCLUDED.workplan_version_erp;
            """, (
                wp_id,
                item.get("version"),
                bool(item.get("is_current", True)),
                item.get("user_id") or 1,
                site_ids[0],
                item.get("client_id"),
                item.get("company_id"),
                item.get("source"),
                item.get("status"),
                item.get("product_vers_id"),
                item.get("workplan_status"),
                item.get("part_no"),
                item.get("part_desc"),
                item.get("workplan_desc"),
                item.get("workplan_type"),
                item.get("workplan_version_erp"),
                item.get("created_at") or datetime.now()
            ))

            workplans.append({
                "id": wp_id,
                "part_no": item.get("part_no"),
                "url": f"{SOURCE_API_URL}/workplans/workplan/{wp_id}"
            })

        # 2) GENERATE extra workplans
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.workplans;")
        max_id = int(cur.fetchone()[0])

        for i in range(extra_n):
            new_id = max_id + i + 1
            part_no = product_part_numbers[i % len(product_part_numbers)]

            cur.execute("""
                INSERT INTO staging.workplans
                (id, version, is_current, user_id, site_id, client_id, company_id,
                 source, status, product_vers_id, workplan_status, part_no,
                 part_desc, workplan_desc, workplan_type, workplan_version_erp,
                 created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                new_id,
                1,
                True,
                1,
                site_ids[i % len(site_ids)],
                client_ids[i % len(client_ids)],
                company_code_ids[i % len(company_code_ids)],
                1,
                1,
                i + 1,
                "R",
                part_no,
                f"Work plan for {part_no}",
                f"Generated SMT/THT production plan - {part_no}",
                random.choice(["SMT", "MIX", "THT"]),
                f"GEN-WP-{new_id}-V1",
                datetime.now()
            ))

            workplans.append({
                "id": new_id,
                "part_no": part_no,
                "url": f"staging.workplans/{new_id}"
            })

    conn.commit()

    print(f"✅ copied {len(items)} workplans")
    print(f"✅ generated {extra_n} extra workplans")

    return workplans

def load_and_generate_worksteps(
    writer: PostgresWriter,
    workplan_ids: list[int],
    erp_group_ids: list[int],
    extra_n_per_workplan: int = 5
) -> list[dict[str, Any]]:

    if not workplan_ids:
        raise RuntimeError("workplan_ids est vide.")
    if not erp_group_ids:
        raise RuntimeError("erp_group_ids est vide.")

    r = requests.get(
        f"{SOURCE_API_URL}/worksteps/worksteps/",
        headers=headers,
        timeout=30,
        verify=False
    )

    print("GET WORKSTEPS STATUS:", r.status_code)
    print("GET WORKSTEPS RESPONSE:", r.text[:300])

    r.raise_for_status()

    data = r.json()
    items = data if isinstance(data, list) else data.get("results", [])

    conn = writer.connect()
    created_steps: list[dict[str, Any]] = []

    with conn.cursor() as cur:
        # 1) COPY API mère
        for item in items:
            step_id = int(item["id"])

            cur.execute("""
                INSERT INTO staging.worksteps
                (id, workplan_id, erp_group_id, workstep_no, step,
                 setup_time, te_person, te_machine, te_time_base, te_qty_base,
                 transport_time, wait_time, status, panel_count, workstep_desc,
                 erp_grp_no, erp_grp_desc, time_unit, setup_flag,
                 workstep_version_erp, info, confirmation, sequentiell,
                 workstep_type, traceflag, step_type, created_at, stamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    workplan_id = EXCLUDED.workplan_id,
                    erp_group_id = EXCLUDED.erp_group_id,
                    workstep_no = EXCLUDED.workstep_no,
                    step = EXCLUDED.step,
                    setup_time = EXCLUDED.setup_time,
                    te_person = EXCLUDED.te_person,
                    te_machine = EXCLUDED.te_machine,
                    te_time_base = EXCLUDED.te_time_base,
                    te_qty_base = EXCLUDED.te_qty_base,
                    transport_time = EXCLUDED.transport_time,
                    wait_time = EXCLUDED.wait_time,
                    status = EXCLUDED.status,
                    panel_count = EXCLUDED.panel_count,
                    workstep_desc = EXCLUDED.workstep_desc,
                    erp_grp_no = EXCLUDED.erp_grp_no,
                    erp_grp_desc = EXCLUDED.erp_grp_desc,
                    time_unit = EXCLUDED.time_unit,
                    setup_flag = EXCLUDED.setup_flag,
                    workstep_version_erp = EXCLUDED.workstep_version_erp,
                    info = EXCLUDED.info,
                    confirmation = EXCLUDED.confirmation,
                    sequentiell = EXCLUDED.sequentiell,
                    workstep_type = EXCLUDED.workstep_type,
                    traceflag = EXCLUDED.traceflag,
                    step_type = EXCLUDED.step_type,
                    stamp = EXCLUDED.stamp;
            """, (
                step_id,
                item.get("workplan_id"),
                item.get("erp_group_id"),
                item.get("workstep_no"),
                item.get("step"),
                item.get("setup_time"),
                item.get("te_person"),
                item.get("te_machine"),
                item.get("te_time_base"),
                item.get("te_qty_base"),
                item.get("transport_time"),
                item.get("wait_time"),
                item.get("status"),
                item.get("panel_count"),
                item.get("workstep_desc"),
                item.get("erp_grp_no"),
                item.get("erp_grp_desc"),
                item.get("time_unit"),
                item.get("setup_flag"),
                item.get("workstep_version_erp"),
                item.get("info"),
                item.get("confirmation"),
                item.get("sequentiell"),
                item.get("workstep_type"),
                item.get("traceflag"),
                item.get("step_type"),
                item.get("created_at") or datetime.now(),
                item.get("stamp") or datetime.now()
            ))

            created_steps.append({
                "id": step_id,
                "workplan_id": item.get("workplan_id"),
                "url": f"staging.worksteps/{step_id}"
            })

        # 2) GENERATE worksteps
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.worksteps;")
        max_id = int(cur.fetchone()[0])
        new_id = max_id + 1

        for wp_id in workplan_ids:
            for step_i, (code, desc, step_type) in enumerate(PCB_PROCESS_STEPS[:extra_n_per_workplan]):
                eid = erp_group_ids[step_i % len(erp_group_ids)]

                cur.execute("""
                    INSERT INTO staging.worksteps
                    (id, workplan_id, erp_group_id, workstep_no, step,
                     setup_time, te_person, te_machine, te_time_base, te_qty_base,
                     transport_time, wait_time, status, panel_count, workstep_desc,
                     erp_grp_no, erp_grp_desc, time_unit, setup_flag,
                     workstep_version_erp, info, confirmation, sequentiell,
                     workstep_type, traceflag, step_type, created_at, stamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                """, (
                    new_id,
                    wp_id,
                    eid,
                    (step_i + 1) * 10,
                    step_i + 1,
                    round(random.uniform(15, 60), 2),
                    1,
                    round(AVG_CYCLE_TIME_SEC / 60, 2),
                    60,
                    1,
                    round(random.uniform(1, 10), 2),
                    round(random.uniform(0, 30), 2),
                    1,
                    random.choice([1, 2, 4]),
                    desc,
                    f"ERP-{code}",
                    desc,
                    "MIN",
                    "X" if step_i == 0 else "",
                    f"GEN-WS-{wp_id:04d}-{step_i+1:02d}",
                    step_type,
                    "AUTO",
                    "X",
                    step_type,
                    "X",
                    random.choice(["manuel", "auto", "semiAuto"]),
                    datetime.now(),
                    datetime.now()
                ))

                created_steps.append({
                    "id": new_id,
                    "workplan_id": wp_id,
                    "url": f"staging.worksteps/{new_id}"
                })

                new_id += 1

    conn.commit()

    print(f"✅ copied {len(items)} worksteps")
    print(f"✅ generated {len(created_steps) - len(items)} extra worksteps")

    return created_steps

def load_and_generate_bom_headers(
    writer: PostgresWriter,
    part_number_to_id: dict[str, int],
    extra_n: int = 10
) -> list[dict]:

    r = requests.get(
        f"{SOURCE_API_URL}/bom-headers/bom/headers/",
        headers=headers,
        timeout=30,
        verify=False
    )

    r.raise_for_status()

    data = r.json()
    items = data if isinstance(data, list) else data.get("results", [])

    conn = writer.connect()
    rows = []

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM staging.part_master ORDER BY id;")
        valid_part_master_ids = [int(row[0]) for row in cur.fetchall()]

        if not valid_part_master_ids:
            raise RuntimeError("Aucun id trouvé dans staging.part_master.")

        valid_part_master_set = set(valid_part_master_ids)

        for item in items:
            part_master_id = item.get("part_master_id")

            if part_master_id is not None:
                part_master_id = int(part_master_id)

            if part_master_id not in valid_part_master_set:
                print(f"⚠️ BOM header ignoré: part_master_id={part_master_id} introuvable")
                continue

            cur.execute("""
                INSERT INTO staging.bom_headers
                (id, part_master_id, description, valid_from, valid_to, state, version, is_current)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    part_master_id = EXCLUDED.part_master_id,
                    description = EXCLUDED.description,
                    valid_from = EXCLUDED.valid_from,
                    valid_to = EXCLUDED.valid_to,
                    state = EXCLUDED.state,
                    version = EXCLUDED.version,
                    is_current = EXCLUDED.is_current;
            """, (
                int(item["id"]),
                part_master_id,
                item.get("description"),
                item.get("valid_from"),
                item.get("valid_to"),
                item.get("state"),
                item.get("version"),
                item.get("is_current"),
            ))

            rows.append(item)

        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.bom_headers;")
        max_id = int(cur.fetchone()[0])

        for i in range(extra_n):
            new_id = max_id + i + 1
            pm_id = valid_part_master_ids[i % len(valid_part_master_ids)]

            cur.execute("""
                INSERT INTO staging.bom_headers
                (id, part_master_id, description, valid_from, state, version, is_current)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    part_master_id = EXCLUDED.part_master_id,
                    description = EXCLUDED.description,
                    valid_from = EXCLUDED.valid_from,
                    state = EXCLUDED.state,
                    version = EXCLUDED.version,
                    is_current = EXCLUDED.is_current;
            """, (
                new_id,
                pm_id,
                f"GEN BOM {pm_id}",
                datetime.now(),
                "released",
                1,
                True
            ))

            rows.append({"id": new_id, "part_master_id": pm_id})

    conn.commit()
    return rows

def load_and_generate_bom_items(
    writer: PostgresWriter,
    bom_header_ids: list[int],
    component_ids: list[int],
    extra_per_header: int = 10
):
    conn = writer.connect()

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM staging.part_master ORDER BY id;")
        valid_part_master_ids = [int(row[0]) for row in cur.fetchall()]

        if not valid_part_master_ids:
            raise RuntimeError("Aucun composant trouvé dans staging.part_master.")

        cur.execute("SELECT id FROM staging.bom_headers ORDER BY id;")
        valid_bom_header_ids = [int(row[0]) for row in cur.fetchall()]

        bom_header_ids = [bh for bh in bom_header_ids if bh in valid_bom_header_ids]

        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.bom_items;")
        max_id = int(cur.fetchone()[0])
        new_id = max_id + 1

        for bh_id in bom_header_ids:
            for i in range(extra_per_header):
                cur.execute("""
                    INSERT INTO staging.bom_items
                    (id, bom_header_id, part_master_id, quantity, is_product, component_name, layer)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (
                    new_id,
                    bh_id,
                    random.choice(valid_part_master_ids),
                    random.randint(1, 10),
                    False,
                    f"Component-{new_id}",
                    random.choice([1, 2])
                ))

                new_id += 1

    conn.commit()
    print("✅ bom_items generated")

# ---------------------------------------------------------------------------
# Execution 
#gen_work_orders_api
#gen_serial_numbers_api
#gen_active_workorders_api
#gen_bookings_api
#gen_measurement_data_api
#gen_machine_condition_data_api
# ---------------------------------------------------------------------------

def load_and_generate_work_orders(
    writer: PostgresWriter,
    client_ids: list[int],
    company_code_ids: list[int],
    site_ids: list[int],
    product_part_numbers: list[str],
    product_part_ids: list[int],
    workplan_by_part_no: dict[str, dict],
    window_start: datetime,
    window_end: datetime,
    n: int,
    wo_qty_min: int,
    wo_qty_max: int,
) -> tuple[list[int], dict]:

    conn = writer.connect()
    created_ids: list[int] = []
    windows: dict = {}

    with conn.cursor() as cur:

        # 🔹 1) COPY API mère
        r = requests.get(
            f"{SOURCE_API_URL}/workorders/workorders/",
            headers=headers,
            timeout=30,
            verify=False
        )

        r.raise_for_status()  # ✅ important

        data = r.json()
        items = data if isinstance(data, list) else data.get("results", [])

        for item in items:
            cur.execute("""
                INSERT INTO staging.work_orders
                (id, workorder_no, workorder_type, part_number, workorder_qty,
                 startdate, deliverydate, unit, bom_version, workplan_type,
                 backflush, source, workplan_version, workorder_desc,
                 workplan_valid_from, status, site_id, client_id, company_id,
                 workorder_state, aps_planning_start_date, aps_planning_stamp,
                 aps_planning_end_date, aps_order_fixation)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    workorder_no = EXCLUDED.workorder_no,
                    workorder_type = EXCLUDED.workorder_type,
                    part_number = EXCLUDED.part_number,
                    workorder_qty = EXCLUDED.workorder_qty;
            """, (
                int(item["id"]),
                item.get("workorder_no") or f"WO-{item['id']}",
                item.get("workorder_type") or "P",
                item.get("part_number") or "UNKNOWN",
                item.get("workorder_qty") or 1,
                item.get("startdate"),
                item.get("deliverydate"),
                item.get("unit") or "EA",
                int(item.get("bom_version") or 1),
                item.get("workplan_type") or "SMT",
                int(item.get("backflush") or 0),
                int(item.get("source") or 1),
                int(item.get("workplan_version") or 1),
                item.get("workorder_desc") or f"Work order {item['id']}",
                item.get("workplan_valid_from"),
                item.get("status") or "R",
                site_ids[0],
                item.get("client_id") or client_ids[0],
                item.get("company_id") or company_code_ids[0],
                item.get("workorder_state") or "active",
                item.get("aps_planning_start_date"),
                item.get("aps_planning_stamp"),
                item.get("aps_planning_end_date"),
                item.get("aps_order_fixation") or 0
            ))

            created_ids.append(int(item["id"]))  # ✅ fix

        # 🔹 2) GENERATE
        cur.execute("SELECT COALESCE(MAX(id),0) FROM staging.work_orders")
        max_id = int(cur.fetchone()[0])

        for i in range(n):
            new_id = max_id + i + 1

            pn_idx = random.randrange(len(product_part_numbers))
            pn = product_part_numbers[pn_idx]
            pid = product_part_ids[pn_idx]  # ✅ utilisé maintenant

            qty = random.randint(wo_qty_min, wo_qty_max)

            start_dt = window_start + timedelta(hours=random.randint(1, 100))
            delivery_dt = start_dt + timedelta(hours=random.randint(1, 48))

            cur.execute("""
                INSERT INTO staging.work_orders
                (id, workorder_no, workorder_type, part_number, workorder_qty,
                 startdate, deliverydate, unit, bom_version, workplan_type,
                 backflush, source, workplan_version, workorder_desc,
                 workplan_valid_from, status, site_id, client_id, company_id,
                 workorder_state, aps_planning_start_date, aps_planning_stamp,
                 aps_planning_end_date, aps_order_fixation)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                new_id,
                f"GEN-WO-{new_id}",
                "P",
                pn,
                qty,
                start_dt,
                delivery_dt,
                "EA",
                1,
                "SMT",
                0,
                1,
                1,
                f"{pn} generated production batch",
                window_start,
                "R",
                site_ids[i % len(site_ids)],
                client_ids[i % len(client_ids)],
                company_code_ids[i % len(company_code_ids)],
                "active",
                start_dt,
                delivery_dt,
                delivery_dt,
                0
            ))

            created_ids.append(new_id)

            windows[new_id] = {
                "part_number": pn,
                "part_id": pid, 
                "company_id": company_code_ids[i % len(company_code_ids)],
                "qty": qty,
                "start": start_dt,
                "end": delivery_dt,
            }

    conn.commit()

    print(f"✅ copied {len(items)} work_orders")
    print(f"✅ generated {n} work_orders")

    return created_ids, windows

def load_and_generate_serial_numbers(
    writer: PostgresWriter,
    windows: dict,
    max_per_workorder: int = 50
) -> dict[int, list[int]]:

    conn = writer.connect()
    wo_snr_map: dict[int, list[int]] = {}

    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.serial_numbers;")
        max_id = int(cur.fetchone()[0])
        new_id = max_id + 1

        cur.execute("SELECT id FROM staging.part_master ORDER BY id;")
        valid_part_ids = [int(row[0]) for row in cur.fetchall()]

        if not valid_part_ids:
            raise RuntimeError("Aucun id trouvé dans staging.part_master.")

        cur.execute("SELECT id FROM staging.company_codes ORDER BY id;")
        valid_company_ids = [int(row[0]) for row in cur.fetchall()]

        if not valid_company_ids:
            raise RuntimeError("Aucun id trouvé dans staging.company_codes.")

        for wid, w in windows.items():
            qty = int(w.get("qty", 1))
            target = min(max(1, qty), max_per_workorder)

            start_dt = w.get("start") or datetime.now()
            snr_ids: list[int] = []

            for pos in range(1, target + 1):
                part_id = valid_part_ids[pos % len(valid_part_ids)]
                company_code_id = valid_company_ids[pos % len(valid_company_ids)]
                created_on = start_dt + timedelta(seconds=pos * 240)

                cur.execute("""
                    INSERT INTO staging.serial_numbers
                    (id, serial_number, serial_number_pos, serial_number_ref_pos,
                     serial_number_active, serial_number_ref, splitted,
                     workorder_id, part_id, customer_part_number,
                     workorder_type, serial_number_type, cluster_name,
                     cluster_type, created_on, created_by, company_code_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                """, (
                    new_id,
                    f"SNR-WO-{wid:06d}-{pos:05d}",
                    pos,
                    pos,
                    "Y",
                    f"SNR-WO-{wid:06d}-{pos:05d}",
                    False,
                    wid,
                    part_id,
                    f"CUST-{part_id}",
                    "S",
                    "S",
                    "ASSEMBLY",
                    "O",
                    created_on,
                    1,
                    company_code_id,
                ))

                snr_ids.append(new_id)
                new_id += 1

            wo_snr_map[wid] = snr_ids

    conn.commit()
    print(f"✅ generated {sum(len(v) for v in wo_snr_map.values())} serial_numbers")

    return wo_snr_map

def load_and_generate_active_workorders(
    writer: PostgresWriter,
    windows: dict,
    station_ids: list[int],
    window_end: datetime,
    n: int = 20
) -> list[dict[str, Any]]:

    if not windows:
        print("  - active_workorders: no work_orders")
        return []

    if not station_ids:
        raise RuntimeError("station_ids est vide.")

    conn = writer.connect()
    created: list[dict[str, Any]] = []

    active_wids = list(windows.keys())
    n = min(n, len(active_wids))

    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.active_workorders;")
        max_id = int(cur.fetchone()[0])
        new_id = max_id + 1

        for i in range(n):
            wid = active_wids[i % len(active_wids)]

            created_at = window_end - timedelta(hours=random.uniform(0.5, 8))
            updated_at = created_at + timedelta(minutes=random.randint(5, 60))

            cur.execute("""
                INSERT INTO staging.active_workorders
                (id, workorder_id, station_id, state, process_layer, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                new_id,
                wid,
                station_ids[i % len(station_ids)],
                1,
                0,
                created_at,
                updated_at
            ))

            created.append({
                "id": new_id,
                "workorder_id": wid,
            })

            new_id += 1

    conn.commit()
    print(f"✅ generated {len(created)} active_workorders")

    return created


def load_and_generate_bookings(
    writer: PostgresWriter,
    windows: dict,
    wo_snr_map: dict[int, list[int]],
    station_ids: list[int],
    failure_type_ids: list[int],
    pass_p: float = 0.85,
    fail_p: float = 0.10,
    scrap_p: float = 0.05,
    target_bookings: int = 500,
) -> list[dict[str, Any]]:

    if not windows:
        raise RuntimeError("windows est vide.")
    if not station_ids:
        raise RuntimeError("station_ids est vide.")

    conn = writer.connect()
    created: list[dict[str, Any]] = []

    wo_list = list(windows.items())

    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.bookings;")
        max_id = int(cur.fetchone()[0])
        new_id = max_id + 1

        cur.execute("SELECT id FROM staging.serial_numbers ORDER BY id;")
        valid_snr_ids = [int(row[0]) for row in cur.fetchall()]

        cur.execute("SELECT failure_type_id FROM staging.failure_types ORDER BY failure_type_id;")
        valid_failure_ids = [int(row[0]) for row in cur.fetchall()]

        for i in range(target_bookings):
            wid, w = random.choice(wo_list)

            start_dt = w.get("start") or datetime.now() - timedelta(days=1)
            end_dt = w.get("end") or datetime.now()

            if end_dt <= start_dt:
                end_dt = start_dt + timedelta(hours=1)

            ts = start_dt + (end_dt - start_dt) * random.random()

            r = random.random()
            if r < pass_p:
                state = "pass"
                failed_id = None
            elif r < pass_p + fail_p:
                state = "fail"
                failed_id = random.choice(valid_failure_ids) if valid_failure_ids else None
            else:
                state = "scrap"
                failed_id = random.choice(valid_failure_ids) if valid_failure_ids else None

            snr_ids = wo_snr_map.get(wid, [])
            valid_snr_for_wo = [sid for sid in snr_ids if sid in valid_snr_ids]
            snr_id = random.choice(valid_snr_for_wo) if valid_snr_for_wo else None

            updated_at = ts + timedelta(seconds=random.randint(0, 300))
            rct = max(30.0, random.gauss(240, 30.0))

            cur.execute("""
                INSERT INTO staging.bookings
                (id, workorder_id, station_id, failed_id, serial_number_id,
                 process_layer, date_of_booking, state, mesure_id,
                 real_cycle_time, type, snr_booking, booked_by,
                 created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                new_id,
                wid,
                station_ids[i % len(station_ids)],
                failed_id,
                snr_id,
                random.randint(0, 3),
                ts,
                state,
                None,
                round(rct, 3),
                "SNR" if snr_id else "batch",
                bool(snr_id),
                "Admin",
                ts,
                updated_at
            ))

            created.append({
                "id": new_id,
                "workorder_id": wid
            })

            new_id += 1

    conn.commit()
    print(f"✅ generated {len(created)} bookings")

    return created


def load_and_generate_measurement_data(
    writer: PostgresWriter,
    windows: dict,
    station_ids: list[int],
    n: int = 500
) -> list[dict[str, Any]]:

    if not windows:
        raise RuntimeError("windows est vide.")
    if not station_ids:
        raise RuntimeError("station_ids est vide.")

    conn = writer.connect()
    created = []
    wo_list = list(windows.items())

    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.measurement_data;")
        max_id = int(cur.fetchone()[0])
        new_id = max_id + 1

        for i in range(n):
            wid, w = random.choice(wo_list)

            start_dt = w.get("start") or datetime.now() - timedelta(days=1)
            end_dt = w.get("end") or datetime.now()

            if end_dt <= start_dt:
                end_dt = start_dt + timedelta(hours=1)

            ts = start_dt + (end_dt - start_dt) * random.random()
            fin = ts + timedelta(seconds=random.choice([1, 2, 3]))

            m = random.choice(MEASUREMENT_CATALOG)

            lo = float(m["lower_limit"])
            hi = float(m["upper_limit"])
            nominal = float(m["nominal"])
            tolerance = float(m["tolerance"])

            val = round(
                random.uniform(lo, hi)
                if random.random() < 0.95
                else random.choice([
                    random.uniform(lo * 0.85, lo),
                    random.uniform(hi, hi * 1.15),
                ]),
                4
            )

            fail_code = 0 if lo <= val <= hi else 1

            cur.execute("""
                INSERT INTO staging.measurement_data
                (id, station_id, workorder_id, book_date,
                 measure_name, measure_value, lower_limit, upper_limit,
                 nominal, tolerance, measure_fail_code, measure_type,
                 created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                new_id,
                station_ids[i % len(station_ids)],
                wid,
                ts,
                m["measure_name"],
                val,
                lo,
                hi,
                nominal,
                tolerance,
                fail_code,
                m["measure_type"],
                ts,
                fin
            ))

            created.append({
                "id": new_id,
                "workorder_id": wid
            })

            new_id += 1

    conn.commit()
    print(f"✅ generated {len(created)} measurement_data")

    return created


def load_and_generate_machine_condition_data(
    writer: PostgresWriter,
    station_ids: list[int],
    machine_condition_ids: list[int],
    window_start: datetime,
    window_end: datetime,
    n_max: int = 500,
    downtime_target: float = 0.15,
    run_min_h: float = 1,
    run_max_h: float = 4,
    down_min_h: float = 0.1,
    down_max_h: float = 1,
) -> list[dict[str, Any]]:

    if not station_ids:
        raise RuntimeError("station_ids est vide.")
    if not machine_condition_ids:
        raise RuntimeError("machine_condition_ids est vide.")

    conn = writer.connect()
    created: list[dict[str, Any]] = []

    run_min_s = run_min_h * 3600
    run_max_s = run_max_h * 3600
    down_min_s = down_min_h * 3600
    down_max_s = down_max_h * 3600

    running_mc_id = machine_condition_ids[-1]
    non_running_ids = machine_condition_ids[:-1] or [running_mc_id]

    produced = 0
    total_run = 0.0
    total_down = 0.0

    per_station_max = max(50, n_max // max(1, len(station_ids)))

    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM staging.machine_condition_data;")
        max_id = int(cur.fetchone()[0])
        new_id = max_id + 1

        for station_id in station_ids:
            if produced >= n_max:
                break

            total_seconds = (window_end - window_start).total_seconds()
            run_left = total_seconds * (1.0 - downtime_target)
            down_left = total_seconds * downtime_target
            station_produced = 0

            for sw_start, sw_end in [(window_start, window_end)]:
                if station_produced >= per_station_max or produced >= n_max:
                    break

                t = sw_start

                while t < sw_end and station_produced < per_station_max and produced < n_max:
                    remaining = (sw_end - t).total_seconds()

                    if remaining <= 0:
                        break

                    is_down = (
                        random.random() < (down_left / max(1, down_left + run_left))
                        if down_left > 0 and run_left > 0
                        else down_left > 0
                    )

                    if is_down:
                        duration = min(
                            remaining,
                            down_left,
                            random.uniform(down_min_s, down_max_s)
                        )
                        condition_id = random.choice(non_running_ids)
                    else:
                        duration = min(
                            remaining,
                            run_left,
                            random.uniform(run_min_s, run_max_s)
                        )
                        condition_id = running_mc_id

                    start_dt = t
                    end_dt = start_dt + timedelta(seconds=float(duration))

                    if end_dt <= start_dt:
                        break

                    segment_seconds = (end_dt - start_dt).total_seconds()

                    if is_down:
                        down_left -= segment_seconds
                        total_down += segment_seconds
                        level = "A"
                        color_rgb = "#8b1818"
                    else:
                        run_left -= segment_seconds
                        total_run += segment_seconds
                        level = "P"
                        color_rgb = "#13be1e"

                    cur.execute("""
                        INSERT INTO staging.machine_condition_data
                        (id, date_from, date_to, station_id, condition_id,
                         level, condition_stamp, condition_type, color_rgb,
                         condition_created, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                    """, (
                        new_id,
                        start_dt,
                        end_dt,
                        int(station_id),
                        int(condition_id),
                        level,
                        end_dt,
                        "s",
                        color_rgb,
                        start_dt,
                        end_dt
                    ))

                    created.append({
                        "id": new_id,
                        "station_id": station_id,
                    })

                    new_id += 1
                    produced += 1
                    station_produced += 1
                    t = end_dt

    conn.commit()

    total = total_run + total_down
    print(f"✅ generated {len(created)} machine_condition_data")

    if total > 0:
        print(f"    downtime: {total_down / total * 100:.1f}% target {downtime_target * 100:.0f}%")

    return created

if __name__ == "__main__":
    PG = PgConfig()
    writer = PostgresWriter(PG)

    try:
        company_code_ids = load_and_generate_company_codes(writer)
        client_ids = load_and_generate_clients(writer, company_code_ids)
        site_ids = load_and_generate_sites(writer, company_code_ids, extra_n=5)
        cells_ids = load_and_generate_cells(writer, site_ids, extra_n=20)
        machine_group_ids = load_and_generate_machine_groups(writer, extra_n=30)
        station_legacy_to_api_id = load_and_generate_stations(writer, extra_n=20)
        line_legacy_to_api_id = load_and_generate_lines(
            writer,
            station_legacy_to_api_id=station_legacy_to_api_id,
            extra_n=5
        )
        associations = build_line_station_associations(
            line_legacy_to_api_id,
            station_legacy_to_api_id)

        line_station_association =load_line_station_associations(writer, associations)
        
        part_group_type_ids = load_and_generate_part_group_types(writer, extra_n=10)
        
        part_group_map = load_and_generate_part_groups(writer, extra_n=6)
        part_type_ids = load_and_generate_part_types(writer, extra_n=6)
        
        part_number_map = load_and_generate_part_number_map(writer,part_type_ids, part_group_map, machine_group_ids, site_ids, unit_id=1 )
        
        part_master_ids = list(part_number_map.values())
        product_ids = part_master_ids[:8]
        component_ids = part_master_ids[8:]
        
        if not component_ids:
            print("⚠️ component_ids vide → fallback sur tous les part_master_ids")
            component_ids = part_master_ids
    
        failure_group_types_ids = load_and_generate_failure_group_types(writer, extra_n=6)
        failure_type_map = load_and_generate_failure_types(writer, 
                                                           failure_group_ids=failure_group_types_ids, 
                                                           site_ids=site_ids, 
                                                           extra_n=10)
        
        machine_condition_group_ids = load_and_generate_machine_condition_groups(writer, extra_n=3)
        machine_condition_map = load_and_generate_machine_conditions(writer,
                                                                    machine_condition_group_ids=machine_condition_group_ids,
                                                                    extra_n=6 )
        
        erp_group_map = load_and_generate_erp_groups(writer, extra_n=5)
        station_erp_assignments = load_and_generate_assign_stations_to_erpgrp(writer,extra_n=100)
        
        print("clients=", client_ids)
        workplan_rows = load_and_generate_workplans(writer,
                                                    site_ids=site_ids,
                                                    client_ids=client_ids,
                                                    company_code_ids=company_code_ids,
                                                    product_part_numbers=list(part_number_map.keys()),extra_n=10)
        workplan_by_part_no = {
            row["part_no"]: {"id": row["id"], "url": row["url"]}
            for row in workplan_rows
            if row.get("part_no")
        }
        
        workplan_ids = [row["id"] for row in workplan_rows]
        erp_group_ids = list(erp_group_map.values())
        workstep_rows = load_and_generate_worksteps(writer,
                                                    workplan_ids=workplan_ids,
                                                    erp_group_ids=erp_group_ids,
                                                    extra_n_per_workplan=5)
        
        bom_headers = load_and_generate_bom_headers(writer, part_number_map)

        
        bom_items = load_and_generate_bom_items(writer,[b["id"] for b in bom_headers],component_ids )
        
        window_end = datetime.now()
        window_start = window_end - timedelta(days=90)

        work_order_ids, windows = load_and_generate_work_orders(writer, client_ids,company_code_ids,
                                                                site_ids, list(part_number_map.keys()), list(part_number_map.values()), workplan_by_part_no,
                                                                window_start, window_end, n=50, wo_qty_min=10, wo_qty_max=100 )

        
        wo_snr_map = load_and_generate_serial_numbers(writer, windows, max_per_workorder=50)
        active_workorder_rows = load_and_generate_active_workorders(writer, windows=windows, 
                                                                    station_ids=list(station_legacy_to_api_id.values()),
                                                                    window_end=window_end,
                                                                    n=20)
        
        booking_rows = load_and_generate_bookings(writer, windows=windows,
                                                  wo_snr_map=wo_snr_map,
                                                  station_ids=list(station_legacy_to_api_id.values()),
                                                  failure_type_ids=list(failure_type_map.values()),
                                                  pass_p=0.85, fail_p=0.10, scrap_p=0.05, target_bookings=500)
        
        measurement_rows = load_and_generate_measurement_data(writer,windows=windows, station_ids=list(station_legacy_to_api_id.values()), n=500)
        
        machine_condition_data_rows = load_and_generate_machine_condition_data(writer,
                                                                               station_ids=list(station_legacy_to_api_id.values()),
                                                                               machine_condition_ids=list(machine_condition_map.values()),
                                                                               window_start=window_start,
                                                                               window_end=window_end,
                                                                               n_max=500,
                                                                               downtime_target=0.15,
                                                                               run_min_h=1,
                                                                               run_max_h=4,
                                                                               down_min_h=0.1,
                                                                               down_max_h=1
                                                                               )
    finally:
        writer.close()