from __future__ import annotations

import os
import requests
import psycopg2
from dataclasses import dataclass
from dotenv import load_dotenv
from datetime import datetime
from constants import *
from datetime import datetime, timedelta
from generate_industrial_data_v2 import *


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



def pg_table_qualified(pg: PgConfig, table: str) -> str:
    return f'{pg.schema}.{table}'


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

# ---------------------------------------------------------------------------
# Structure industrielle
# cells → sites
# machine_groups → cells
# stations → machine_groups
# lines → stations
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# REFERENTIELS 
# part_groups → part_group_types
# part_number_map → part_types + part_groups
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Execution 
#gen_work_orders_api
#gen_serial_numbers_api
#gen_active_workorders_api
#gen_bookings_api
#gen_measurement_data_api
#gen_machine_condition_data_api
# ---------------------------------------------------------------------------

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
        conn = writer.connect()

        with conn.cursor() as cur:

            # Workorders
            cur.execute("""
                SELECT id,
                    startdate,
                    deliverydate,
                    part_number,
                    company_id,
                    workorder_qty
                FROM staging.work_orders
                WHERE startdate IS NOT NULL
                AND deliverydate IS NOT NULL;
            """)

            rows = cur.fetchall()

            windows = {}

            for row in rows:
                wid, start_dt, end_dt, part_number, company_id, qty = row

                windows[int(wid)] = {
                    "start": start_dt,
                    "end": end_dt,
                    "part_number": part_number,
                    "company_id": company_id,
                    "qty": qty,
                }

            # serial numbers
            cur.execute("""
                SELECT workorder_id, id
                FROM staging.serial_numbers
                WHERE workorder_id IS NOT NULL;
            """)

            wo_snr_map = {}

            for wid, snr_id in cur.fetchall():
                wo_snr_map.setdefault(int(wid), []).append(int(snr_id))

            # stations
            cur.execute("SELECT id FROM staging.stations ORDER BY id;")
            station_ids = [int(r[0]) for r in cur.fetchall()]

            # machine conditions
            cur.execute("SELECT id FROM staging.machine_conditions ORDER BY id;")
            machine_condition_ids = [int(r[0]) for r in cur.fetchall()]

            # failure types
            cur.execute("""
                SELECT failure_type_id
                FROM staging.failure_types
                ORDER BY failure_type_id;
            """)
            failure_type_ids = [int(r[0]) for r in cur.fetchall()]

        window_start = datetime.now() - timedelta(days=90)
        window_end = datetime.now()

        booking_rows = load_and_generate_bookings(writer, windows=windows,
                                                  wo_snr_map=wo_snr_map,
                                                  station_ids=station_ids,
                                                  failure_type_ids=failure_type_ids,
                                                  pass_p=0.85, fail_p=0.10, scrap_p=0.05, target_bookings=500)
       
        measurement_rows = load_and_generate_measurement_data(writer,windows=windows, station_ids=station_ids, n=500)
        
        machine_condition_data_rows = load_and_generate_machine_condition_data(writer,
                                                                               station_ids=station_ids,
                                                                               machine_condition_ids=machine_condition_ids,
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