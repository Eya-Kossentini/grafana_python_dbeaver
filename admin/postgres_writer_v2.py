from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional
import psycopg2 as psycopg


TABLE_SCHEMA_DEFAULT = "public"
TABLE_NAME_DEFAULT = "work_orders"

SERIAL_NUMBER_ID_MAX = int(os.getenv("SERIAL_NUMBER_ID_MAX", "1000"))


def now_ts_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@dataclass(frozen=True)
class PgConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    schema: str = TABLE_SCHEMA_DEFAULT
    table: str = TABLE_NAME_DEFAULT


def pg_table_qualified(pg: PgConfig) -> str:
    if not pg.schema.replace("_", "").isalnum():
        raise ValueError("Invalid schema name")
    if not pg.table.replace("_", "").isalnum():
        raise ValueError("Invalid table name")
    return f'"{pg.schema}"."{pg.table}"'


def pg_connect(pg: PgConfig):

    def _connect_with_host(host: str):
        conn = psycopg.connect(
            host=host,
            port=pg.port,
            dbname=pg.dbname,
            user=pg.user,
            password=pg.password
        )
        conn.autocommit = True  # Set autocommit after connection
        return conn

    try:
        return _connect_with_host(pg.host)
    except psycopg.OperationalError as e:
        msg = str(e)
        if "getaddrinfo failed" in msg or "failed to resolve host" in msg:
            fallbacks = ["localhost", "127.0.0.1"]
            last_exc: Exception = e
            for host in fallbacks:
                try:
                    socket.getaddrinfo(host, pg.port)
                    return _connect_with_host(host)
                except Exception as ee:
                    last_exc = ee
            raise psycopg.OperationalError(
                f"Could not resolve Postgres host '{pg.host}'. "
                f"Try PGHOST=localhost (or your server IP). Original error: {e}"
            ) from last_exc
        raise


class PostgresWriter:
    def __init__(self, pg: PgConfig):
        self.pg = pg
        self.conn = None

    def connect(self):
        if self.conn is None:
            self.conn = pg_connect(self.pg)
        return self.conn

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def fetch_id_list(self, sql: str) -> list[int]:
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return [int(r[0]) for r in rows if r and r[0] is not None]

    def get_next_id(self) -> int:
        conn = self.connect()
        tbl = pg_table_qualified(self.pg)
        with conn.cursor() as cur:
            cur.execute(f"SELECT COALESCE(MAX(id), 0) FROM {tbl};")
            (max_id,) = cur.fetchone()
        return int(max_id) + 1

    def insert_measurement(self, row_id: int, payload: dict[str, Any]) -> None:
        conn = self.connect()
        tbl = pg_table_qualified(self.pg)

        # NOTE: measurement_data uses uppercase quoted column names in the schema.
        # The generator inserts via bulk SQL with "ID", "STATION_ID", "WORKORDER_ID",
        # "BOOK_DATE", "MEASURE_NAME", "MEASURE_VALUE", "LOWER_LIMIT", "UPPER_LIMIT",
        # "NOMINAL", "TOLERANCE", "MEASURE_FAIL_CODE", "MEASURE_TYPE".
        # When used as a TimescaleDB hypertable there may be no unique constraint on "ID".
        sql = f"""
        INSERT INTO {tbl} (
            "ID",
            "STATION_ID",
            "WORKORDER_ID",
            "BOOK_DATE",
            "MEASURE_NAME",
            "MEASURE_VALUE",
            "LOWER_LIMIT",
            "UPPER_LIMIT",
            "NOMINAL",
            "TOLERANCE",
            "MEASURE_FAIL_CODE",
            "MEASURE_TYPE",
            created_at,
            updated_at
        ) VALUES (
            %(id)s,
            %(station_id)s,
            %(workorder_id)s,
            CAST(%(book_date)s AS timestamp),
            %(measure_name)s,
            %(measure_value)s,
            %(lower_limit)s,
            %(upper_limit)s,
            %(nominal)s,
            %(tolerance)s,
            %(measure_fail_code)s,
            %(measure_type)s,
            CAST(%(created_at)s AS timestamp),
            CAST(%(updated_at)s AS timestamp)
        );
        """

        params = {
            "id": row_id,
            "station_id": int(payload.get("STATION_ID") or payload["station_id"]),
            "workorder_id": int(payload.get("WORKORDER_ID") or payload["workorder_id"]),
            "book_date": str(payload.get("BOOK_DATE") or payload.get("book_date") or now_ts_str()),
            "measure_name": payload.get("MEASURE_NAME") or payload.get("measure_name"),
            "measure_value": payload.get("MEASURE_VALUE") or payload.get("measure_value"),
            "lower_limit": payload.get("LOWER_LIMIT") or payload.get("lower_limit"),
            "upper_limit": payload.get("UPPER_LIMIT") or payload.get("upper_limit"),
            "nominal": payload.get("NOMINAL") or payload.get("nominal"),
            "tolerance": payload.get("TOLERANCE") or payload.get("tolerance"),
            "measure_fail_code": payload.get("MEASURE_FAIL_CODE") or payload.get("measure_fail_coder") or 0,
            "measure_type": payload.get("MEASURE_TYPE") or payload.get("measure_type"),
            "created_at": str(payload.get("created_at") or now_ts_str()),
            "updated_at": str(payload.get("updated_at") or now_ts_str()),
        }

        with conn.cursor() as cur:
            cur.execute(sql, params)

    def update_measurement_updated_at(self, row_id: int, updated_at: str) -> None:
        conn = self.connect()
        tbl = pg_table_qualified(self.pg)

        sql = f"""
        UPDATE {tbl}
        SET
            updated_at = CAST(%(updated_at)s AS timestamp)
        WHERE
            id = %(id)s;
        """
        params = {
            "id": int(row_id),
            "updated_at": str(updated_at),
        }
        with conn.cursor() as cur:
            cur.execute(sql, params)

    def insert_booking(self, row_id: int, payload: dict[str, Any]) -> None:
        conn = self.connect()
        tbl = pg_table_qualified(self.pg)

        workorder_id = int(payload["workorder_id"])
        station_id = int(payload["station_id"])
        mesure_id = int(payload["mesure_id"])

        serial_number_id: Optional[int]
        if payload.get("serial_number_id") is None:
            serial_number_id = None
        else:
            serial_number_id = int(payload["serial_number_id"])

        process_layer: Optional[int]
        if payload.get("process_layer") is None:
            process_layer = None
        else:
            process_layer = int(payload["process_layer"])

        state = payload.get("state")
        if state is None:
            state = payload.get("state pass")

        state_norm = str(state).lower() if state is not None else None

        allowed_states = {"pass", "fail", "scrap"}
        if state_norm is not None and state_norm not in allowed_states:
            raise ValueError(
                f"Invalid booking state '{state}'. Allowed: pass|fail|scrap"
            )

        # Enforce consistency: PASS bookings must not carry a failure type.
        # FAIL/SCRAP bookings should have a valid failure_type id.
        failed_id: Optional[int]
        if state_norm == "pass":
            failed_id = None
        else:
            if payload.get("failed_id") is None:
                raise ValueError("failed_id is required when state is fail|scrap")
            failed_id = int(payload["failed_id"])

        booking_type = payload.get("type")
        if booking_type is not None:
            allowed_types = {"snr", "batch"}
            if str(booking_type).strip().lower() not in allowed_types:
                raise ValueError("Invalid bookings.type. Allowed: SNR|batch")

        # ID validity is enforced by Postgres foreign keys (see schema constraints).
        # Keep only minimal sanity checks here so we don't reject valid IDs.
        if workorder_id <= 0:
            raise ValueError("workorder_id must be a positive integer")
        if station_id <= 0:
            raise ValueError("station_id must be a positive integer")
        if failed_id is not None and failed_id <= 0:
            raise ValueError("failed_id must be a positive integer")
        if serial_number_id is not None and not (
            1 <= serial_number_id <= SERIAL_NUMBER_ID_MAX
        ):
            raise ValueError(f"serial_number_id must be in [1..{SERIAL_NUMBER_ID_MAX}]")
        if process_layer is not None and not (0 <= process_layer <= 3):
            raise ValueError("process_layer must be in [0..3]")
        if not (1 <= mesure_id <= 14):
            raise ValueError("mesure_id must be in [1..14]")

        # NOTE: When `bookings` is converted to a TimescaleDB hypertable, it may not
        # have a unique constraint on `id`, so we avoid ON CONFLICT.
        sql = f"""
        INSERT INTO {tbl} (
            id,
            workorder_id,
            station_id,
            failed_id,
            serial_number_id,
            process_layer,
            date_of_booking,
            state,
            mesure_id,
            real_cycle_time,
            \"type\",
            created_at,
            updated_at
        ) VALUES (
            %(id)s,
            %(workorder_id)s,
            %(station_id)s,
            %(failed_id)s,
            %(serial_number_id)s,
            %(process_layer)s,
            CAST(%(date_of_booking)s AS timestamp),
            %(state)s,
            %(mesure_id)s,
            %(real_cycle_time)s,
            %(type)s,
            CAST(%(created_at)s AS timestamp),
            CAST(%(updated_at)s AS timestamp)
        );
        """

        params = {
            "id": row_id,
            "workorder_id": workorder_id,
            "station_id": station_id,
            "failed_id": failed_id,
            "serial_number_id": serial_number_id,
            "process_layer": process_layer,
            "date_of_booking": str(payload.get("date_of_booking") or now_ts_str()),
            "state": state_norm,
            "mesure_id": mesure_id,
            "real_cycle_time": payload.get("real_cycle_time"),
            "type": (str(booking_type).strip() if booking_type is not None else None),
            "created_at": str(payload.get("created_at") or now_ts_str()),
            "updated_at": str(payload.get("updated_at") or now_ts_str()),
        }

        with conn.cursor() as cur:
            cur.execute(sql, params)

    def insert_machine_condition(self, row_id: int, payload: dict[str, Any]) -> None:
        conn = self.connect()
        tbl = pg_table_qualified(self.pg)

        group_id = int(payload["group_id"])
        if group_id <= 0:
            raise ValueError("group_id must be a positive integer")

        sql = f"""
        INSERT INTO {tbl} (
            id,
            group_id,
            condition_name,
            condition_description,
            color_rgb,
            is_active,
            created_at,
            updated_at
        ) VALUES (
            %(id)s,
            %(group_id)s,
            %(condition_name)s,
            %(condition_description)s,
            %(color_rgb)s,
            %(is_active)s,
            CAST(%(created_at)s AS timestamp),
            CAST(%(updated_at)s AS timestamp)
        )
        ON CONFLICT (id) DO UPDATE SET
            group_id = EXCLUDED.group_id,
            condition_name = EXCLUDED.condition_name,
            condition_description = EXCLUDED.condition_description,
            color_rgb = EXCLUDED.color_rgb,
            is_active = EXCLUDED.is_active,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at;
        """

        params = {
            "id": row_id,
            "group_id": group_id,
            "condition_name": payload.get("condition_name"),
            "condition_description": payload.get("condition_description"),
            "color_rgb": payload.get("color_rgb"),
            "is_active": payload.get("is_active"),
            "created_at": str(payload.get("created_at") or now_ts_str()),
            "updated_at": str(payload.get("updated_at") or now_ts_str()),
        }

        with conn.cursor() as cur:
            cur.execute(sql, params)

    def insert_machine_condition_data(
        self, row_id: int, payload: dict[str, Any]
    ) -> None:
        conn = self.connect()
        tbl = pg_table_qualified(self.pg)

        station_id = int(payload["station_id"])
        condition_id = int(payload["condition_id"])
        if station_id <= 0:
            raise ValueError("station_id must be a positive integer")
        if condition_id <= 0:
            raise ValueError("condition_id must be a positive integer")

        # NOTE: When `machine_condition_data` is converted to a TimescaleDB hypertable,
        # it may not have a unique constraint on `id`. We still need "start then finish"
        # semantics, so we do UPDATE-by-id first, and INSERT if missing.

        params = {
            "id": int(row_id),
            "date_from": str(payload.get("date_from") or now_ts_str()),
            "date_to": payload.get("date_to"),
            "station_id": station_id,
            "condition_id": condition_id,
            "level": payload.get("level"),
            "condition_created": payload.get("condition_created"),
            "condition_stamp": payload.get("condition_stamp"),
            "condition_type": payload.get("condition_type"),
            "color_rgb": payload.get("color_rgb"),
            "updated_at": payload.get("updated_at"),
        }

        sql_update = f"""
        UPDATE {tbl}
        SET
            date_from = CAST(%(date_from)s AS timestamp),
            date_to = CAST(%(date_to)s AS timestamp),
            station_id = %(station_id)s,
            condition_id = %(condition_id)s,
            level = %(level)s,
            condition_created = CAST(%(condition_created)s AS timestamp),
            condition_stamp = CAST(%(condition_stamp)s AS timestamp),
            condition_type = %(condition_type)s,
            color_rgb = %(color_rgb)s,
            updated_at = CAST(%(updated_at)s AS timestamp)
        WHERE
            id = %(id)s;
        """

        sql_insert = f"""
        INSERT INTO {tbl} (
            id,
            date_from,
            date_to,
            station_id,
            condition_id,
            level,
            condition_created,
            condition_stamp,
            condition_type,
            color_rgb,
            updated_at
        ) VALUES (
            %(id)s,
            CAST(%(date_from)s AS timestamp),
            CAST(%(date_to)s AS timestamp),
            %(station_id)s,
            %(condition_id)s,
            %(level)s,
            CAST(%(condition_created)s AS timestamp),
            CAST(%(condition_stamp)s AS timestamp),
            %(condition_type)s,
            %(color_rgb)s,
            CAST(%(updated_at)s AS timestamp)
        );
        """

        with conn.cursor() as cur:
            cur.execute(sql_update, params)
            if int(cur.rowcount or 0) == 0:
                cur.execute(sql_insert, params)

    def insert_work_order(self, row_id: int, payload: dict[str, Any]) -> None:
        conn = self.connect()
        tbl = pg_table_qualified(self.pg)

        sql = f"""
        INSERT INTO {tbl} (
            id,
            workorder_no,
            workorder_qty,
            startdate,
            deliverydate,
            created,
            stamp,
            workorder_state,
            aps_planning_start_date,
            aps_planning_stamp,
            aps_planning_end_date,
            site_id,
            client_id,
            company_id
        ) VALUES (
            %(id)s,
            %(workorder_no)s,
            %(workorder_qty)s,
            CAST(%(startdate)s AS timestamp),
            CAST(%(deliverydate)s AS timestamp),
            CAST(%(created)s AS timestamp),
            CAST(%(stamp)s AS timestamp),
            %(workorder_state)s,
            CAST(%(aps_planning_start_date)s AS timestamp),
            CAST(%(aps_planning_stamp)s AS timestamp),
            CAST(%(aps_planning_end_date)s AS timestamp),
            %(site_id)s,
            %(client_id)s,
            %(company_id)s
        )
        ON CONFLICT (id) DO UPDATE SET
            workorder_no = EXCLUDED.workorder_no,
            workorder_qty = EXCLUDED.workorder_qty,
            startdate = EXCLUDED.startdate,
            deliverydate = EXCLUDED.deliverydate,
            created = EXCLUDED.created,
            stamp = EXCLUDED.stamp,
            workorder_state = EXCLUDED.workorder_state,
            aps_planning_start_date = EXCLUDED.aps_planning_start_date,
            aps_planning_stamp = EXCLUDED.aps_planning_stamp,
            aps_planning_end_date = EXCLUDED.aps_planning_end_date,
            site_id = EXCLUDED.site_id,
            client_id = EXCLUDED.client_id,
            company_id = EXCLUDED.company_id;
        """

        workorder_state = payload.get("workorder_state")
        if workorder_state is not None:
            allowed = {"open", "planned", "active", "finished", "delivered"}
            if str(workorder_state).strip().lower() not in allowed:
                raise ValueError(
                    "Invalid workorder_state. Allowed: open|planned|active|finished|delivered"
                )

        params = {
            "id": int(row_id),
            "workorder_no": payload.get("workorder_no"),
            "workorder_qty": payload.get("workorder_qty"),
            "startdate": payload.get("startdate"),
            "deliverydate": payload.get("deliverydate"),
            "created": payload.get("created"),
            "stamp": payload.get("stamp"),
            "workorder_state": (
                str(workorder_state).strip().lower()
                if workorder_state is not None
                else None
            ),
            "aps_planning_start_date": payload.get("aps_planning_start_date"),
            "aps_planning_stamp": payload.get("aps_planning_stamp"),
            "aps_planning_end_date": payload.get("aps_planning_end_date"),
            "site_id": int(payload.get("site_id") or 1),
            "client_id": int(payload.get("client_id") or 1),
            "company_id": int(payload.get("company_id") or 1),
        }

        with conn.cursor() as cur:
            cur.execute(sql, params)

    def insert_active_workorder(self, row_id: int, payload: dict[str, Any]) -> None:
        conn = self.connect()
        tbl = pg_table_qualified(self.pg)

        # NOTE: When `active_workorders` is converted to a TimescaleDB hypertable, it may not
        # have a unique constraint on `id`, so we avoid ON CONFLICT.
        sql = f"""
        INSERT INTO {tbl} (
            id,
            workorder_id,
            station_id,
            state,
            process_layer,
            created_at,
            updated_at
        ) VALUES (
            %(id)s,
            %(workorder_id)s,
            %(station_id)s,
            %(state)s,
            %(process_layer)s,
            CAST(%(created_at)s AS timestamp),
            CAST(%(updated_at)s AS timestamp)
        );
        """

        params = {
            "id": int(row_id),
            "workorder_id": int(payload["workorder_id"]),
            "station_id": int(payload["station_id"]),
            "state": (str(payload["state"]) if payload.get("state") is not None else None),
            "process_layer": payload.get("process_layer"),
            "created_at": str(payload.get("created_at") or now_ts_str()),
            "updated_at": str(
                payload.get("updated_at") or payload.get("created_at") or now_ts_str()
            ),
        }

        with conn.cursor() as cur:
            cur.execute(sql, params)

    def cleanup_active_workorders(self) -> int:
        """Remove rows from active_workorders whose referenced work order is not active.

        This keeps `public.active_workorders` aligned with `public.work_orders.workorder_state`.
        """

        conn = self.connect()
        tbl = pg_table_qualified(self.pg)

        sql = f"""
        DELETE FROM {tbl} a
        WHERE EXISTS (
            SELECT 1
            FROM public.work_orders w
            WHERE w.id = a.workorder_id
              AND lower(w.workorder_state) <> 'active'
        );
        """

        with conn.cursor() as cur:
            cur.execute(sql)
            return int(cur.rowcount or 0)
