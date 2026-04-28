from dataclasses import dataclass
import psycopg2
import requests

import os
from dotenv import load_dotenv

load_dotenv()

POST_API_URL = "http://127.0.0.1:8000"
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
            
            
def push_bookings_to_api(writer: PostgresWriter):
    conn = writer.connect()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                workorder_id,
                station_id,
                failed_id,
                serial_number_id,
                process_layer,
                date_of_booking,
                state,
                mesure_id,
                real_cycle_time,
                type,
                snr_booking,
                created_at,
                updated_at,
                id
            FROM staging.bookings
            ORDER BY id;
        """)

        rows = cur.fetchall()

    for row in rows:
        payload = {
            "workorder_id": row[0],
            "station_id": row[1],
            "failed_id": row[2],
            "serial_number_id": row[3],
            "process_layer": row[4],
            "date_of_booking": row[5].isoformat() if row[5] else None,
            "state": row[6],
            "mesure_id": row[7],
            "real_cycle_time": float(row[8]) if row[8] else None,
            "type": row[9],
            "snr_booking": row[10],
            "created_at": row[11].isoformat() if row[11] else None,
            "updated_at": row[12].isoformat() if row[12] else None,
            "id": row[13],
        }

        r = requests.post(
            f"{POST_API_URL}/bookings/bookings/",
            json=payload,
            headers=headers,
            timeout=30,
            verify=False
        )

        print("POST booking", payload["id"], r.status_code, r.text[:200])
        
        
if __name__ == "__main__":
    PG = PgConfig()
    writer = PostgresWriter(PG)

    try:
        push_bookings_to_api(writer)
    finally:
        writer.close()