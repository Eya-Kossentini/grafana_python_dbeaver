import random
from datetime import datetime
import psycopg2

DB_HOST = "localhost"   # si tu lances le script depuis ton PC
DB_PORT = "5435"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "admin123"


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def save_availability(item):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO public.availability_results (
            production_day,
            station_id,
            run_time_hours,
            micro_stop_hours,
            breakdown_hours,
            planned_stop_hours,
            availability_pct
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (production_day, station_id)
        DO UPDATE SET
            run_time_hours = EXCLUDED.run_time_hours,
            micro_stop_hours = EXCLUDED.micro_stop_hours,
            breakdown_hours = EXCLUDED.breakdown_hours,
            planned_stop_hours = EXCLUDED.planned_stop_hours,
            availability_pct = EXCLUDED.availability_pct,
            created_at = NOW()
        RETURNING id;
    """, (
        item.production_day,
        item.station_id,
        item.run_time_hours,
        item.micro_stop_hours,
        item.breakdown_hours,
        item.planned_stop_hours,
        item.availability_pct,
    ))

    row_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return row_id
    


def save_performance(item):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO public.performance_results (
            production_day,
            station_id,
            run_time_hours,
            micro_stop_hours,
            performance_pct
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (production_day, station_id)
        DO UPDATE SET
            run_time_hours = EXCLUDED.run_time_hours,
            micro_stop_hours = EXCLUDED.micro_stop_hours,
            performance_pct = EXCLUDED.performance_pct,
            created_at = NOW()
        RETURNING id;
    """, (
        item.production_day,
        item.station_id,
        item.run_time_hours,
        item.micro_stop_hours,
        item.performance_pct,
    ))

    row_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return row_id
    
    

def save_oee(item):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO public.oee_results (
            production_day,
            station_id,
            availability_pct,
            performance_pct,
            quality_pct,
            quality_missing,
            oee_pct
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (production_day, station_id)
        DO UPDATE SET
            availability_pct = EXCLUDED.availability_pct,
            performance_pct = EXCLUDED.performance_pct,
            quality_pct = EXCLUDED.quality_pct,
            quality_missing = EXCLUDED.quality_missing,
            oee_pct = EXCLUDED.oee_pct,
            created_at = NOW()
        RETURNING id;
    """, (
        item.production_day,
        item.station_id,
        item.availability_pct,
        item.performance_pct,
        item.quality_pct,
        item.quality_missing,
        item.oee_pct
    ))

    row_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return row_id
    
    
