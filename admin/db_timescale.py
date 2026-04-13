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
        INSERT INTO availability_kpi(
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
        INSERT INTO performance_kpi (
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
        INSERT INTO oee_kpi(
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
    
    
def save_quality(item):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO quality_kpi(
            production_day,
            station_id,
            total_bookings,
            good_count,
            fail_count,
            scrap_count,
            quality_pct,
            defect_rate_pct
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (production_day, station_id)
        DO UPDATE SET
            total_bookings = EXCLUDED.total_bookings,
            good_count = EXCLUDED.good_count,
            fail_count = EXCLUDED.fail_count,
            scrap_count = EXCLUDED.scrap_count,
            quality_pct = EXCLUDED.quality_pct,
            defect_rate_pct = EXCLUDED.defect_rate_pct,
            created_at = NOW()
        RETURNING id;
    """, (
        item.production_day,
        item.station_id,
        item.total_bookings,
        item.good_count,
        item.fail_count,
        item.scrap_count,
        item.quality_pct,
        item.defect_rate_pct
    ))

    row_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return row_id
    
    
def save_pareto(item):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO pareto_losses_kpi(
                station_id,
                production_day,
                loss_type,
                loss_hours,
                loss_pct,
                cumulative_pct,
                pareto_rank,
                is_critical
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (station_id, production_day, loss_type)
        DO UPDATE SET
            loss_hours = EXCLUDED.loss_hours,
            loss_pct = EXCLUDED.loss_pct,
            cumulative_pct = EXCLUDED.cumulative_pct,
            pareto_rank = EXCLUDED.pareto_rank,
            is_critical = EXCLUDED.is_critical,
            created_at = NOW()
        RETURNING station_id;
    """, (
        item.station_id,
        item.production_day,
        item.loss_type,
        item.loss_hours,
        item.loss_pct,
        item.cumulative_pct,
        item.pareto_rank,
        item.is_critical
    ))

    row_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return row_id
    
    
    

def save_reliability(item):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO reliability_diagnostic_kpi(
            station_id,
            mtbf_hours,
            top_loss_type,
            top_loss_pct,
            pareto_rank,
            criticality_level,
            diagnosis
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (station_id)
        DO UPDATE SET
            mtbf_hours = EXCLUDED.mtbf_hours,
            top_loss_type = EXCLUDED.top_loss_type,
            top_loss_pct = EXCLUDED.top_loss_pct,
            pareto_rank = EXCLUDED.pareto_rank,
            criticality_level = EXCLUDED.criticality_level,
            diagnosis = EXCLUDED.diagnosis
        RETURNING station_id;
    """, (
        item.station_id,
        item.mtbf_hours,
        item.top_loss_type,
        item.top_loss_pct,
        item.pareto_rank,
        item.criticality_level,
        item.diagnosis
    ))

    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    return row[0] if row else None




def save_scrap(item):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO scrap_by_day_kpi(
            production_day,
            station_id,
            total_bookings,
            scrap_count,
            scrap_rate_pct
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (production_day, station_id)
        DO UPDATE SET
            total_bookings = EXCLUDED.total_bookings,
            scrap_count = EXCLUDED.scrap_count,
            scrap_rate_pct = EXCLUDED.scrap_rate_pct,
            created_at = NOW()
        RETURNING id;
    """, (
        item.production_day,
        item.station_id,
        item.total_bookings,
        item.scrap_count,
        item.scrap_rate_pct
    ))

    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    return row[0] if row else None


def save_downtime(item):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO downtime_by_station_kpi(
            station_id,
            production_day,
            downtime_type,
            downtime_hours,
            downtime_minutes,
            downtime_events
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (station_id, production_day, downtime_type)
        DO UPDATE SET
            downtime_hours = EXCLUDED.downtime_hours,
            downtime_minutes = EXCLUDED.downtime_minutes,
            downtime_events = EXCLUDED.downtime_events,
            created_at = NOW()
        RETURNING station_id;
    """, (
        item.station_id,
        item.production_day,
        item.downtime_type,
        item.downtime_hours,
        item.downtime_minutes,
        item.downtime_events
    ))

    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    return row[0] if row else None




def save_dashboard(item):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO dashboard_overview(
            station_id,
            production_day,
            oee_pct,
            availability_pct,
            performance_pct,
            quality_pct,
            mtbf_hours,
            mttr_hours
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (station_id, production_day)
        DO UPDATE SET
            oee_pct = EXCLUDED.oee_pct,
            availability_pct = EXCLUDED.availability_pct,
            performance_pct = EXCLUDED.performance_pct,
            quality_pct = EXCLUDED.quality_pct,
            mtbf_hours = EXCLUDED.mtbf_hours,
            mttr_hours = EXCLUDED.mttr_hours,  
            created_at = NOW()
        RETURNING station_id;
    """, (
        item.station_id,
        item.production_day,
        item.oee_pct,
        item.availability_pct,
        item.performance_pct,
        item.quality_pct,
        item.mtbf_hours,
        item.mttr_hours
    ))

    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    return row[0] if row else None




def save_failure_loss(item):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO failure_loss_diagnostic_kpi(
            station_id,
            top_failure_group,
            top_failure_count,
            top_failure_pct,
            top_loss_type,
            top_loss_hours,
            top_loss_pct,
            criticality_level,
            diagnosis
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (station_id, top_failure_pct)
        DO UPDATE SET
            top_failure_group = EXCLUDED.top_failure_group,
            top_failure_count = EXCLUDED.top_failure_count,
            top_loss_type = EXCLUDED.top_loss_type,
            top_loss_hours = EXCLUDED.top_loss_hours,
            top_loss_pct = EXCLUDED.top_loss_pct,
            criticality_level = EXCLUDED.criticality_level,  
            diagnosis = EXCLUDED.diagnosis,
            created_at = NOW()
        RETURNING station_id;
    """, (
        item.station_id,
        item.top_failure_group,
        item.top_failure_count,
        item.top_failure_pct,
        item.top_loss_type,
        item.top_loss_hours,
        item.top_loss_pct,
        item.criticality_level,
        item.diagnosis
    ))

    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    return row[0] if row else None




def save_mttr(item):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO mttr_kpi(
            station_id,
            repair_time_hours,
            failure_count,
            mttr_hours
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (station_id, repair_time_hours)
        DO UPDATE SET
            failure_count = EXCLUDED.failure_count,
            mttr_hours = EXCLUDED.mttr_hours,
            created_at = NOW()
        RETURNING station_id;
    """, (
        item.station_id,
        item.repair_time_hours,
        item.failure_count,
        item.mttr_hours
    ))

    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    return row[0] if row else None



def save_mtbf(item):
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO mtbf_kpi(
            station_id,
            run_time_hours,
            failure_count,
            mtbf_hours
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (station_id, run_time_hours)
        DO UPDATE SET
            failure_count = EXCLUDED.failure_count,
            mtbf_hours = EXCLUDED.mtbf_hours,
            created_at = NOW()
        RETURNING station_id;
    """, (
        item.station_id,
        item.run_time_hours,
        item.failure_count,
        item.mtbf_hours
    ))

    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    return row[0] if row else None
  
    
def save_defect(item):
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO defect_rate_kpi(
            station_id,
            total_bookings,
            good_count,
            fail_count,
            scrap_count,
            defect_count,
            defect_rate_pct
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (station_id, total_bookings)
        DO UPDATE SET
            good_count = EXCLUDED.good_count,
            fail_count = EXCLUDED.fail_count,
            scrap_count = EXCLUDED.scrap_count,
            defect_count = EXCLUDED.defect_count,
            defect_rate_pct = EXCLUDED.defect_rate_pct,
            created_at = NOW()
        RETURNING station_id;
    """, (
        item.station_id,
        item.total_bookings,
        item.good_count,
        item.fail_count,
        item.scrap_count,
        item.defect_count,
        item.defect_rate_pct
    ))

    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    return row[0] if row else None
