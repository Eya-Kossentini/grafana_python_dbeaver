from datetime import datetime, timedelta, time
import random
import psycopg2

PG = {
    "host": "localhost",
    "port": 5435,
    "dbname": "postgres",
    "user": "postgres",
    "password": "admin123",
}
CONDITION_KEYWORDS = {
    "running": ["1000", "running"],
    "microstop": ["1003", "micro stop"],
    "waiting": ["1001", "waiting"],
    "cleaning": ["5001", "cleaning"],
    "setup": ["6001", "setup"],
    "breakdown": ["2000", "breakdown"],
    "maintenance": ["3100", "preventive maintenance"],
    "break": ["3005", "break", "no production"],
    "trial": ["3003", "trial"],
    "meeting": ["3004", "meeting"],
}

CONDITION_COLORS = {
    "running": "#13be1e",
    "microstop": "#d6a624",
    "waiting": "#c811aa",
    "cleaning": "#099f95",
    "setup": "#4940c9",
    "breakdown": "#8b1818",
    "maintenance": "#e6d628",
    "break": "#3e6eac",
    "trial": "#3a884e",
    "meeting": "#ea7f06",
}

CONDITION_WEIGHTS = [
    ("running", 0.78),
    ("microstop", 0.06),
    ("waiting", 0.03),
    ("cleaning", 0.03),
    ("setup", 0.04),
    ("breakdown", 0.03),
    ("maintenance", 0.02),
    ("break", 0.005),
    ("trial", 0.005),
    ("meeting", 0.005),
]


def get_condition_ids(cur):
    cur.execute("""
        SELECT id,
               COALESCE(condition_name, '') AS condition_name,
               COALESCE(condition_description, '') AS condition_description
        FROM staging.machine_conditions;
    """)

    rows = cur.fetchall()
    condition_ids = {}

    for key, keywords in CONDITION_KEYWORDS.items():
        for condition_id, name, desc in rows:
            text = f"{name} {desc}".lower()

            if any(keyword.lower() in text for keyword in keywords):
                condition_ids[key] = int(condition_id)
                break

    if "running" not in condition_ids:
        raise RuntimeError("Aucune condition Running trouvée dans staging.machine_conditions")

    # fallback : si une condition manque, on utilise running
    for key in CONDITION_KEYWORDS:
        condition_ids.setdefault(key, condition_ids["running"])

    return condition_ids


def weighted_condition():
    r = random.random()
    cumulative = 0

    for condition, weight in CONDITION_WEIGHTS:
        cumulative += weight
        if r <= cumulative:
            return condition

    return "running"


def generate_segments(day):
    """
    Génère une journée avec plusieurs segments réalistes.
    """
    day_start = datetime.combine(day, time(0, 0, 0))
    day_end = datetime.combine(day, time(23, 59, 59))

    segments = []
    current = day_start

    while current < day_end:
        condition = weighted_condition()
        
        if condition == "running":
            duration_minutes = random.randint(90, 240)

        elif condition == "microstop":
            duration_minutes = random.randint(2, 10)

        elif condition == "waiting":
            duration_minutes = random.randint(5, 20)

        elif condition == "cleaning":
            duration_minutes = random.randint(10, 40)

        elif condition == "setup":
            duration_minutes = random.randint(20, 60)

        elif condition == "breakdown":
            duration_minutes = random.randint(15, 120)

        elif condition == "maintenance":
            duration_minutes = random.randint(30, 180)

        elif condition == "meeting":
            duration_minutes = random.randint(15, 60)

        elif condition == "trial":
            duration_minutes = random.randint(30, 90)

        else:
            duration_minutes = random.randint(30, 120)

        segment_end = current + timedelta(minutes=duration_minutes)

        if segment_end > day_end:
            segment_end = day_end

        if segment_end > current:
            segments.append((current, segment_end, condition))

        current = segment_end

    return segments


def main():
    conn = psycopg2.connect(**PG)

    try:
        with conn.cursor() as cur:
            condition_ids = get_condition_ids(cur)

            print("✅ Conditions utilisées:")
            for key, cid in condition_ids.items():
                print(f"   {key}: {cid}")

            cur.execute("""
                TRUNCATE staging.machine_condition_data CASCADE;
            """)
            print("🗑️ staging.machine_condition_data vidée")

            cur.execute("""
                SELECT
                    DATE(date_of_booking) AS production_day,
                    station_id,
                    COUNT(*) AS booking_count
                FROM staging.bookings
                WHERE date_of_booking IS NOT NULL
                  AND station_id IS NOT NULL
                GROUP BY 1, 2
                ORDER BY 1, 2;
            """)

            booking_groups = cur.fetchall()

            if not booking_groups:
                raise RuntimeError("Aucun booking trouvé.")

            print(f"✅ {len(booking_groups)} couples date/station trouvés depuis bookings")

            cur.execute("""
                SELECT COALESCE(MAX(id), 0)
                FROM staging.machine_condition_data;
            """)
            next_id = int(cur.fetchone()[0]) + 1

            inserted = 0

            for production_day, station_id, booking_count in booking_groups:
                segments = generate_segments(production_day)

                for date_from, date_to, condition_key in segments:
                    condition_id = condition_ids[condition_key]
                    color_rgb = CONDITION_COLORS[condition_key]

                    level = "P" if condition_key == "running" else "A"

                    cur.execute("""
                        INSERT INTO staging.machine_condition_data (
                            id,
                            date_from,
                            date_to,
                            station_id,
                            condition_id,
                            level,
                            condition_stamp,
                            condition_type,
                            color_rgb,
                            condition_created,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                    """, (
                        next_id,
                        date_from,
                        date_to,
                        int(station_id),
                        int(condition_id),
                        level,
                        date_to,
                        "s",
                        color_rgb,
                        date_from,
                        date_to,
                    ))

                    next_id += 1
                    inserted += 1

            conn.commit()

            print(f"✅ {inserted} lignes MCD générées depuis bookings")

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()