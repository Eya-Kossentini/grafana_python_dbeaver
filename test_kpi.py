"""
Generateur de donnees coherentes pour machine_condition_data et bookings.

- Meme periode pour les deux datasets
- Memes stations
- Diversite maximale des conditions (toutes les conditions du fichier partage)
- Les bookings sont generes UNIQUEMENT pendant les periodes Running / Micro Stop
  (group_id = 6, c'est-a-dire production reelle)
- Permet le calcul coherent des KPI Quality et OEE

Usage:
    python generate_data.py
"""

import random
from datetime import datetime, timedelta, time
import psycopg2

# ============================================================
# PARAMETRES DE GENERATION
# ============================================================

NB_DAYS = 30                # Nombre de jours a generer
NB_STATIONS = 10            # Nombre de stations (1..NB_STATIONS)
START_DATE = datetime(2026, 2, 1)  # Date de debut

# Plage horaire de production par jour
PRODUCTION_START_HOUR = 8
PRODUCTION_END_HOUR = 20    # Production de 08h a 20h = 12h/jour

# Cycle theorique d'un booking (en secondes) - 1 piece toutes les 30 secondes
CYCLE_TIME_SECONDS = 30

# Ratios de qualite des bookings
RATIO_PASS = 0.92
RATIO_FAIL = 0.05
RATIO_SCRAP = 0.03

# Production aussi le weekend ?
INCLUDE_WEEKENDS = False

# Connexion DB (meme config que tes routers)
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5435,
    "dbname": "postgres",
    "user": "postgres",
    "password": "admin123",
}

# Seed pour reproductibilite (mets None pour aleatoire)
RANDOM_SEED = 42


# ============================================================
# CONDITIONS DISPONIBLES (extrait du fichier partage)
# ============================================================
# group_id 6 = Production reelle (Running, Micro Stop, Waiting, etc.)
# group_id 5 = Arrets planifies (Maintenance, Cleaning, Setup, Meeting...)
# group_id 4 = Arrets non planifies (Breakdown, Part Shortage)

CONDITIONS = [
    # group_id 6 = PRODUCTION (les bookings sont generes dans ces blocs)
    {"id": 1,  "group_id": 6, "name": "1000", "desc": "Running",                       "color": "#13be1e", "type": "PRODUCTIVE"},
    {"id": 3,  "group_id": 6, "name": "1200", "desc": "Rate Deviation & Others",       "color": "#8a607b", "type": "PRODUCTIVE"},
    {"id": 17, "group_id": 6, "name": "1001", "desc": "Waiting",                       "color": "#c811aa", "type": "PRODUCTIVE"},
    {"id": 18, "group_id": 6, "name": "1002", "desc": "Cooling phase during molding",  "color": "#d12323", "type": "PRODUCTIVE"},
    {"id": 27, "group_id": 6, "name": "1003", "desc": "Micro Stop",                    "color": "#d6a624", "type": "PRODUCTIVE"},

    # group_id 5 = ARRETS PLANIFIES
    {"id": 2,  "group_id": 5, "name": "5001", "desc": "Cleaning",                      "color": "#099f95", "type": "PLANNED_STOP"},
    {"id": 7,  "group_id": 5, "name": "3100", "desc": "Preventive Maintenance",        "color": "#e6d628", "type": "PLANNED_STOP"},
    {"id": 8,  "group_id": 5, "name": "3000", "desc": "Inventory Check",               "color": "#377764", "type": "PLANNED_STOP"},
    {"id": 11, "group_id": 5, "name": "3003", "desc": "Trial & Pilot Run",             "color": "#3a884e", "type": "PLANNED_STOP"},
    {"id": 12, "group_id": 5, "name": "3004", "desc": "Meeting",                       "color": "#ea7f06", "type": "PLANNED_STOP"},
    {"id": 13, "group_id": 5, "name": "3005", "desc": "No Production & Break",         "color": "#3e6eac", "type": "PLANNED_STOP"},
    {"id": 20, "group_id": 5, "name": "2002", "desc": "Calibration",                   "color": "#54e990", "type": "PLANNED_STOP"},
    {"id": 21, "group_id": 5, "name": "3001", "desc": "Material Check",                "color": "#4daeea", "type": "PLANNED_STOP"},
    {"id": 22, "group_id": 5, "name": "3002", "desc": "Fire Drills",                   "color": "#c56767", "type": "PLANNED_STOP"},
    {"id": 29, "group_id": 5, "name": "6001", "desc": "Setup",                         "color": "#4940c9", "type": "PLANNED_STOP"},

    # group_id 4 = ARRETS NON PLANIFIES
    {"id": 5,  "group_id": 4, "name": "2100", "desc": "Part Shortage",                 "color": "#d7dab9", "type": "UNPLANNED_STOP"},
    {"id": 6,  "group_id": 4, "name": "2000", "desc": "Breakdown",                     "color": "#8b1818", "type": "UNPLANNED_STOP"},
]

# Pre-classement par type
PRODUCTIVE_CONDS = [c for c in CONDITIONS if c["type"] == "PRODUCTIVE"]
PLANNED_STOP_CONDS = [c for c in CONDITIONS if c["type"] == "PLANNED_STOP"]
UNPLANNED_STOP_CONDS = [c for c in CONDITIONS if c["type"] == "UNPLANNED_STOP"]


# ============================================================
# CHARGEMENT DES REFERENTIELS EXISTANTS (respect des FK)
# ============================================================

def load_workorder_ids():
    """Lit les IDs des work_orders directement depuis PostgreSQL."""
    print("Chargement des work_orders depuis la DB...")
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM staging.work_orders;")
            ids = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if not ids:
        raise ValueError("Aucun work_order trouve dans staging.work_orders")

    print(f"  -> {len(ids)} work_orders disponibles (id {min(ids)} a {max(ids)})")
    return ids


def load_failure_type_ids():
    """Lit les IDs des failure_types directement depuis PostgreSQL."""
    print("Chargement des failure_types depuis la DB...")
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT failure_type_id FROM staging.failure_types;")
            ids = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if not ids:
        raise ValueError("Aucun failure_type trouve dans staging.failure_types")

    print(f"  -> {len(ids)} failure_types disponibles (id {min(ids)} a {max(ids)})")
    return ids


def load_serial_numbers_by_workorder():
    """
    Lit les serial_numbers depuis PostgreSQL et retourne un dict
    {workorder_id: [list of serial_number ids]}.
    Permet de piocher un SN coherent avec le workorder du booking.
    """
    print("Chargement des serial_numbers depuis la DB...")
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, workorder_id FROM staging.serial_numbers;")
            rows = cur.fetchall()
    finally:
        conn.close()

    sn_by_wo = {}
    for sn_id, wo_id in rows:
        if wo_id is None:
            continue
        sn_by_wo.setdefault(wo_id, []).append(sn_id)

    if not sn_by_wo:
        raise ValueError("Aucun serial_number trouve dans staging.serial_numbers")

    print(f"  -> {sum(len(v) for v in sn_by_wo.values())} SN repartis sur {len(sn_by_wo)} workorders")
    return sn_by_wo


def load_mesure_ids():
    """Lit les IDs des measurement_data directement depuis PostgreSQL."""
    print("Chargement des measurement_data depuis la DB...")
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM staging.measurement_data;")
            ids = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if not ids:
        print("  -> Aucun measurement_data trouve, mesure_id sera toujours 0")
        return []

    print(f"  -> {len(ids)} measurement_data disponibles (id {min(ids)} a {max(ids)})")
    return ids


# ============================================================
# GENERATEUR
# ============================================================

def pick_condition_type():
    """Choisit le type de prochain bloc selon des probabilites realistes."""
    r = random.random()
    if r < 0.70:
        return "PRODUCTIVE"
    elif r < 0.85:
        return "PLANNED_STOP"
    else:
        return "UNPLANNED_STOP"


def pick_condition_by_type(cond_type: str):
    """Pioche une condition aleatoire dans le pool du type donne."""
    if cond_type == "PRODUCTIVE":
        # Running tres frequent, autres conditions productives moins frequentes
        weights = [0.65, 0.08, 0.10, 0.07, 0.10]
        return random.choices(PRODUCTIVE_CONDS, weights=weights, k=1)[0]
    elif cond_type == "PLANNED_STOP":
        return random.choice(PLANNED_STOP_CONDS)
    else:
        return random.choice(UNPLANNED_STOP_CONDS)


def pick_block_duration(cond_type: str) -> int:
    """Duree d'un bloc en secondes selon son type."""
    if cond_type == "PRODUCTIVE":
        # Bloc productif : 20 min a 2h
        return random.randint(20 * 60, 120 * 60)
    elif cond_type == "PLANNED_STOP":
        # Arret planifie : 5 min a 45 min
        return random.randint(5 * 60, 45 * 60)
    else:
        # Arret non planifie : 2 min a 60 min
        return random.randint(2 * 60, 60 * 60)


def generate_mcd_for_station_day(station_id: int, day: datetime, next_mcd_id: int):
    """
    Genere les blocs MCD pour une station sur une journee.
    Retourne (liste_mcd, liste_blocs_productifs, prochain_id).
    Les blocs productifs sont retournes pour generer les bookings dedans.
    """
    mcds = []
    productive_blocks = []  # (date_from, date_to) ou les bookings peuvent etre generes

    current = day.replace(hour=PRODUCTION_START_HOUR, minute=0, second=0, microsecond=0)
    end_of_day = day.replace(hour=PRODUCTION_END_HOUR, minute=0, second=0, microsecond=0)

    while current < end_of_day:
        cond_type = pick_condition_type()
        condition = pick_condition_by_type(cond_type)
        duration_sec = pick_block_duration(cond_type)

        date_from = current
        date_to = min(current + timedelta(seconds=duration_sec), end_of_day)

        mcd = {
            "id": next_mcd_id,
            "date_from": date_from,
            "date_to": date_to,
            "station_id": station_id,
            "condition_id": condition["id"],
            "level": "INFO",
            "condition_stamp": date_from,
            "condition_type": condition["desc"],
            "color_rgb": condition["color"],
            "condition_created": date_from,
        }
        mcds.append(mcd)
        next_mcd_id += 1

        if cond_type == "PRODUCTIVE" and condition["name"] in ("1000", "1003"):
            # Bookings generes pendant Running et Micro Stop uniquement
            productive_blocks.append((date_from, date_to))

        current = date_to

    return mcds, productive_blocks, next_mcd_id


def generate_bookings_for_block(
    station_id: int,
    date_from: datetime,
    date_to: datetime,
    next_booking_id: int,
    workorder_id: int,
    available_failure_type_ids: list,
    sn_by_workorder: dict,
    available_mesure_ids: list,
):
    """Genere des bookings reparties uniformement dans un bloc productif."""
    bookings = []
    duration_sec = (date_to - date_from).total_seconds()
    nb_bookings = max(1, int(duration_sec / CYCLE_TIME_SECONDS))

    # SN disponibles pour ce workorder (peut etre vide si WO sans SN)
    sn_pool = sn_by_workorder.get(workorder_id, [])

    # Type de booking du bloc : SNR (tracking unitaire) ou batch (par lot)
    # Si pas de SN disponibles pour le WO, force batch (pas de tracking unitaire possible)
    if not sn_pool:
        block_type = "batch"
    else:
        block_type = random.choices(["SNR", "batch"], weights=[0.75, 0.25], k=1)[0]

    for i in range(nb_bookings):
        offset_sec = (i + 0.5) * CYCLE_TIME_SECONDS + random.uniform(-5, 5)
        offset_sec = max(0, min(offset_sec, duration_sec))
        booking_date = date_from + timedelta(seconds=offset_sec)

        # Tirage du state
        r = random.random()
        if r < RATIO_PASS:
            state = "pass"
        elif r < RATIO_PASS + RATIO_FAIL:
            state = "fail"
        else:
            state = "scrap"

        # failed_id : toujours sur scrap, parfois sur fail (~60%), jamais sur pass
        if state == "scrap":
            failed_id = random.choice(available_failure_type_ids)
        elif state == "fail":
            failed_id = random.choice(available_failure_type_ids) if random.random() < 0.6 else None
        else:
            failed_id = None

        # serial_number_id et process_layer dependent du type
        if block_type == "SNR":
            # Mode SNR : tracking unitaire
            serial_number_id = random.choice(sn_pool) if random.random() < 0.7 else None
            process_layer = 0 if state in ("fail", "scrap") else None
        else:
            # Mode batch : pas de tracking unitaire, pas de SN, pas de process_layer
            serial_number_id = None
            process_layer = None

        # mesure_id : majoritairement 0, parfois un vrai id (surtout sur fail/scrap)
        if state == "scrap" and available_mesure_ids and random.random() < 0.3:
            mesure_id = random.choice(available_mesure_ids)
        elif state == "fail" and available_mesure_ids and random.random() < 0.15:
            mesure_id = random.choice(available_mesure_ids)
        else:
            mesure_id = 0

        # booked_by : majoritairement "Admin", parfois un email d'operateur
        booked_by = random.choices(
            ["Admin", "Admin_SMT@ift.com", "operator1@ift.com", "operator2@ift.com", "supervisor@ift.com"],
            weights=[0.6, 0.15, 0.1, 0.1, 0.05],
            k=1,
        )[0]

        bookings.append({
            "id": next_booking_id,
            "workorder_id": workorder_id,
            "station_id": station_id,
            "failed_id": failed_id,
            "serial_number_id": serial_number_id,
            "process_layer": process_layer,
            "date_of_booking": booking_date,
            "state": state,
            "mesure_id": mesure_id,
            "real_cycle_time": 0,
            "type": block_type,
            "snr_booking": True,
            "booked_by": booked_by,
            "quantity": None,
        })
        next_booking_id += 1

    return bookings, next_booking_id


def generate_all():
    """Boucle principale : genere tout le dataset."""
    if RANDOM_SEED is not None:
        random.seed(RANDOM_SEED)

    # Chargement des referentiels existants (respect FK)
    available_workorder_ids = load_workorder_ids()
    available_failure_type_ids = load_failure_type_ids()
    sn_by_workorder = load_serial_numbers_by_workorder()
    available_mesure_ids = load_mesure_ids()

    all_mcds = []
    all_bookings = []
    next_mcd_id = 1
    next_booking_id = 1

    for day_offset in range(NB_DAYS):
        day = START_DATE + timedelta(days=day_offset)

        # Skip weekends si demande
        if not INCLUDE_WEEKENDS and day.weekday() >= 5:
            continue

        for station_id in range(1, NB_STATIONS + 1):
            mcds, productive_blocks, next_mcd_id = generate_mcd_for_station_day(
                station_id, day, next_mcd_id
            )
            all_mcds.extend(mcds)

            for block_from, block_to in productive_blocks:
                # Pioche un workorder_id reel pour ce bloc productif
                workorder_id = random.choice(available_workorder_ids)
                bookings, next_booking_id = generate_bookings_for_block(
                    station_id, block_from, block_to,
                    next_booking_id, workorder_id,
                    available_failure_type_ids,
                    sn_by_workorder,
                    available_mesure_ids,
                )
                all_bookings.extend(bookings)

    return all_mcds, all_bookings


# ============================================================
# INSERTION EN BASE
# ============================================================

def insert_into_db(mcds, bookings):
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            print(f"Insertion de {len(mcds)} lignes dans staging.machine_condition_data...")
            cur.executemany(
                """
                INSERT INTO staging.machine_condition_data
                    (id, date_from, date_to, station_id, condition_id, level,
                     condition_stamp, condition_type, color_rgb, condition_created)
                VALUES (%(id)s, %(date_from)s, %(date_to)s, %(station_id)s,
                        %(condition_id)s, %(level)s, %(condition_stamp)s,
                        %(condition_type)s, %(color_rgb)s, %(condition_created)s)
                ON CONFLICT (id) DO NOTHING;
                """,
                mcds,
            )

            print(f"Insertion de {len(bookings)} lignes dans staging.bookings...")
            cur.executemany(
                """
                INSERT INTO staging.bookings
                    (id, workorder_id, station_id, failed_id, serial_number_id,
                     process_layer, date_of_booking, state, mesure_id, real_cycle_time,
                     type, snr_booking, booked_by)
                VALUES (%(id)s, %(workorder_id)s, %(station_id)s, %(failed_id)s,
                        %(serial_number_id)s, %(process_layer)s, %(date_of_booking)s,
                        %(state)s, %(mesure_id)s, %(real_cycle_time)s, %(type)s,
                        %(snr_booking)s, %(booked_by)s)
                ON CONFLICT (id) DO NOTHING;
                """,
                bookings,
            )

        conn.commit()
        print("Insertion terminee avec succes.")
    except Exception as e:
        conn.rollback()
        print(f"Erreur lors de l'insertion : {e}")
        raise
    finally:
        conn.close()


# ============================================================
# STATS POUR VERIFICATION
# ============================================================

def print_stats(mcds, bookings):
    from collections import Counter

    print("\n" + "=" * 60)
    print("STATISTIQUES DE GENERATION")
    print("=" * 60)
    print(f"Periode        : {START_DATE.date()} -> {(START_DATE + timedelta(days=NB_DAYS)).date()}")
    print(f"Stations       : 1 -> {NB_STATIONS}")
    print(f"MCD lignes     : {len(mcds)}")
    print(f"Bookings       : {len(bookings)}")

    cond_counter = Counter(m["condition_type"] for m in mcds)
    print(f"\nDiversite des conditions ({len(cond_counter)} types differents) :")
    for cond, n in sorted(cond_counter.items(), key=lambda x: -x[1]):
        print(f"  - {cond:<35} {n:>6} blocs")

    state_counter = Counter(b["state"] for b in bookings)
    total_bk = len(bookings)
    print(f"\nRepartition des states (bookings) :")
    for state, n in sorted(state_counter.items(), key=lambda x: -x[1]):
        pct = 100 * n / total_bk if total_bk else 0
        print(f"  - {state:<10} {n:>6}  ({pct:.1f}%)")

    print("=" * 60)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("Generation des donnees...")
    mcds, bookings = generate_all()
    print_stats(mcds, bookings)

    # Decommenter pour inserer en base :
    insert_into_db(mcds, bookings)

