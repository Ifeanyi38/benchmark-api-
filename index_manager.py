import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'benchmark_project.settings')
django.setup()

from django.db import connection
from pymongo import MongoClient


def get_mongo_db():
    client = MongoClient('mongodb://localhost:27018/')
    return client['airline_benchmark']


# ── Add indexes ───────────────────────────────────────────────────────────────
# Indexes are added after seeding so they are built on real data.
# We skip idx_flight_origin, idx_flight_destination and idx_booking_passenger
# because Django already creates these automatically for foreign key columns.
# Adding them again would cause duplicate index errors.

def add_mysql_indexes():
    print("Adding MySQL indexes...")
    indexes = [
        ("idx_flight_status",    "CREATE INDEX idx_flight_status ON benchmark_flight(status);"),
        ("idx_flight_departure", "CREATE INDEX idx_flight_departure ON benchmark_flight(departure_time);"),
        ("idx_flight_price",     "CREATE INDEX idx_flight_price ON benchmark_flight(base_price);"),
        ("idx_booking_status",   "CREATE INDEX idx_booking_status ON benchmark_booking(status);"),
        ("idx_booking_flight",   "CREATE INDEX idx_booking_flight ON benchmark_booking(flight_id);"),
        ("idx_passenger_email",  "CREATE INDEX idx_passenger_email ON benchmark_passenger(email);"),
    ]

    with connection.cursor() as cursor:
        for name, sql in indexes:
            try:
                cursor.execute(sql)
                print(f"  Added: {name}")
            except Exception as e:
                print(f"  Skipped {name}: already exists")

    print("MySQL indexes added successfully.")
    return {'database': 'MySQL', 'status': 'indexes_added', 'success': True}


def add_mongo_indexes():
    print("Adding MongoDB indexes...")
    db = get_mongo_db()

    indexes = [
        (db.flights,    [("origin.city", 1)],       "origin_city"),
        (db.flights,    [("destination.city", 1)],  "destination_city"),
        (db.flights,    [("status", 1)],             "flight_status"),
        (db.flights,    [("departure_time", 1)],     "departure_time"),
        (db.flights,    [("base_price", 1)],         "base_price"),
        (db.bookings,   [("passenger_id", 1)],       "booking_passenger"),
        (db.bookings,   [("flight_id", 1)],          "booking_flight"),
        (db.bookings,   [("status", 1)],             "booking_status"),
        (db.passengers, [("email", 1)],              "passenger_email"),
    ]

    for collection, keys, name in indexes:
        try:
            collection.create_index(keys, name=name)
            print(f"  Added: {name}")
        except Exception as e:
            print(f"  Skipped (already exists): {e}")

    print("MongoDB indexes added successfully.")
    return {'database': 'MongoDB', 'status': 'indexes_added', 'success': True}


# ── Remove indexes ────────────────────────────────────────────────────────────
# We only remove the indexes we created manually.
# Django's automatically created foreign key indexes
# (idx_flight_origin, idx_flight_destination, idx_booking_passenger)
# are not removed because Django needs them for foreign key relationships
# and they will be recreated automatically on migration anyway.

def remove_mysql_indexes():
    print("Removing MySQL indexes...")
    indexes = [
        ("idx_flight_status",    "benchmark_flight"),
        ("idx_flight_departure", "benchmark_flight"),
        ("idx_flight_price",     "benchmark_flight"),
        ("idx_booking_status",   "benchmark_booking"),
        ("idx_booking_flight",   "benchmark_booking"),
        ("idx_passenger_email",  "benchmark_passenger"),
    ]

    with connection.cursor() as cursor:
        for name, table in indexes:
            try:
                cursor.execute(f"DROP INDEX {name} ON {table};")
                print(f"  Removed: {name}")
            except Exception as e:
                print(f"  Skipped {name}: {e}")

    print("MySQL indexes removed.")
    return {'database': 'MySQL', 'status': 'indexes_removed', 'success': True}


def remove_mongo_indexes():
    print("Removing MongoDB indexes...")
    db = get_mongo_db()

    index_names = [
        (db.flights,    "origin_city"),
        (db.flights,    "destination_city"),
        (db.flights,    "flight_status"),
        (db.flights,    "departure_time"),
        (db.flights,    "base_price"),
        (db.bookings,   "booking_passenger"),
        (db.bookings,   "booking_flight"),
        (db.bookings,   "booking_status"),
        (db.passengers, "passenger_email"),
    ]

    for collection, name in index_names:
        try:
            collection.drop_index(name)
            print(f"  Removed: {name}")
        except Exception as e:
            print(f"  Skipped: {e}")

    print("MongoDB indexes removed.")
    return {'database': 'MongoDB', 'status': 'indexes_removed', 'success': True}


# ── Check index status ────────────────────────────────────────────────────────
# Only checks for our custom indexes, not Django's automatic foreign key indexes.
# This gives an accurate picture of whether the user has added indexes or not.

def get_mysql_indexes():
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT INDEX_NAME, TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = 'airline_benchmark'
            AND INDEX_NAME IN (
                'idx_flight_status',
                'idx_flight_departure',
                'idx_flight_price',
                'idx_booking_status',
                'idx_booking_flight',
                'idx_passenger_email'
            )
            ORDER BY TABLE_NAME, INDEX_NAME;
        """)
        rows = cursor.fetchall()
    return [{'index': r[0], 'table': r[1], 'column': r[2]} for r in rows]


def get_mongo_indexes():
    db      = get_mongo_db()
    results = []
    for collection_name in ['flights', 'bookings', 'passengers']:
        collection = db[collection_name]
        for index in collection.list_indexes():
            if index['name'] != '_id_':
                results.append({
                    'index':      index['name'],
                    'collection': collection_name,
                })
    return results


# ── Entry points called by Django API ─────────────────────────────────────────

def add_indexes():
    mysql_result = add_mysql_indexes()
    mongo_result = add_mongo_indexes()
    return {
        'status':        'indexes_added',
        'mysql':         mysql_result,
        'mongodb':       mongo_result,
        'mysql_indexes': get_mysql_indexes(),
        'mongo_indexes': get_mongo_indexes(),
    }


def remove_indexes():
    mysql_result = remove_mysql_indexes()
    mongo_result = remove_mongo_indexes()
    return {
        'status':  'indexes_removed',
        'mysql':   mysql_result,
        'mongodb': mongo_result,
    }


def check_indexes():
    return {
        'mysql':   get_mysql_indexes(),
        'mongodb': get_mongo_indexes(),
    }


# ── Terminal usage ────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys
    action = sys.argv[1] if len(sys.argv) > 1 else 'check'

    if action == 'add':
        result = add_indexes()
        print(f"\nIndexes added successfully")
    elif action == 'remove':
        result = remove_indexes()
        print(f"\nIndexes removed successfully")
    else:
        result = check_indexes()
        print(f"\nMySQL indexes: {result['mysql']}")
        print(f"MongoDB indexes: {result['mongodb']}")