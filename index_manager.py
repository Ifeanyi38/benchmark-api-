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
# Building an index on an empty table is pointless -- the optimizer
# needs data to build an accurate index structure.

def add_mysql_indexes():
    print("Adding MySQL indexes...")
    indexes = [
        # Flight origin and destination are the most frequently filtered
        # columns in search queries -- indexing them gives the biggest
        # performance gain on flight search operations.
        "CREATE INDEX IF NOT EXISTS idx_flight_origin ON benchmark_flight(origin_id);",
        "CREATE INDEX IF NOT EXISTS idx_flight_destination ON benchmark_flight(destination_id);",

        # Status is filtered in almost every query -- scheduled flights
        # for search, confirmed bookings for seat availability checks.
        "CREATE INDEX IF NOT EXISTS idx_flight_status ON benchmark_flight(status);",
        "CREATE INDEX IF NOT EXISTS idx_booking_status ON benchmark_booking(status);",

        # Passenger and flight foreign keys on bookings are used by
        # the view bookings and cancel booking operations.
        "CREATE INDEX IF NOT EXISTS idx_booking_passenger ON benchmark_booking(passenger_id);",
        "CREATE INDEX IF NOT EXISTS idx_booking_flight ON benchmark_booking(flight_id);",

        # Email is the primary way passengers look up their bookings
        # on the dashboard -- this index makes that lookup instant.
        "CREATE INDEX IF NOT EXISTS idx_passenger_email ON benchmark_passenger(email);",

        # Departure time is used in date range search queries.
        "CREATE INDEX IF NOT EXISTS idx_flight_departure ON benchmark_flight(departure_time);",

        # Base price is used in price range filter queries.
        "CREATE INDEX IF NOT EXISTS idx_flight_price ON benchmark_flight(base_price);",
    ]

    with connection.cursor() as cursor:
        for sql in indexes:
            try:
                cursor.execute(sql)
                print(f"  Added: {sql.split('idx_')[1].split(' ')[0]}")
            except Exception as e:
                print(f"  Skipped (already exists): {e}")

    print("MySQL indexes added successfully.")
    return {'database': 'MySQL', 'status': 'indexes_added', 'success': True}


def add_mongo_indexes():
    print("Adding MongoDB indexes...")
    db = get_mongo_db()

    indexes = [
        # Same fields as MySQL for a fair comparison.
        # MongoDB uses compound indexes differently from MySQL --
        # the order of fields in a compound index matters in MongoDB
        # but not as much in MySQL's B-tree implementation.
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
# Indexes are removed when the user wants to run the no-index benchmark.
# We drop indexes but keep the data so the same dataset is used for
# both the indexed and non-indexed benchmarks -- this ensures a fair comparison.

def remove_mysql_indexes():
    print("Removing MySQL indexes...")
    indexes = [
        "DROP INDEX IF EXISTS idx_flight_origin ON benchmark_flight;",
        "DROP INDEX IF EXISTS idx_flight_destination ON benchmark_flight;",
        "DROP INDEX IF EXISTS idx_flight_status ON benchmark_flight;",
        "DROP INDEX IF EXISTS idx_flight_departure ON benchmark_flight;",
        "DROP INDEX IF EXISTS idx_flight_price ON benchmark_flight;",
        "DROP INDEX IF EXISTS idx_booking_status ON benchmark_booking;",
        "DROP INDEX IF EXISTS idx_booking_passenger ON benchmark_booking;",
        "DROP INDEX IF EXISTS idx_booking_flight ON benchmark_booking;",
        "DROP INDEX IF EXISTS idx_passenger_email ON benchmark_passenger;",
    ]

    with connection.cursor() as cursor:
        for sql in indexes:
            try:
                cursor.execute(sql)
                print(f"  Removed: {sql.split('idx_')[1].split(' ')[0]}")
            except Exception as e:
                print(f"  Skipped: {e}")

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
# Returns which indexes currently exist on both databases.
# The dashboard uses this to show whether indexes are active or not.

def get_mysql_indexes():
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT INDEX_NAME, TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = 'airline_benchmark'
            AND INDEX_NAME LIKE 'idx_%'
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
        'status':  'indexes_added',
        'mysql':   mysql_result,
        'mongodb': mongo_result,
        'mysql_indexes':  get_mysql_indexes(),
        'mongo_indexes':  get_mongo_indexes(),
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