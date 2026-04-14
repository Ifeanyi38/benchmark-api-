import os
import django
import time
import statistics
import random
import string
from decimal import Decimal
from datetime import datetime, timedelta
import traceback

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'benchmark_project.settings')
django.setup()

import mysql.connector
from mysql.connector import pooling
from pymongo import MongoClient
from bson import ObjectId, Decimal128
from faker import Faker

fake = Faker()

# ── Connection setup ──────────────────────────────────────────────────────────
# Connections are created lazily on first use rather than at module load time.
# This prevents connection timeout errors when Django restarts after the
# database has been idle for a long period between benchmark sessions.

_mysql_pool   = None
_mongo_client = None
_mongo_db     = None


def get_mysql_conn():
    global _mysql_pool
    if _mysql_pool is None:
        _mysql_pool = pooling.MySQLConnectionPool(
            pool_name    = 'benchmark_pool',
            pool_size    = 5,
            host         = '127.0.0.1',
            port         = 3307,
            user         = 'root',
            password     = 'password',
            database     = 'airline_benchmark'
        )
    return _mysql_pool.get_connection()


def get_mongo_db():
    global _mongo_client, _mongo_db
    if _mongo_client is None:
        _mongo_client = MongoClient('mongodb://localhost:27018/')
        _mongo_db     = _mongo_client['airline_benchmark']
    return _mongo_db


def random_ref():
    return 'SK' + ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))


# ── Benchmark data cache ──────────────────────────────────────────────────────
# 20 sample IDs are loaded before benchmarking starts and reused across all
# runs. This is not the dataset size -- it is just a pool of realistic test
# subjects to rotate through during the 100 benchmark runs per operation.
# The actual queries still run against the full dataset in both databases.
# Only passengers that have bookings are cached so view_bookings always
# returns real results rather than empty sets.

_cache = {}

def load_cache():
    conn   = get_mysql_conn()
    cursor = conn.cursor()

    # MySQL -- select 20 random passengers that have at least one booking.
    # ORDER BY RAND() LIMIT 20 restricts at database level so this works
    # at any dataset size without hitting memory limits.
    cursor.execute("""
        SELECT DISTINCT b.passenger_id
        FROM benchmark_booking b
        ORDER BY RAND()
        LIMIT 20
    """)
    _cache['mysql_passenger_ids'] = [r[0] for r in cursor.fetchall()]

    cursor.execute("""
        SELECT id, aircraft_id
        FROM benchmark_flight
        WHERE status = 'scheduled'
        ORDER BY RAND()
        LIMIT 20
    """)
    _cache['mysql_flight_rows'] = cursor.fetchall()

    cursor.execute("""
        SELECT id FROM benchmark_booking
        WHERE status = 'confirmed'
        ORDER BY RAND()
        LIMIT 1
    """)
    row = cursor.fetchone()
    _cache['mysql_confirmed_booking_id'] = row[0] if row else None

    cursor.execute("""
        SELECT id, passenger_id, flight_id, seat_id, total_price
        FROM benchmark_booking
        WHERE status = 'cancelled'
        ORDER BY RAND()
        LIMIT 1
    """)
    row = cursor.fetchone()
    _cache['mysql_cancelled_booking'] = row if row else None

    cursor.execute("""
        SELECT DISTINCT a.city FROM benchmark_airport a
        JOIN benchmark_flight f ON f.origin_id = a.id
        LIMIT 20
    """)
    _cache['origin_cities'] = [r[0] for r in cursor.fetchall()]

    cursor.execute("""
        SELECT DISTINCT a.city FROM benchmark_airport a
        JOIN benchmark_flight f ON f.destination_id = a.id
        LIMIT 20
    """)
    _cache['destination_cities'] = [r[0] for r in cursor.fetchall()]

    cursor.close()
    conn.close()

    db = get_mongo_db()

    # MongoDB -- use aggregation $sample instead of distinct to avoid the
    # 16MB cap that distinct hits when there are millions of unique passenger
    # IDs. $sample picks 20 random bookings and we extract their passenger IDs.
    # This works identically to the MySQL approach at all dataset sizes.
    sample_bookings = list(db.bookings.aggregate([
        {'$match': {'status': {'$in': ['confirmed', 'cancelled']}}},
        {'$sample': {'size': 20}},
        {'$project': {'passenger_id': 1}}
    ]))
    _cache['mongo_passenger_ids'] = [b['passenger_id'] for b in sample_bookings]

    flight_docs = list(db.flights.aggregate([
        {'$match': {'status': 'scheduled'}},
        {'$sample': {'size': 20}}
    ]))
    _cache['mongo_flight_docs'] = flight_docs

    booking_confirmed = db.bookings.find_one({'status': 'confirmed'})
    _cache['mongo_confirmed_booking_id'] = booking_confirmed['_id'] if booking_confirmed else None

    booking_cancelled = db.bookings.find_one({'status': 'cancelled'})
    _cache['mongo_cancelled_booking'] = booking_cancelled if booking_cancelled else None

    print("Benchmark cache loaded successfully.")
    print(f"  MySQL  -- {len(_cache['mysql_passenger_ids'])} passengers with bookings cached")
    print(f"  MongoDB -- {len(_cache['mongo_passenger_ids'])} passengers with bookings cached")


# ── Core measurement function ─────────────────────────────────────────────────
# Runs a given function N times and returns statistical metrics.
# 100 runs smooths out OS scheduling noise and database caching effects.
# The timer wraps only the query execution -- cache loading happens before
# timing begins so it does not inflate the benchmark measurements.

def measure(func, runs=100):
    times = []
    for _ in range(runs):
        start = time.perf_counter()
        func()
        end   = time.perf_counter()
        times.append((end - start) * 1000)

    return {
        'min':   round(min(times), 3),
        'max':   round(max(times), 3),
        'avg':   round(sum(times) / len(times), 3),
        'stdev': round(statistics.stdev(times), 3),
        'runs':  runs
    }


# ── MySQL READ operations ─────────────────────────────────────────────────────
# All MySQL operations use raw SQL via mysql-connector-python with a
# connection pool. This eliminates Django ORM overhead and ensures results
# reflect actual MySQL engine performance rather than framework performance.

def mysql_search_flights():
    # Two JOINs required to get city names stored in a separate airports table.
    # Uses random cities from cache to ensure results are realistic across runs.
    origin_city = random.choice(_cache.get('origin_cities', ['London']))
    dest_city   = random.choice(_cache.get('destination_cities', ['New York']))
    conn        = get_mysql_conn()
    cursor      = conn.cursor()
    cursor.execute("""
        SELECT f.flight_number, a1.city AS origin_city,
               a2.city AS destination_city,
               f.departure_time, f.base_price, f.status
        FROM benchmark_flight f
        JOIN benchmark_airport a1 ON f.origin_id = a1.id
        JOIN benchmark_airport a2 ON f.destination_id = a2.id
        WHERE a1.city LIKE %s
        AND   a2.city LIKE %s
        AND   f.status = 'scheduled'
        LIMIT 50
    """, (f'%{origin_city}%', f'%{dest_city}%'))
    cursor.fetchall()
    cursor.close()
    conn.close()


def mysql_view_bookings():
    # Fetches all bookings for one passenger across four joined tables.
    # Random passenger from cache ensures varied results across runs.
    passenger_id = random.choice(_cache.get('mysql_passenger_ids', [1]))
    conn         = get_mysql_conn()
    cursor       = conn.cursor()
    cursor.execute("""
        SELECT b.booking_reference, b.status, b.total_price,
               p.first_name, p.last_name, p.email,
               f.flight_number, f.departure_time,
               a1.city AS origin, a2.city AS destination,
               s.seat_number, s.seat_class
        FROM benchmark_booking b
        JOIN benchmark_passenger p  ON b.passenger_id = p.id
        JOIN benchmark_flight f     ON b.flight_id = f.id
        JOIN benchmark_airport a1   ON f.origin_id = a1.id
        JOIN benchmark_airport a2   ON f.destination_id = a2.id
        JOIN benchmark_seat s       ON b.seat_id = s.id
        WHERE b.passenger_id = %s
    """, (passenger_id,))
    cursor.fetchall()
    cursor.close()
    conn.close()


def mysql_seat_availability():
    # NOT IN subquery finds seats not yet booked on a specific flight.
    # Random flight from cache ensures varied results across runs.
    row         = random.choice(_cache.get('mysql_flight_rows', [(1, 1)]))
    flight_id   = row[0]
    aircraft_id = row[1]
    conn        = get_mysql_conn()
    cursor      = conn.cursor()
    cursor.execute("""
        SELECT s.seat_number, s.seat_class
        FROM benchmark_seat s
        WHERE s.aircraft_id = %s
        AND s.id NOT IN (
            SELECT b.seat_id FROM benchmark_booking b
            WHERE b.flight_id = %s
            AND   b.status = 'confirmed'
        )
    """, (aircraft_id, flight_id))
    cursor.fetchall()
    cursor.close()
    conn.close()


def mysql_complex_search():
    # Multi-condition search with city filter, price range and status.
    # Same city source and price range as mongo_complex_search for fairness.
    origin_city = random.choice(_cache.get('origin_cities', ['London']))
    conn        = get_mysql_conn()
    cursor      = conn.cursor()
    cursor.execute("""
        SELECT f.flight_number, a1.city AS origin,
               a2.city AS destination,
               f.base_price, f.departure_time
        FROM benchmark_flight f
        JOIN benchmark_airport a1 ON f.origin_id = a1.id
        JOIN benchmark_airport a2 ON f.destination_id = a2.id
        WHERE a1.city LIKE %s
        AND   f.base_price BETWEEN %s AND %s
        AND   f.status = 'scheduled'
        ORDER BY f.base_price ASC
        LIMIT 50
    """, (f'%{origin_city}%', 100, 800))
    cursor.fetchall()
    cursor.close()
    conn.close()


def mysql_aggregation():
    # Full aggregation across all bookings joined with flights and airports.
    # Most expensive operation -- requires scanning entire bookings table
    # and joining with three other tables before grouping.
    conn   = get_mysql_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT a1.city AS origin, a2.city AS destination,
               COUNT(b.id) AS total_bookings
        FROM benchmark_booking b
        JOIN benchmark_flight f   ON b.flight_id = f.id
        JOIN benchmark_airport a1 ON f.origin_id = a1.id
        JOIN benchmark_airport a2 ON f.destination_id = a2.id
        GROUP BY a1.city, a2.city
        ORDER BY total_bookings DESC
        LIMIT 10
    """)
    cursor.fetchall()
    cursor.close()
    conn.close()


# ── MySQL WRITE operations ────────────────────────────────────────────────────

def mysql_insert_booking():
    # Inserts a real booking then deletes it immediately after measurement
    # to keep the dataset consistent across all 100 runs.
    conn         = get_mysql_conn()
    cursor       = conn.cursor()
    passenger_id = random.choice(_cache.get('mysql_passenger_ids', [1]))
    row          = random.choice(_cache.get('mysql_flight_rows', [(1, 1)]))
    flight_id    = row[0]
    aircraft_id  = row[1]

    cursor.execute("""
        SELECT s.id FROM benchmark_seat s
        WHERE s.aircraft_id = %s
        AND s.id NOT IN (
            SELECT seat_id FROM benchmark_booking WHERE flight_id = %s
        )
        LIMIT 1
    """, (aircraft_id, flight_id))
    seat = cursor.fetchone()

    if seat:
        ref = random_ref()
        cursor.execute("""
            INSERT INTO benchmark_booking
            (passenger_id, flight_id, seat_id, booking_reference,
             status, total_price, booked_at)
            VALUES (%s, %s, %s, %s, 'confirmed', %s, NOW())
        """, (passenger_id, flight_id, seat[0], ref,
              round(random.uniform(49, 1500), 2)))
        conn.commit()
        booking_id = cursor.lastrowid
        cursor.execute(
            "DELETE FROM benchmark_booking WHERE id = %s",
            (booking_id,)
        )
        conn.commit()

    cursor.close()
    conn.close()


def mysql_bulk_insert():
    # Inserts 100 passengers at once using executemany then deletes them.
    # executemany sends all inserts in a single round trip to the server.
    conn   = get_mysql_conn()
    cursor = conn.cursor()

    passengers = [
        (fake.first_name(), fake.last_name(),
         f"bulk_{i}_{random_ref()}@test.com",
         fake.phone_number()[:20],
         f"BULK{i}{random_ref()}",
         fake.date_of_birth(minimum_age=18, maximum_age=80).strftime('%Y-%m-%d'))
        for i in range(100)
    ]

    cursor.executemany("""
        INSERT INTO benchmark_passenger
        (first_name, last_name, email, phone, passport_number, date_of_birth)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, passengers)
    conn.commit()
    cursor.execute(
        "DELETE FROM benchmark_passenger WHERE email LIKE 'bulk_%@test.com'"
    )
    conn.commit()
    cursor.close()
    conn.close()


# ── MySQL UPDATE operations ───────────────────────────────────────────────────

def mysql_cancel_booking():
    # Updates status to cancelled then restores it -- dataset stays consistent.
    booking_id = _cache.get('mysql_confirmed_booking_id')
    if not booking_id:
        return
    conn   = get_mysql_conn()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE benchmark_booking SET status = 'cancelled' WHERE id = %s",
        (booking_id,)
    )
    conn.commit()
    cursor.execute(
        "UPDATE benchmark_booking SET status = 'confirmed' WHERE id = %s",
        (booking_id,)
    )
    conn.commit()
    cursor.close()
    conn.close()


def mysql_update_flight():
    # Updates status to delayed then restores it.
    row       = random.choice(_cache.get('mysql_flight_rows', [(1, 1)]))
    flight_id = row[0]
    conn      = get_mysql_conn()
    cursor    = conn.cursor()
    cursor.execute(
        "UPDATE benchmark_flight SET status = 'delayed' WHERE id = %s",
        (flight_id,)
    )
    conn.commit()
    cursor.execute(
        "UPDATE benchmark_flight SET status = 'scheduled' WHERE id = %s",
        (flight_id,)
    )
    conn.commit()
    cursor.close()
    conn.close()


# ── MySQL DELETE operation ────────────────────────────────────────────────────

def mysql_delete_booking():
    # Deletes a booking and reinserts it after measurement to keep
    # the dataset consistent for the next run.
    row = _cache.get('mysql_cancelled_booking')
    if not row:
        return
    booking_id, passenger_id, flight_id, seat_id, total_price = row
    conn   = get_mysql_conn()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM benchmark_booking WHERE id = %s",
        (booking_id,)
    )
    conn.commit()
    cursor.execute("""
        INSERT INTO benchmark_booking
        (passenger_id, flight_id, seat_id, booking_reference,
         status, total_price, booked_at)
        VALUES (%s, %s, %s, %s, 'cancelled', %s, NOW())
    """, (passenger_id, flight_id, seat_id, random_ref(), total_price))
    conn.commit()
    cursor.execute(
        "SELECT id FROM benchmark_booking ORDER BY id DESC LIMIT 1"
    )
    new_row = cursor.fetchone()
    if new_row:
        _cache['mysql_cancelled_booking'] = (
            new_row[0], passenger_id, flight_id, seat_id, total_price
        )
    cursor.close()
    conn.close()


# ── MongoDB READ operations ───────────────────────────────────────────────────
# All MongoDB operations use pymongo directly -- no ORM layer.
# This ensures a fair comparison with MySQL which also uses direct drivers.

def mongo_search_flights():
    # Searches embedded origin.city and destination.city fields directly
    # inside the flight document -- no join needed unlike MySQL.
    origin_city = random.choice(_cache.get('origin_cities', ['London']))
    dest_city   = random.choice(_cache.get('destination_cities', ['New York']))
    db          = get_mongo_db()
    list(db.flights.find({
        'origin.city':      {'$regex': origin_city, '$options': 'i'},
        'destination.city': {'$regex': dest_city,   '$options': 'i'},
        'status':           'scheduled'
    }).limit(50))


def mongo_view_bookings():
    # Single collection read using embedded passenger snapshot --
    # no join needed because booking documents contain passenger data.
    if not _cache.get('mongo_passenger_ids'):
        return
    passenger_id = random.choice(_cache['mongo_passenger_ids'])
    db           = get_mongo_db()
    list(db.bookings.find({'passenger_id': passenger_id}))


def mongo_seat_availability():
    # Finds booked seats then filters the embedded seats array in the
    # aircraft document -- no separate seats table needed.
    flight = random.choice(_cache.get('mongo_flight_docs', [{}]))
    if not flight:
        return
    db     = get_mongo_db()
    booked = db.bookings.distinct(
        'seat.seat_number',
        {'flight_id': flight['_id'], 'status': 'confirmed'}
    )
    aircraft = db.aircraft.find_one({'_id': flight['aircraft_id']})
    if aircraft:
        [s for s in aircraft['seats'] if s['seat_number'] not in booked]


def mongo_complex_search():
    # Multi-condition search using same city and price range as MySQL
    # for a fair comparison across identical filter conditions.
    origin_city = random.choice(_cache.get('origin_cities', ['London']))
    db          = get_mongo_db()
    list(db.flights.find({
        'origin.city':  {'$regex': origin_city, '$options': 'i'},
        'base_price':   {'$gte': Decimal128('100'), '$lte': Decimal128('800')},
        'status':       'scheduled'
    }).sort('base_price', 1).limit(50))


def mongo_aggregation():
    # Aggregation pipeline uses embedded flight_snapshot.origin and
    # destination fields stored inside each booking document.
    # No join needed -- all route data is already in the booking document.
    db = get_mongo_db()
    list(db.bookings.aggregate([
        {
            '$group': {
                '_id': {
                    'origin':      '$flight_snapshot.origin',
                    'destination': '$flight_snapshot.destination'
                },
                'count': {'$sum': 1}
            }
        },
        {'$sort':  {'count': -1}},
        {'$limit': 10}
    ]))


# ── MongoDB WRITE operations ──────────────────────────────────────────────────

def mongo_insert_booking():
    # Inserts a real booking document then deletes it immediately.
    # No foreign key checks -- MongoDB simply writes the document.
    if not _cache.get('mongo_passenger_ids') or not _cache.get('mongo_flight_docs'):
        return
    passenger_id = random.choice(_cache['mongo_passenger_ids'])
    flight       = random.choice(_cache['mongo_flight_docs'])
    db           = get_mongo_db()
    aircraft     = db.aircraft.find_one({'_id': flight['aircraft_id']})
    if not aircraft or not aircraft['seats']:
        return

    seat   = aircraft['seats'][0]
    result = db.bookings.insert_one({
        '_id':               ObjectId(),
        'passenger_id':      passenger_id,
        'flight_id':         flight['_id'],
        'booking_reference': random_ref(),
        'status':            'confirmed',
        'total_price':       flight.get('base_price', Decimal128('100')),
        'booked_at':         datetime.utcnow(),
        'seat': {
            'seat_number': seat['seat_number'],
            'seat_class':  seat['seat_class'],
        },
        'passenger_snapshot': {
            'first_name': 'Benchmark',
            'last_name':  'Test',
            'email':      'benchmark@test.com',
        },
        'flight_snapshot': {
            'flight_number':  flight.get('flight_number', 'SK000001'),
            'origin':         flight['origin']['code'],
            'destination':    flight['destination']['code'],
            'departure_time': flight.get('departure_time', datetime.utcnow()),
        }
    })
    db.bookings.delete_one({'_id': result.inserted_id})


def mongo_bulk_insert():
    # Inserts 100 passenger documents using insert_many in a single
    # network round trip then deletes them immediately.
    db         = get_mongo_db()
    passengers = [
        {
            '_id':             ObjectId(),
            'first_name':      fake.first_name(),
            'last_name':       fake.last_name(),
            'email':           f"bulk_{i}_{random_ref()}@test.com",
            'phone':           fake.phone_number()[:20],
            'passport_number': f"BULK{i}{random_ref()}",
            'date_of_birth':   datetime.utcnow(),
        }
        for i in range(100)
    ]
    result = db.passengers.insert_many(passengers)
    db.passengers.delete_many({'_id': {'$in': result.inserted_ids}})


# ── MongoDB UPDATE operations ─────────────────────────────────────────────────

def mongo_cancel_booking():
    # Updates status field then restores it -- no referential integrity checks.
    booking_id = _cache.get('mongo_confirmed_booking_id')
    if not booking_id:
        return
    db = get_mongo_db()
    db.bookings.update_one(
        {'_id': booking_id},
        {'$set': {'status': 'cancelled'}}
    )
    db.bookings.update_one(
        {'_id': booking_id},
        {'$set': {'status': 'confirmed'}}
    )


def mongo_update_flight():
    # Updates flight status then restores it.
    flight = random.choice(_cache.get('mongo_flight_docs', [{}]))
    if not flight:
        return
    db = get_mongo_db()
    db.flights.update_one(
        {'_id': flight['_id']},
        {'$set': {'status': 'delayed'}}
    )
    db.flights.update_one(
        {'_id': flight['_id']},
        {'$set': {'status': 'scheduled'}}
    )


# ── MongoDB DELETE operation ──────────────────────────────────────────────────

def mongo_delete_booking():
    # Deletes a booking and reinserts it after measurement.
    # No cascade rules or constraint checks -- simpler than MySQL delete.
    booking = _cache.get('mongo_cancelled_booking')
    if not booking:
        return
    db = get_mongo_db()
    db.bookings.delete_one({'_id': booking['_id']})
    booking['_id']               = ObjectId()
    booking['booking_reference'] = random_ref()
    db.bookings.insert_one(booking)
    _cache['mongo_cancelled_booking'] = booking


# ── Operation registry ────────────────────────────────────────────────────────

OPERATIONS = {
    'search_flights': {
        'label':    'Search flights',
        'category': 'READ',
        'mysql':    mysql_search_flights,
        'mongodb':  mongo_search_flights,
    },
    'view_bookings': {
        'label':    'View bookings',
        'category': 'READ',
        'mysql':    mysql_view_bookings,
        'mongodb':  mongo_view_bookings,
    },
    'seat_availability': {
        'label':    'Seat availability',
        'category': 'READ',
        'mysql':    mysql_seat_availability,
        'mongodb':  mongo_seat_availability,
    },
    'complex_search': {
        'label':    'Complex search',
        'category': 'READ',
        'mysql':    mysql_complex_search,
        'mongodb':  mongo_complex_search,
    },
    'aggregation': {
        'label':    'Aggregation',
        'category': 'READ',
        'mysql':    mysql_aggregation,
        'mongodb':  mongo_aggregation,
    },
    'insert_booking': {
        'label':    'Insert booking',
        'category': 'WRITE',
        'mysql':    mysql_insert_booking,
        'mongodb':  mongo_insert_booking,
    },
    'bulk_insert': {
        'label':    'Bulk insert',
        'category': 'WRITE',
        'mysql':    mysql_bulk_insert,
        'mongodb':  mongo_bulk_insert,
    },
    'cancel_booking': {
        'label':    'Cancel booking',
        'category': 'UPDATE',
        'mysql':    mysql_cancel_booking,
        'mongodb':  mongo_cancel_booking,
    },
    'update_flight': {
        'label':    'Update flight status',
        'category': 'UPDATE',
        'mysql':    mysql_update_flight,
        'mongodb':  mongo_update_flight,
    },
    'delete_booking': {
        'label':    'Delete booking',
        'category': 'DELETE',
        'mysql':    mysql_delete_booking,
        'mongodb':  mongo_delete_booking,
    },
}


# ── Main benchmark runner ─────────────────────────────────────────────────────

def run_benchmarks(operations='all', runs=100):
    load_cache()

    # Aggregation is benchmarked with 10 runs because each run involves
    # a full dataset scan with multiple joins. Both databases use the same
    # reduced run count so the comparison remains fair.
    heavy_operations = ['aggregation']

    if operations == 'all':
        selected = list(OPERATIONS.keys())
    else:
        selected = [op for op in operations if op in OPERATIONS]

    if not selected:
        return {'error': 'No valid operations selected'}

    results = []

    for key in selected:
        op      = OPERATIONS[key]
        op_runs = 10 if key in heavy_operations else runs
        print(f"\nBenchmarking: {op['label']} ({op_runs} runs)")

        try:
            mysql_result = measure(op['mysql'], op_runs)
            print(f"  MySQL   -- avg: {mysql_result['avg']}ms")
        except Exception as e:
            print(f"  MySQL   -- ERROR: {e}")
            traceback.print_exc()
            raise

        try:
            mongo_result = measure(op['mongodb'], op_runs)
            print(f"  MongoDB -- avg: {mongo_result['avg']}ms")
        except Exception as e:
            print(f"  MongoDB -- ERROR: {e}")
            traceback.print_exc()
            raise

        results.append({
            'operation': op['label'],
            'category':  op['category'],
            'key':       key,
            'runs_used': op_runs,
            'mysql': {
                'avg':   mysql_result['avg'],
                'min':   mysql_result['min'],
                'max':   mysql_result['max'],
                'stdev': mysql_result['stdev'],
            },
            'mongodb': {
                'avg':   mongo_result['avg'],
                'min':   mongo_result['min'],
                'max':   mongo_result['max'],
                'stdev': mongo_result['stdev'],
            },
            'faster': 'MySQL' if mysql_result['avg'] < mongo_result['avg'] else 'MongoDB'
        })

    return results


# ── Terminal usage ────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys
    ops = sys.argv[1:] if len(sys.argv) > 1 else 'all'
    print(f"Running benchmarks: {ops}")
    results = run_benchmarks(ops)
    print(f"\nResults:")
    for r in results:
        print(f"\n{r['operation']} ({r['category']})")
        print(f"  MySQL   -- avg: {r['mysql']['avg']}ms | min: {r['mysql']['min']}ms | max: {r['mysql']['max']}ms")
        print(f"  MongoDB -- avg: {r['mongodb']['avg']}ms | min: {r['mongodb']['min']}ms | max: {r['mongodb']['max']}ms")
        print(f"  Faster: {r['faster']}")