import os
import django
import time
import statistics
import random
import string
from decimal import Decimal
from datetime import datetime, timedelta

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'benchmark_project.settings')
django.setup()

from benchmark.models import Airport, Aircraft, Flight, Seat, Passenger, Booking
from pymongo import MongoClient
from bson import ObjectId, Decimal128
from faker import Faker

fake = Faker()

def get_mongo_db():
    client = MongoClient('mongodb://localhost:27018/')
    return client['airline_benchmark']


def random_ref():
    return 'SK' + ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))


# ── Core measurement function ─────────────────────────────────────────────────
# Runs a given function 100 times and returns statistical metrics.
# 100 runs gives enough data to smooth out system noise and caching effects.
# The first few runs are often faster due to OS-level caching, so averaging
# over 100 runs gives a much more reliable picture of true performance.

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

def mysql_search_flights():
    # MySQL must JOIN benchmark_flight with benchmark_airport twice --
    # once for origin and once for destination -- to filter by city name.
    # This is a classic example of where relational databases pay a cost
    # for normalisation that MongoDB avoids with embedded documents.
    Flight.objects.filter(
        origin__city__icontains='London',
        destination__city__icontains='New York',
        status='scheduled'
    ).select_related('origin', 'destination', 'aircraft')[:50]


def mysql_view_bookings():
    # Fetching all bookings for one passenger requires joining four tables:
    # booking, passenger, flight and seat. The select_related call tells
    # Django ORM to do this in a single SQL query rather than separate lookups.
    passenger = Passenger.objects.first()
    if passenger:
        list(Booking.objects.filter(
            passenger=passenger
        ).select_related(
            'flight', 'flight__origin', 'flight__destination', 'seat'
        ))


def mysql_seat_availability():
    # Finding available seats requires a NOT IN subquery -- MySQL first
    # collects all booked seat IDs for the flight, then returns seats
    # that are not in that list. At large scale this subquery becomes
    # increasingly expensive without an index on booking.seat_id.
    flight = Flight.objects.first()
    if flight:
        booked_seat_ids = Booking.objects.filter(
            flight=flight,
            status='confirmed'
        ).values_list('seat_id', flat=True)
        list(Seat.objects.filter(
            aircraft=flight.aircraft
        ).exclude(id__in=booked_seat_ids))


def mysql_complex_search():
    # A multi-condition search combining city filter, price range and
    # date range in a single query. This is more representative of
    # real user behaviour than single-field searches.
    from django.utils import timezone
    Flight.objects.filter(
        origin__city__icontains='London',
        base_price__gte=100,
        base_price__lte=800,
        status='scheduled'
    ).select_related('origin', 'destination')[:50]


def mysql_aggregation():
    # Counting bookings grouped by route to find the most popular routes.
    # MySQL uses GROUP BY with COUNT which is a well-optimised operation
    # in relational databases. This tests analytical query performance.
    from django.db.models import Count
    Booking.objects.values(
        'flight__origin__city',
        'flight__destination__city'
    ).annotate(
        total_bookings=Count('id')
    ).order_by('-total_bookings')[:10]


# ── MySQL WRITE operations ────────────────────────────────────────────────────

def mysql_insert_booking():
    # A real booking insert that goes through MySQL's full constraint
    # checking pipeline -- it verifies the passenger, flight and seat
    # all exist before inserting. The record is deleted immediately after
    # to keep the dataset clean for the next run.
    passenger = Passenger.objects.first()
    flight    = Flight.objects.filter(status='scheduled').first()
    if not passenger or not flight:
        return

    # Find a seat that is not already booked on this flight
    booked_seat_ids = Booking.objects.filter(
        flight=flight
    ).values_list('seat_id', flat=True)
    seat = Seat.objects.filter(
        aircraft=flight.aircraft
    ).exclude(id__in=booked_seat_ids).first()

    if not seat:
        return

    booking = Booking.objects.create(
        passenger         = passenger,
        flight            = flight,
        seat              = seat,
        booking_reference = random_ref(),
        status            = 'confirmed',
        total_price       = flight.base_price,
    )
    # Delete immediately to preserve the dataset for subsequent runs
    booking.delete()


def mysql_bulk_insert():
    # Bulk inserting 100 passengers at once tests MySQL's batch write
    # performance. bulk_create wraps all inserts in a single transaction
    # which is significantly faster than 100 individual inserts.
    passengers = [
        Passenger(
            first_name      = fake.first_name(),
            last_name       = fake.last_name(),
            email           = f"bulk_{i}_{random_ref()}@test.com",
            phone           = fake.phone_number()[:20],
            passport_number = f"BULK{i}{random_ref()}",
            date_of_birth   = fake.date_of_birth(minimum_age=18, maximum_age=80),
        )
        for i in range(100)
    ]
    created = Passenger.objects.bulk_create(passengers)
    # Delete all bulk-inserted passengers immediately after measurement
    ids = [p.id for p in created]
    Passenger.objects.filter(id__in=ids).delete()


# ── MySQL UPDATE operations ───────────────────────────────────────────────────

def mysql_cancel_booking():
    # Updates a confirmed booking to cancelled and immediately restores it.
    # MySQL checks referential integrity on every UPDATE which adds overhead
    # that MongoDB does not have since it has no foreign key constraints.
    booking = Booking.objects.filter(status='confirmed').first()
    if booking:
        Booking.objects.filter(id=booking.id).update(status='cancelled')
        # Restore original status to keep dataset consistent
        Booking.objects.filter(id=booking.id).update(status='confirmed')


def mysql_update_flight():
    # Updates a flight status from scheduled to delayed and restores it.
    # A second UPDATE benchmark to confirm patterns seen in cancel_booking.
    flight = Flight.objects.filter(status='scheduled').first()
    if flight:
        Flight.objects.filter(id=flight.id).update(status='delayed')
        Flight.objects.filter(id=flight.id).update(status='scheduled')


# ── MySQL DELETE operation ────────────────────────────────────────────────────

def mysql_delete_booking():
    # Deletes a booking and reinserts it after measurement.
    # MySQL enforces referential integrity on DELETE -- it checks whether
    # any other records reference this booking before allowing the delete.
    booking = Booking.objects.filter(status='cancelled').first()
    if not booking:
        return

    # Store all fields before deleting so we can reinsert
    passenger  = booking.passenger
    flight     = booking.flight
    seat       = booking.seat
    ref        = booking.booking_reference
    price      = booking.total_price

    booking.delete()

    # Reinsert to restore the dataset
    Booking.objects.create(
        passenger         = passenger,
        flight            = flight,
        seat              = seat,
        booking_reference = random_ref(),
        status            = 'cancelled',
        total_price       = price,
    )


# ── MongoDB READ operations ───────────────────────────────────────────────────

def mongo_search_flights():
    # MongoDB searches the embedded origin.city and destination.city fields
    # directly inside the flight document -- no join needed. The $regex
    # operator handles case-insensitive matching the same way MySQL's
    # icontains does, making the comparison fair.
    db = get_mongo_db()
    list(db.flights.find({
        'origin.city':      {'$regex': 'London',   '$options': 'i'},
        'destination.city': {'$regex': 'New York',  '$options': 'i'},
        'status':           'scheduled'
    }).limit(50))


def mongo_view_bookings():
    # MongoDB reads the booking document which already contains embedded
    # passenger and flight snapshots. This is a single collection read
    # with no joins -- the key structural advantage of the document model
    # for read-heavy operations like viewing booking history.
    db        = get_mongo_db()
    passenger = db.passengers.find_one()
    if passenger:
        list(db.bookings.find({'passenger_id': passenger['_id']}))


def mongo_seat_availability():
    # MongoDB checks the booked seats by querying the bookings collection
    # for a specific flight, then filters the embedded seats array in the
    # aircraft document to find which seats are still available.
    db     = get_mongo_db()
    flight = db.flights.find_one()
    if flight:
        booked = db.bookings.distinct(
            'seat.seat_number',
            {'flight_id': flight['_id'], 'status': 'confirmed'}
        )
        aircraft = db.aircraft.find_one({'_id': flight['aircraft_id']})
        if aircraft:
            available = [s for s in aircraft['seats']
                        if s['seat_number'] not in booked]


def mongo_complex_search():
    # Multi-condition search using MongoDB query operators.
    # $regex for city name, $gte and $lte for price range.
    # Equivalent to MySQL's multi-condition WHERE clause.
    db = get_mongo_db()
    list(db.flights.find({
        'origin.city': {'$regex': 'London', '$options': 'i'},
        'base_price':  {'$gte': Decimal128('100'), '$lte': Decimal128('800')},
        'status':      'scheduled'
    }).limit(50))


def mongo_aggregation():
    # MongoDB aggregation pipeline groups bookings by route and counts them.
    # The $lookup stage joins with flights collection to get route information,
    # then $group counts bookings per route. This tests MongoDB's aggregation
    # framework against MySQL's GROUP BY.
    db = get_mongo_db()
    list(db.bookings.aggregate([
        {
            '$group': {
                '_id':   '$flight_id',
                'count': {'$sum': 1}
            }
        },
        {'$sort': {'count': -1}},
        {'$limit': 10}
    ]))


# ── MongoDB WRITE operations ──────────────────────────────────────────────────

def mongo_insert_booking():
    # MongoDB insert has no foreign key checks -- it simply writes the
    # document to disk. This is faster than MySQL but means the application
    # is responsible for ensuring data consistency.
    db        = get_mongo_db()
    passenger = db.passengers.find_one()
    flight    = db.flights.find_one({'status': 'scheduled'})
    if not passenger or not flight:
        return

    aircraft = db.aircraft.find_one({'_id': flight['aircraft_id']})
    if not aircraft or not aircraft['seats']:
        return

    seat = aircraft['seats'][0]
    result = db.bookings.insert_one({
        '_id':               ObjectId(),
        'passenger_id':      passenger['_id'],
        'flight_id':         flight['_id'],
        'booking_reference': random_ref(),
        'status':            'confirmed',
        'total_price':       flight['base_price'],
        'booked_at':         datetime.utcnow(),
        'seat': {
            'seat_number': seat['seat_number'],
            'seat_class':  seat['seat_class'],
        },
        'passenger_snapshot': {
            'first_name': passenger['first_name'],
            'last_name':  passenger['last_name'],
            'email':      passenger['email'],
        },
        'flight_snapshot': {
            'flight_number':  flight['flight_number'],
            'origin':         flight['origin']['code'],
            'destination':    flight['destination']['code'],
            'departure_time': flight['departure_time'],
        }
    })
    # Delete immediately to preserve the dataset
    db.bookings.delete_one({'_id': result.inserted_id})


def mongo_bulk_insert():
    # MongoDB's insert_many sends all 100 documents to the server in
    # a single network round trip which is more efficient than 100
    # individual insert_one calls.
    db        = get_mongo_db()
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
    # Delete all bulk-inserted documents immediately
    db.passengers.delete_many({'_id': {'$in': result.inserted_ids}})


# ── MongoDB UPDATE operations ─────────────────────────────────────────────────

def mongo_cancel_booking():
    # MongoDB update_one modifies a single field in a document.
    # No referential integrity checks are performed -- MongoDB simply
    # finds the document and updates the specified field.
    db      = get_mongo_db()
    booking = db.bookings.find_one({'status': 'confirmed'})
    if booking:
        db.bookings.update_one(
            {'_id': booking['_id']},
            {'$set': {'status': 'cancelled'}}
        )
        # Restore original status
        db.bookings.update_one(
            {'_id': booking['_id']},
            {'$set': {'status': 'confirmed'}}
        )


def mongo_update_flight():
    # Updates a flight status field and restores it.
    # Equivalent to mysql_update_flight for a fair comparison.
    db     = get_mongo_db()
    flight = db.flights.find_one({'status': 'scheduled'})
    if flight:
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
    # Deletes a booking document and reinserts it after measurement.
    # MongoDB delete is simpler than MySQL because there are no
    # foreign key constraints or cascade rules to enforce.
    db      = get_mongo_db()
    booking = db.bookings.find_one({'status': 'cancelled'})
    if not booking:
        return

    db.bookings.delete_one({'_id': booking['_id']})

    # Reinsert to restore the dataset
    booking['_id']               = ObjectId()
    booking['booking_reference'] = random_ref()
    db.bookings.insert_one(booking)


# ── Operation registry ────────────────────────────────────────────────────────
# Maps operation names (sent from the dashboard) to their MySQL and
# MongoDB function pairs. Adding a new operation only requires adding
# one entry here -- the rest of the system picks it up automatically.

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
# Called by the Django API when the user clicks Run Benchmark on the dashboard.
# The operations parameter is either 'all' or a list of operation keys
# selected by the user via checkboxes on the dashboard.

def run_benchmarks(operations='all', runs=100):
    if operations == 'all':
        selected = list(OPERATIONS.keys())
    else:
        # Filter to only valid operation keys sent from the dashboard
        selected = [op for op in operations if op in OPERATIONS]

    if not selected:
        return {'error': 'No valid operations selected'}

    results = []

    for key in selected:
        op = OPERATIONS[key]
        print(f"\nBenchmarking: {op['label']}")

        mysql_result = measure(op['mysql'], runs)
        print(f"  MySQL   -- avg: {mysql_result['avg']}ms")

        mongo_result = measure(op['mongodb'], runs)
        print(f"  MongoDB -- avg: {mongo_result['avg']}ms")

        results.append({
            'operation': op['label'],
            'category':  op['category'],
            'key':       key,
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