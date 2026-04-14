import json
from django.db import connection
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from benchmark.models import Airport, Aircraft, Flight, Seat, Passenger, Booking
from pymongo import MongoClient

# Global flag to stop seeding if clear is clicked mid-seed
seeding_active = False


def get_mongo_db():
    client = MongoClient('mongodb://localhost:27018/')
    return client['airline_benchmark']


@require_http_methods(["GET"])
def db_status(request):
    try:
        mysql_counts = {
            'airports':   Airport.objects.count(),
            'aircraft':   Aircraft.objects.count(),
            'flights':    Flight.objects.count(),
            'passengers': Passenger.objects.count(),
            'bookings':   Booking.objects.count(),
        }
    except Exception as e:
        mysql_counts = {'error': str(e)}

    try:
        db = get_mongo_db()
        mongo_counts = {
            'airports':   db.airports.count_documents({}),
            'aircraft':   db.aircraft.count_documents({}),
            'flights':    db.flights.count_documents({}),
            'passengers': db.passengers.count_documents({}),
            'bookings':   db.bookings.count_documents({}),
        }
    except Exception as e:
        mongo_counts = {'error': str(e)}

    return JsonResponse({
        'mysql':          mysql_counts,
        'mongodb':        mongo_counts,
        'seeding_active': seeding_active,
    })


@csrf_exempt
@require_http_methods(["POST"])
def clear_databases(request):
    global seeding_active
    seeding_active = False

    results = {}

    try:
        from index_manager import remove_mysql_indexes
        remove_mysql_indexes()
    except Exception:
        pass

    # TRUNCATE is instant regardless of record count.
    # SET FOREIGN_KEY_CHECKS = 0 disables constraint validation
    # temporarily so tables can be truncated in any order without
    # foreign key errors.
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            cursor.execute("TRUNCATE TABLE benchmark_booking;")
            cursor.execute("TRUNCATE TABLE benchmark_passenger;")
            cursor.execute("TRUNCATE TABLE benchmark_flight;")
            cursor.execute("TRUNCATE TABLE benchmark_seat;")
            cursor.execute("TRUNCATE TABLE benchmark_aircraft;")
            cursor.execute("TRUNCATE TABLE benchmark_airport;")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        results['mysql'] = {
            'status':  'cleared',
            'success': True,
            'message': 'All MySQL tables cleared successfully'
        }
    except Exception as e:
        results['mysql'] = {
            'status':  'error',
            'success': False,
            'message': str(e)
        }

    try:
        db = get_mongo_db()
        db.bookings.drop()
        db.passengers.drop()
        db.flights.drop()
        db.aircraft.drop()
        db.airports.drop()
        results['mongodb'] = {
            'status':  'cleared',
            'success': True,
            'message': 'All MongoDB collections cleared successfully'
        }
    except Exception as e:
        results['mongodb'] = {
            'status':  'error',
            'success': False,
            'message': str(e)
        }

    both_success = results['mysql']['success'] and results['mongodb']['success']
    results['status']  = 'cleared' if both_success else 'partial_error'
    results['message'] = 'Both databases cleared successfully' if both_success else 'One or more databases failed to clear'

    return JsonResponse(results)


@csrf_exempt
@require_http_methods(["POST"])
def seed_databases(request):
    global seeding_active

    if seeding_active:
        return JsonResponse({
            'status':  'error',
            'message': 'Seeding is already in progress. Please wait or clear the databases first.'
        }, status=400)

    try:
        body = json.loads(request.body)
        size = body.get('size', '1k')
    except Exception:
        size = '1k'

    try:
        seeding_active = True
        from seed import run_seed
        result = run_seed(size)
        seeding_active = False
        return JsonResponse({
            'status':  'seeded',
            'message': f'Both databases seeded successfully with {size} dataset',
            'size':    size,
            'mysql':   result['mysql'],
            'mongodb': result['mongodb'],
        })
    except Exception as e:
        seeding_active = False
        return JsonResponse({
            'status':  'error',
            'message': str(e)
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def add_indexes(request):
    try:
        from index_manager import add_indexes as run_add_indexes
        result = run_add_indexes()
        return JsonResponse({
            'status':         'indexes_added',
            'message':        'Indexes added to both databases successfully',
            'mysql':          result['mysql'],
            'mongodb':        result['mongodb'],
            'mysql_indexes':  result['mysql_indexes'],
            'mongo_indexes':  result['mongo_indexes'],
        })
    except Exception as e:
        return JsonResponse({
            'status':  'error',
            'message': str(e)
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def remove_indexes(request):
    try:
        from index_manager import remove_indexes as run_remove_indexes
        result = run_remove_indexes()
        return JsonResponse({
            'status':  'indexes_removed',
            'message': 'Indexes removed from both databases successfully',
            'mysql':   result['mysql'],
            'mongodb': result['mongodb'],
        })
    except Exception as e:
        return JsonResponse({
            'status':  'error',
            'message': str(e)
        }, status=500)


@require_http_methods(["GET"])
def index_status(request):
    try:
        from index_manager import check_indexes
        result = check_indexes()
        return JsonResponse({
            'status':      'ok',
            'mysql':       result['mysql'],
            'mongodb':     result['mongodb'],
            'mysql_count': len(result['mysql']),
            'mongo_count': len(result['mongodb']),
            'indexed':     len(result['mysql']) > 0 or len(result['mongodb']) > 0
        })
    except Exception as e:
        return JsonResponse({
            'status':  'error',
            'message': str(e)
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def run_benchmark(request):
    try:
        body       = json.loads(request.body)
        operations = body.get('operations', 'all')
        runs       = body.get('runs', 100)
    except Exception:
        operations = 'all'
        runs       = 100

    try:
        from benchmark_runner import run_benchmarks
        results = run_benchmarks(operations, runs)
        return JsonResponse({
            'status':     'completed',
            'message':    'Benchmark completed successfully',
            'operations': len(results),
            'results':    results,
        })
    except Exception as e:
        return JsonResponse({
            'status':  'error',
            'message': str(e)
        }, status=500)


@require_http_methods(["GET"])
def get_operations(request):
    from benchmark_runner import OPERATIONS
    ops = [
        {
            'key':      key,
            'label':    op['label'],
            'category': op['category'],
        }
        for key, op in OPERATIONS.items()
    ]
    return JsonResponse({'operations': ops})