import json
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from benchmark.models import Airport, Aircraft, Flight, Seat, Passenger, Booking
from pymongo import MongoClient


def get_mongo_db():
    client = MongoClient('mongodb://localhost:27018/')
    return client['airline_benchmark']


# ── Database status ───────────────────────────────────────────────────────────
# Returns current record counts for both databases.
# The dashboard calls this on load and after every seed or clear
# to update the status panel showing how many records exist.

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
        'mysql':   mysql_counts,
        'mongodb': mongo_counts,
    })


# ── Clear databases ───────────────────────────────────────────────────────────
# Called when the user clicks the Clear Databases button.
# Deletion order matters in MySQL because of foreign key constraints.

@csrf_exempt
@require_http_methods(["POST"])
def clear_databases(request):
    results = {}

    try:
        Booking.objects.all().delete()
        Passenger.objects.all().delete()
        Flight.objects.all().delete()
        Seat.objects.all().delete()
        Aircraft.objects.all().delete()
        Airport.objects.all().delete()
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


# ── Seed databases ────────────────────────────────────────────────────────────
# Called when the user clicks Seed Databases after selecting a dataset size.
# Runs the seed script and returns verification counts so the dashboard
# can confirm both databases received identical data.

@csrf_exempt
@require_http_methods(["POST"])
def seed_databases(request):
    try:
        body = json.loads(request.body)
        size = body.get('size', '1k')
    except Exception:
        size = '1k'

    try:
        from seed import run_seed
        result = run_seed(size)
        return JsonResponse({
            'status':  'seeded',
            'message': f'Both databases seeded successfully with {size} dataset',
            'size':    size,
            'mysql':   result['mysql'],
            'mongodb': result['mongodb'],
        })
    except Exception as e:
        return JsonResponse({
            'status':  'error',
            'message': str(e)
        }, status=500)


# ── Add indexes ───────────────────────────────────────────────────────────────
# Called when the user clicks Add Indexes on the dashboard.
# Indexes are added after seeding so they are built on real data.

@csrf_exempt
@require_http_methods(["POST"])
def add_indexes(request):
    try:
        from index_manager import add_indexes as run_add_indexes
        result = run_add_indexes()
        return JsonResponse({
            'status':  'indexes_added',
            'message': 'Indexes added to both databases successfully',
            'mysql':   result['mysql'],
            'mongodb': result['mongodb'],
            'mysql_indexes':  result['mysql_indexes'],
            'mongo_indexes':  result['mongo_indexes'],
        })
    except Exception as e:
        return JsonResponse({
            'status':  'error',
            'message': str(e)
        }, status=500)


# ── Remove indexes ────────────────────────────────────────────────────────────
# Called when the user clicks Remove Indexes.
# Removes indexes but keeps the data so the same dataset
# can be benchmarked with and without indexes for comparison.

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


# ── Check index status ────────────────────────────────────────────────────────
# Called by the dashboard on load and after add/remove index operations
# to show which indexes are currently active on both databases.

@require_http_methods(["GET"])
def index_status(request):
    try:
        from index_manager import check_indexes
        result = check_indexes()
        return JsonResponse({
            'status':  'ok',
            'mysql':   result['mysql'],
            'mongodb': result['mongodb'],
            'mysql_count':  len(result['mysql']),
            'mongo_count':  len(result['mongodb']),
            'indexed': len(result['mysql']) > 0 or len(result['mongodb']) > 0
        })
    except Exception as e:
        return JsonResponse({
            'status':  'error',
            'message': str(e)
        }, status=500)


# ── Run benchmark ─────────────────────────────────────────────────────────────
# Called when the user clicks Run Benchmark.
# The request body contains a list of selected operation keys
# from the dashboard checkboxes, or 'all' to run everything.

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


# ── Get available operations ──────────────────────────────────────────────────
# Called when the dashboard loads to populate the operation
# checkboxes with the correct labels and categories.

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