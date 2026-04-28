import mysql.connector
from pymongo import MongoClient

MYSQL_CONFIG = {
    'host':     '127.0.0.1',
    'port':     3307,
    'user':     'root',
    'password': 'password',
    'database': 'airline_benchmark'
}

MONGO_URI = 'mongodb://localhost:27018/'
MONGO_DB  = 'airline_benchmark'


def get_mysql_cache_ratio():
    # MySQL tracks every data request through Innodb_buffer_pool_read_requests.
    # When a request finds its data already in the buffer pool that is a hit.
    # When it has to fetch from disk instead that increments Innodb_buffer_pool_reads.
    # Subtracting disk reads from total requests gives us the hit count.
    try:
        conn   = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT variable_name, variable_value
            FROM performance_schema.global_status
            WHERE variable_name IN (
                'Innodb_buffer_pool_read_requests',
                'Innodb_buffer_pool_reads'
            )
        """)
        rows   = {r[0]: int(r[1]) for r in cursor.fetchall()}
        cursor.close()
        conn.close()

        total_requests = rows.get('Innodb_buffer_pool_read_requests', 0)
        disk_reads     = rows.get('Innodb_buffer_pool_reads', 0)

        if total_requests == 0:
            return None, "No buffer pool activity found. Run the benchmark on the dashboard first."

        hit_ratio = ((total_requests - disk_reads) / total_requests) * 100
        return round(hit_ratio, 2), None

    except Exception as e:
        return None, f"Could not connect to MySQL: {e}"


def get_mongo_cache_ratio():
    # MongoDB uses the WiredTiger storage engine which exposes cache stats
    # through the serverStatus command. pages requested from the cache is the
    # total number of page lookups. pages read into cache is how many of those
    # had to come from disk because they were not already in memory.
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        db     = client[MONGO_DB]
        status = db.command('serverStatus')
        cache  = status.get('wiredTiger', {}).get('cache', {})

        pages_requested = cache.get('pages requested from the cache', 0)
        pages_read      = cache.get('pages read into cache', 0)
        client.close()

        if pages_requested == 0:
            return None, "No cache activity found. Run the benchmark on the dashboard first."

        hit_ratio = ((pages_requested - pages_read) / pages_requested) * 100
        return round(hit_ratio, 2), None

    except Exception as e:
        return None, f"Could not connect to MongoDB: {e}"


if __name__ == '__main__':
    print("=" * 55)
    print("  Buffer Cache Hit Ratio — SkyLine Benchmark")
    print("  Condition: ")
    print("=" * 55)
    print()

    dataset = input("Dataset size just benchmarked (1k / 50k / 500k / 1m / 5m): ").strip()

    print()
    print(f"Reading cache statistics from both databases...")
    print()

    mysql_ratio, mysql_error = get_mysql_cache_ratio()
    mongo_ratio, mongo_error = get_mongo_cache_ratio()

    print("-" * 55)
    print(f"  Dataset:    {dataset}  |  Condition: with_index")
    print("-" * 55)

    if mysql_error:
        print(f"  MySQL   cache hit ratio:  ERROR — {mysql_error}")
    else:
        print(f"  MySQL   cache hit ratio:  {mysql_ratio}%")

    if mongo_error:
        print(f"  MongoDB cache hit ratio:  ERROR — {mongo_error}")
    else:
        print(f"  MongoDB cache hit ratio:  {mongo_ratio}%")

    print("-" * 55)
    print()
    print("  Note down these numbers and run again for the next dataset size.")
    print()