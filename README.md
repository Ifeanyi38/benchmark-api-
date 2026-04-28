#  Benchmark API

Technology needeed Python, django, mysql, mongoDB, Docker

Backend API for the  database benchmark system a comparative performance study of MySQL and MongoDB across five dataset scales. Built with Django REST Framework, with both databases running in Docker containers.

> **Frontend dashboard:** [benchmark-dashboard](https://github.com/Ifeanyi38/benchmark-dashboard)

---

## Prerequisites

- Python 3.10+
- Docker Desktop
- Git

---

## Installation

```bash
# Clone the repository
git clone https://github.com/Ifeanyi38/benchmark-api-.git
cd benchmark-api-

# Create and activate virtual environment
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux

# Install dependencies
pip install -r requirements.txt
```

---

## Starting the Databases

Both databases run in Docker. Start them before anything else.

```bash
# Start MySQL and MongoDB containers
docker compose up -d

# Verify both containers are running
docker ps
```

You should see:
- `benchmark_mysql` on port **3307**
- `benchmark_mongo` on port **27018**

---

## Running the API

```bash
# Apply migrations (first time only)
python manage.py migrate

# Start the server
python manage.py runserver
```

API runs at `http://127.0.0.1:8000`

> **Important:** The Django server must be restarted before every benchmark run to ensure a clean cache state. Press `Ctrl+C` to stop and run `python manage.py runserver` again before each run.

---

## Project Structure

```
benchmark-api/
├── benchmark_project/      # Django settings and URL config
├── benchmark/              # Main app — models, views, serializers, URLs
├── benchmark_runner.py     # All ten benchmark operations for MySQL and MongoDB
├── seed.py                 # Synthetic data generation and insertion
├── index_manager.py        # Adds and removes indexes for both databases
├── cache_stats.py          # Collects buffer cache hit ratios after benchmarks
├── docker-compose.yml      # MySQL 8.0 and MongoDB 6.0 container config
└── manage.py
```

---

## Benchmark Protocol

Follow this sequence for each dataset size:

```
1. Seed database via dashboard
2. Remove indexes (for no-index condition)
3. Restart Django  →  python manage.py runserver
4. Run benchmark via dashboard  →  export as {size}_no_index.csv
5. Add indexes via dashboard
6. Restart Django  →  python manage.py runserver
7. Run benchmark via dashboard  →  export as {size}_with_index.csv
8. Repeat for next dataset size
```

---

## Cache Stats

Run immediately after each benchmark while Docker is still running:

```bash
python cache_stats.py
```

Enter the dataset size and index condition when prompted. Results print to the terminal.



## Database Config

| | MySQL | MongoDB |
|---|---|---|
| Host | 127.0.0.1 | localhost |
| Port | 3307 | 27018 |
| Database | airline_benchmark | airline_benchmark |
| Username | root | — |
| Password | password | — |

---

## Stopping Docker

```bash
docker compose down
```

---

## Dataset Sizes

| Size | Passengers | Flights | Bookings | Est. Seed Time |
|---|---|---|---|---|
| 1k | 300 | 200 | 1,000 | < 1 min |
| 50k | 8,000 | 5,000 | 50,000 | ~5 mins |
| 500k | 80,000 | 20,000 | 500,000 | ~30 mins |
| 1m | 150,000 | 50,000 | 1,000,000 | ~1 hour |
| 5m | 700,000 | 200,000 | 5,000,000 | ~6 hours |

---

## Connecting to the Dashboard

Start both projects on the same machine. The dashboard expects this API at `http://127.0.0.1:8000` with no additional configuration needed.

---

## License

MIT — developed as part of an MSc Computing dissertation at the University of Roehampton.
