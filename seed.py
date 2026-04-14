import os
import django
import random
import string
from datetime import timedelta, datetime
from decimal import Decimal

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'benchmark_project.settings')
django.setup()

from faker import Faker
from benchmark.models import Airport, Aircraft, Flight, Seat, Passenger, Booking
from pymongo import MongoClient
from bson import ObjectId, Decimal128

fake = Faker()

DATASET_SIZES = {
    '1k': {
        'passengers': 300,
        'flights':    200,
        'bookings':   1000,
    },
    '50k': {
        'passengers': 8000,
        'flights':    5000,
        'bookings':   50000,
    },
    '500k': {
        'passengers': 80000,
        'flights':    20000,
        'bookings':   500000,
    },
    '1m': {
        'passengers': 150000,
        'flights':    50000,
        'bookings':   1000000,
    },
    '5m': {
        'passengers': 700000,
        'flights':    200000,
        'bookings':   5000000,
    },
    '10m': {
        'passengers': 1200000,
        'flights':    400000,
        'bookings':   10000000,
    },
}

AIRPORTS = [
    ("LHR", "Heathrow",              "London",        "UK"),
    ("JFK", "John F. Kennedy",       "New York",      "USA"),
    ("DXB", "Dubai Intl",            "Dubai",         "UAE"),
    ("CDG", "Charles de Gaulle",     "Paris",         "France"),
    ("SIN", "Changi",                "Singapore",     "Singapore"),
    ("NRT", "Narita",                "Tokyo",         "Japan"),
    ("LAX", "Los Angeles Intl",      "Los Angeles",   "USA"),
    ("SYD", "Kingsford Smith",       "Sydney",        "Australia"),
    ("CPT", "Cape Town Intl",        "Cape Town",     "South Africa"),
    ("ABV", "Nnamdi Azikiwe",        "Abuja",         "Nigeria"),
    ("ORD", "O'Hare Intl",           "Chicago",       "USA"),
    ("FRA", "Frankfurt Intl",        "Frankfurt",     "Germany"),
    ("AMS", "Schiphol",              "Amsterdam",     "Netherlands"),
    ("MAD", "Barajas",               "Madrid",        "Spain"),
    ("BCN", "El Prat",               "Barcelona",     "Spain"),
    ("FCO", "Fiumicino",             "Rome",          "Italy"),
    ("MXP", "Malpensa",              "Milan",         "Italy"),
    ("MUC", "Munich Intl",           "Munich",        "Germany"),
    ("ZRH", "Zurich Intl",           "Zurich",        "Switzerland"),
    ("VIE", "Vienna Intl",           "Vienna",        "Austria"),
    ("BRU", "Brussels Intl",         "Brussels",      "Belgium"),
    ("ARN", "Arlanda",               "Stockholm",     "Sweden"),
    ("OSL", "Gardermoen",            "Oslo",          "Norway"),
    ("CPH", "Kastrup",               "Copenhagen",    "Denmark"),
    ("HEL", "Helsinki Intl",         "Helsinki",      "Finland"),
    ("LIS", "Humberto Delgado",      "Lisbon",        "Portugal"),
    ("ATH", "Eleftherios Venizelos", "Athens",        "Greece"),
    ("IST", "Istanbul Intl",         "Istanbul",      "Turkey"),
    ("DOH", "Hamad Intl",            "Doha",          "Qatar"),
    ("AUH", "Zayed Intl",            "Abu Dhabi",     "UAE"),
    ("KWI", "Kuwait Intl",           "Kuwait City",   "Kuwait"),
    ("RUH", "King Khalid Intl",      "Riyadh",        "Saudi Arabia"),
    ("JED", "King Abdulaziz Intl",   "Jeddah",        "Saudi Arabia"),
    ("BOM", "Chhatrapati Shivaji",   "Mumbai",        "India"),
    ("DEL", "Indira Gandhi Intl",    "New Delhi",     "India"),
    ("BLR", "Kempegowda Intl",       "Bangalore",     "India"),
    ("HKG", "Hong Kong Intl",        "Hong Kong",     "China"),
    ("PEK", "Beijing Capital",       "Beijing",       "China"),
    ("PVG", "Pudong Intl",           "Shanghai",      "China"),
    ("ICN", "Incheon Intl",          "Seoul",         "South Korea"),
    ("KUL", "Kuala Lumpur Intl",     "Kuala Lumpur",  "Malaysia"),
    ("BKK", "Suvarnabhumi",          "Bangkok",       "Thailand"),
    ("CGK", "Soekarno-Hatta",        "Jakarta",       "Indonesia"),
    ("MNL", "Ninoy Aquino Intl",     "Manila",        "Philippines"),
    ("MEL", "Melbourne Intl",        "Melbourne",     "Australia"),
    ("AKL", "Auckland Intl",         "Auckland",      "New Zealand"),
    ("YYZ", "Pearson Intl",          "Toronto",       "Canada"),
    ("YVR", "Vancouver Intl",        "Vancouver",     "Canada"),
    ("GRU", "Guarulhos Intl",        "Sao Paulo",     "Brazil"),
    ("EZE", "Ministro Pistarini",    "Buenos Aires",  "Argentina"),
    ("BOG", "El Dorado Intl",        "Bogota",        "Colombia"),
    ("LIM", "Jorge Chavez Intl",     "Lima",          "Peru"),
    ("SCL", "Arturo Merino",         "Santiago",      "Chile"),
    ("MEX", "Benito Juarez Intl",    "Mexico City",   "Mexico"),
    ("MIA", "Miami Intl",            "Miami",         "USA"),
    ("SFO", "San Francisco Intl",    "San Francisco", "USA"),
    ("BOS", "Logan Intl",            "Boston",        "USA"),
    ("IAD", "Dulles Intl",           "Washington DC", "USA"),
    ("ATL", "Hartsfield-Jackson",    "Atlanta",       "USA"),
    ("DFW", "Dallas Fort Worth",     "Dallas",        "USA"),
    ("CAI", "Cairo Intl",            "Cairo",         "Egypt"),
    ("NBO", "Jomo Kenyatta Intl",    "Nairobi",       "Kenya"),
    ("JNB", "OR Tambo Intl",         "Johannesburg",  "South Africa"),
    ("ACC", "Kotoka Intl",           "Accra",         "Ghana"),
    ("LOS", "Murtala Muhammed",      "Lagos",         "Nigeria"),
    ("ADD", "Bole Intl",             "Addis Ababa",   "Ethiopia"),
    ("CMN", "Mohammed V Intl",       "Casablanca",    "Morocco"),
    ("TUN", "Tunis Carthage",        "Tunis",         "Tunisia"),
    ("ALG", "Houari Boumediene",     "Algiers",       "Algeria"),
    ("DAR", "Julius Nyerere Intl",   "Dar es Salaam", "Tanzania"),
    ("KGL", "Kigali Intl",           "Kigali",        "Rwanda"),
    ("LFW", "Gnassingbe Intl",       "Lome",          "Togo"),
    ("COO", "Cadjehoun Intl",        "Cotonou",       "Benin"),
    ("OUA", "Ouagadougou Intl",      "Ouagadougou",   "Burkina Faso"),
    ("BKO", "Modibo Keita Intl",     "Bamako",        "Mali"),
    ("DKR", "Blaise Diagne Intl",    "Dakar",         "Senegal"),
    ("ABJ", "Felix Houphouet-Boigny","Abidjan",       "Ivory Coast"),
    ("DLA", "Douala Intl",           "Douala",        "Cameroon"),
    ("LBV", "Leon M'ba Intl",        "Libreville",    "Gabon"),
    ("BZV", "Maya-Maya Intl",        "Brazzaville",   "Congo"),
    ("FIH", "N'djili Intl",          "Kinshasa",      "DR Congo"),
    ("LUN", "Kenneth Kaunda Intl",   "Lusaka",        "Zambia"),
    ("HRE", "Robert Gabriel Mugabe", "Harare",        "Zimbabwe"),
    ("MRU", "Sir Seewoosagur",       "Mauritius",     "Mauritius"),
    ("SEZ", "Seychelles Intl",       "Mahe",          "Seychelles"),
    ("TAS", "Islam Karimov Intl",    "Tashkent",      "Uzbekistan"),
    ("ALA", "Almaty Intl",           "Almaty",        "Kazakhstan"),
    ("GYD", "Heydar Aliyev Intl",    "Baku",          "Azerbaijan"),
    ("TBS", "Shota Rustaveli Intl",  "Tbilisi",       "Georgia"),
    ("EVN", "Zvartnots Intl",        "Yerevan",       "Armenia"),
    ("AMM", "Queen Alia Intl",       "Amman",         "Jordan"),
    ("BEY", "Rafic Hariri Intl",     "Beirut",        "Lebanon"),
    ("TLV", "Ben Gurion Intl",       "Tel Aviv",      "Israel"),
    ("MCT", "Muscat Intl",           "Muscat",        "Oman"),
    ("BAH", "Bahrain Intl",          "Manama",        "Bahrain"),
    ("KHI", "Jinnah Intl",           "Karachi",       "Pakistan"),
    ("LHE", "Allama Iqbal Intl",     "Lahore",        "Pakistan"),
    ("CMB", "Bandaranaike Intl",     "Colombo",       "Sri Lanka"),
    ("DAC", "Hazrat Shahjalal Intl", "Dhaka",         "Bangladesh"),
    ("RGN", "Yangon Intl",           "Yangon",        "Myanmar"),
]

AIRCRAFT_MODELS = [
    ("Boeing 737-800",       189),
    ("Boeing 737 MAX 8",     178),
    ("Boeing 737 MAX 10",    230),
    ("Boeing 777-300ER",     396),
    ("Boeing 777X",          426),
    ("Boeing 787-9",         296),
    ("Boeing 787-10",        336),
    ("Boeing 747-400",       524),
    ("Boeing 747-8",         605),
    ("Boeing 767-300ER",     218),
    ("Airbus A320neo",       165),
    ("Airbus A321neo",       194),
    ("Airbus A321XLR",       220),
    ("Airbus A330-300",      335),
    ("Airbus A330neo",       287),
    ("Airbus A350-900",      315),
    ("Airbus A350-1000",     369),
    ("Airbus A380-800",      555),
    ("Embraer E195-E2",      146),
    ("Bombardier CRJ-900",   90),
]

BATCH_SIZE = 5000


def random_ref():
    return 'SK' + ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))


def generate_seats_mysql(aircraft, total_seats):
    rows  = total_seats // 6
    seats = []
    for row in range(1, rows + 1):
        for col in ['A', 'B', 'C', 'D', 'E', 'F']:
            if row <= 2:
                seat_class = 'first'
            elif row <= int(rows * 0.2):
                seat_class = 'business'
            else:
                seat_class = 'economy'
            seats.append(Seat(
                aircraft    = aircraft,
                seat_number = f"{row}{col}",
                seat_class  = seat_class,
            ))
    Seat.objects.bulk_create(seats, ignore_conflicts=True)


# ── Clear functions ───────────────────────────────────────────────────────────

def clear_mysql():
    print("Clearing MySQL...")
    from django.db import connection
    # TRUNCATE is instant regardless of record count.
    # SET FOREIGN_KEY_CHECKS = 0 disables constraint validation temporarily
    # so all tables can be truncated in any order without foreign key errors.
    with connection.cursor() as cursor:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute("TRUNCATE TABLE benchmark_booking;")
        cursor.execute("TRUNCATE TABLE benchmark_passenger;")
        cursor.execute("TRUNCATE TABLE benchmark_flight;")
        cursor.execute("TRUNCATE TABLE benchmark_seat;")
        cursor.execute("TRUNCATE TABLE benchmark_aircraft;")
        cursor.execute("TRUNCATE TABLE benchmark_airport;")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    print("MySQL cleared successfully.")
    return {'database': 'MySQL', 'status': 'cleared', 'success': True}


def clear_mongo():
    print("Clearing MongoDB...")
    client = MongoClient('mongodb://localhost:27018/')
    db     = client['airline_benchmark']
    db.bookings.drop()
    db.passengers.drop()
    db.flights.drop()
    db.aircraft.drop()
    db.airports.drop()
    print("MongoDB cleared successfully.")
    return {'database': 'MongoDB', 'status': 'cleared', 'success': True}


# ── MySQL seed ────────────────────────────────────────────────────────────────

def seed_mysql(num_passengers, num_flights, num_bookings):
    print(f"\nSeeding MySQL -- {num_passengers} passengers, {num_flights} flights, {num_bookings} bookings")

    # Airports
    airport_objs = []
    for code, name, city, country in AIRPORTS:
        obj, _ = Airport.objects.get_or_create(
            code=code,
            defaults={'name': name, 'city': city, 'country': country}
        )
        airport_objs.append(obj)
    print(f"  {len(airport_objs)} airports ready")

    # Aircraft and seats
    aircraft_objs = []
    for model, total in AIRCRAFT_MODELS:
        ac, created = Aircraft.objects.get_or_create(
            model=model, defaults={'total_seats': total}
        )
        aircraft_objs.append(ac)
        if created:
            generate_seats_mysql(ac, total)
    print(f"  {len(aircraft_objs)} aircraft ready")

    # Passengers
    print(f"  Creating {num_passengers} passengers...")
    passenger_batch = []
    for i in range(num_passengers):
        passenger_batch.append(Passenger(
            first_name      = fake.first_name(),
            last_name       = fake.last_name(),
            email           = f"{fake.user_name()}_{i}@benchmark.com",
            phone           = fake.phone_number()[:20],
            passport_number = f"PP{i:09d}",
            date_of_birth   = fake.date_of_birth(minimum_age=18, maximum_age=80),
        ))
        if len(passenger_batch) >= BATCH_SIZE:
            Passenger.objects.bulk_create(passenger_batch, ignore_conflicts=True)
            passenger_batch = []
            print(f"    {i + 1} passengers inserted...")
    if passenger_batch:
        Passenger.objects.bulk_create(passenger_batch, ignore_conflicts=True)
    all_passengers = list(Passenger.objects.values_list('id', flat=True))
    print(f"  {len(all_passengers)} passengers created")

    # Flights
    print(f"  Creating {num_flights} flights...")
    flight_batch = []
    for i in range(num_flights):
        origin, destination = random.sample(airport_objs, 2)
        aircraft  = random.choice(aircraft_objs)
        departure = fake.date_time_between(start_date='+1d', end_date='+365d')
        duration  = timedelta(hours=random.randint(1, 16))

        flight_batch.append(Flight(
            flight_number  = f"SK{i:06d}",
            aircraft       = aircraft,
            origin         = origin,
            destination    = destination,
            departure_time = departure,
            arrival_time   = departure + duration,
            base_price     = Decimal(str(round(random.uniform(49, 1500), 2))),
            status         = 'scheduled',
        ))
        if len(flight_batch) >= BATCH_SIZE:
            Flight.objects.bulk_create(flight_batch, ignore_conflicts=True)
            flight_batch = []
            print(f"    {i + 1} flights inserted...")
    if flight_batch:
        Flight.objects.bulk_create(flight_batch, ignore_conflicts=True)
    all_flights = list(Flight.objects.values_list('id', 'aircraft_id'))
    print(f"  {len(all_flights)} flights created")

    # Build seat map in memory
    seat_map = {}
    for seat in Seat.objects.values('id', 'aircraft_id'):
        ac_id = seat['aircraft_id']
        if ac_id not in seat_map:
            seat_map[ac_id] = []
        seat_map[ac_id].append(seat['id'])

    # Bookings
    print(f"  Creating {num_bookings} bookings...")
    booking_batch = []
    used_refs     = set()
    used_seats    = set()
    booked        = 0

    for _ in range(num_bookings):
        flight_id, aircraft_id = random.choice(all_flights)
        passenger_id = random.choice(all_passengers)
        seats        = seat_map.get(aircraft_id, [])
        available    = [s for s in seats if (flight_id, s) not in used_seats]

        if not available:
            continue

        seat_id = random.choice(available)
        ref     = random_ref()
        while ref in used_refs:
            ref = random_ref()

        used_refs.add(ref)
        used_seats.add((flight_id, seat_id))

        booking_batch.append(Booking(
            passenger_id      = passenger_id,
            flight_id         = flight_id,
            seat_id           = seat_id,
            booking_reference = ref,
            status            = random.choice(['confirmed', 'confirmed', 'confirmed', 'cancelled']),
            total_price       = Decimal(str(round(random.uniform(49, 1500), 2))),
        ))
        booked += 1

        if len(booking_batch) >= BATCH_SIZE:
            Booking.objects.bulk_create(booking_batch, ignore_conflicts=True)
            booking_batch = []
            print(f"    {booked} bookings inserted...")

    if booking_batch:
        Booking.objects.bulk_create(booking_batch, ignore_conflicts=True)

    print(f"  {booked} bookings created")
    print("MySQL seeding complete.")
    return booked


# ── MongoDB seed ──────────────────────────────────────────────────────────────

def seed_mongo(num_passengers, num_flights, num_bookings):
    print(f"\nSeeding MongoDB -- {num_passengers} passengers, {num_flights} flights, {num_bookings} bookings")

    client = MongoClient('mongodb://localhost:27018/')
    db     = client['airline_benchmark']

    # Airports
    airport_docs = []
    for code, name, city, country in AIRPORTS:
        doc = {
            "_id":     ObjectId(),
            "code":    code,
            "name":    name,
            "city":    city,
            "country": country
        }
        airport_docs.append(doc)
    db.airports.insert_many(airport_docs)
    print(f"  {len(airport_docs)} airports created")

    # Aircraft with embedded seats
    aircraft_docs = []
    for model, total in AIRCRAFT_MODELS:
        rows  = total // 6
        seats = []
        for row in range(1, rows + 1):
            for col in ['A', 'B', 'C', 'D', 'E', 'F']:
                if row <= 2:
                    seat_class = 'first'
                elif row <= int(rows * 0.2):
                    seat_class = 'business'
                else:
                    seat_class = 'economy'
                seats.append({
                    'seat_number': f"{row}{col}",
                    'seat_class':  seat_class
                })
        doc = {
            "_id":         ObjectId(),
            "model":       model,
            "total_seats": total,
            "seats":       seats
        }
        aircraft_docs.append(doc)
    db.aircraft.insert_many(aircraft_docs)
    print(f"  {len(aircraft_docs)} aircraft created")

    # Flights
    print(f"  Creating {num_flights} flights...")
    flight_docs = []
    for i in range(num_flights):
        origin      = random.choice(airport_docs)
        destination = random.choice([a for a in airport_docs if a != origin])
        aircraft    = random.choice(aircraft_docs)
        departure   = fake.date_time_between(start_date='+1d', end_date='+365d')
        duration    = timedelta(hours=random.randint(1, 16))

        flight_docs.append({
            "_id":            ObjectId(),
            "flight_number":  f"SK{i:06d}",
            "aircraft_id":    aircraft["_id"],
            "origin": {
                "code":    origin["code"],
                "city":    origin["city"],
                "country": origin["country"]
            },
            "destination": {
                "code":    destination["code"],
                "city":    destination["city"],
                "country": destination["country"]
            },
            "departure_time": departure,
            "arrival_time":   departure + duration,
            "base_price":     Decimal128(str(round(random.uniform(49, 1500), 2))),
            "status":         "scheduled",
        })

        if len(flight_docs) >= BATCH_SIZE:
            db.flights.insert_many(flight_docs)
            flight_docs = []
            print(f"    {i + 1} flights inserted...")

    if flight_docs:
        db.flights.insert_many(flight_docs)

    all_flight_docs = list(db.flights.find(
        {},
        {'_id': 1, 'aircraft_id': 1, 'flight_number': 1,
         'origin': 1, 'destination': 1, 'departure_time': 1, 'base_price': 1}
    ))
    print(f"  {len(all_flight_docs)} flights created")

    # Passengers
    print(f"  Creating {num_passengers} passengers...")
    passenger_docs    = []
    all_passenger_ids = []
    for i in range(num_passengers):
        doc = {
            "_id":             ObjectId(),
            "first_name":      fake.first_name(),
            "last_name":       fake.last_name(),
            "email":           f"{fake.user_name()}_{i}@benchmark.com",
            "phone":           fake.phone_number()[:20],
            "passport_number": f"PP{i:09d}",
            "date_of_birth":   datetime.combine(
                fake.date_of_birth(minimum_age=18, maximum_age=80),
                datetime.min.time()
            ),
        }
        passenger_docs.append(doc)
        all_passenger_ids.append(doc['_id'])

        if len(passenger_docs) >= BATCH_SIZE:
            db.passengers.insert_many(passenger_docs)
            passenger_docs = []
            print(f"    {i + 1} passengers inserted...")

    if passenger_docs:
        db.passengers.insert_many(passenger_docs)
    print(f"  {len(all_passenger_ids)} passengers created")

    # Build aircraft seat map
    aircraft_seat_map = {a['_id']: a['seats'] for a in aircraft_docs}

    # Bookings
    print(f"  Creating {num_bookings} bookings...")
    booking_docs = []
    used_refs    = set()
    used_seats   = set()
    booked       = 0

    for _ in range(num_bookings):
        flight       = random.choice(all_flight_docs)
        passenger_id = random.choice(all_passenger_ids)
        seats        = aircraft_seat_map.get(flight['aircraft_id'], [])
        available    = [
            s for s in seats
            if (str(flight['_id']), s['seat_number']) not in used_seats
        ]

        if not available:
            continue

        seat = random.choice(available)
        ref  = random_ref()
        while ref in used_refs:
            ref = random_ref()

        used_refs.add(ref)
        used_seats.add((str(flight['_id']), seat['seat_number']))

        booking_docs.append({
            "_id":               ObjectId(),
            "passenger_id":      passenger_id,
            "flight_id":         flight['_id'],
            "booking_reference": ref,
            "status":            random.choice(['confirmed', 'confirmed', 'confirmed', 'cancelled']),
            "total_price":       flight['base_price'],
            "booked_at":         datetime.utcnow(),
            "seat": {
                "seat_number": seat['seat_number'],
                "seat_class":  seat['seat_class'],
            },
            "passenger_snapshot": {
                "first_name":      fake.first_name(),
                "last_name":       fake.last_name(),
                "email":           f"snapshot_{booked}@benchmark.com",
                "passport_number": f"PP{booked:09d}",
            },
            "flight_snapshot": {
                "flight_number":  flight['flight_number'],
                "origin":         flight['origin']['code'],
                "destination":    flight['destination']['code'],
                "departure_time": flight['departure_time'],
            },
        })
        booked += 1

        if len(booking_docs) >= BATCH_SIZE:
            db.bookings.insert_many(booking_docs)
            booking_docs = []
            print(f"    {booked} bookings inserted...")

    if booking_docs:
        db.bookings.insert_many(booking_docs)

    print(f"  {booked} bookings created")
    print("MongoDB seeding complete.")
    return booked


# ── Main entry point ──────────────────────────────────────────────────────────

def run_seed(size='1k'):
    if size not in DATASET_SIZES:
        raise ValueError(f"Invalid size '{size}'. Valid options are: {list(DATASET_SIZES.keys())}")

    config         = DATASET_SIZES[size]
    num_passengers = config['passengers']
    num_flights    = config['flights']
    num_bookings   = config['bookings']

    fake.unique.clear()
    random.seed(42)

    print(f"\nStarting seed for dataset size: {size}")
    print(f"Target: {num_passengers} passengers | {num_flights} flights | {num_bookings} bookings")

    clear_mysql()
    clear_mongo()

    seed_mysql(num_passengers, num_flights, num_bookings)
    seed_mongo(num_passengers, num_flights, num_bookings)

    from benchmark.models import Airport, Aircraft, Flight, Passenger, Booking
    mongo_client = MongoClient('mongodb://localhost:27018/')
    mongo_db     = mongo_client['airline_benchmark']

    return {
        'size':   size,
        'status': 'seeded',
        'mysql': {
            'airports':   Airport.objects.count(),
            'aircraft':   Aircraft.objects.count(),
            'flights':    Flight.objects.count(),
            'passengers': Passenger.objects.count(),
            'bookings':   Booking.objects.count(),
            'cleared':    True,
            'seeded':     True,
        },
        'mongodb': {
            'airports':   mongo_db.airports.count_documents({}),
            'aircraft':   mongo_db.aircraft.count_documents({}),
            'flights':    mongo_db.flights.count_documents({}),
            'passengers': mongo_db.passengers.count_documents({}),
            'bookings':   mongo_db.bookings.count_documents({}),
            'cleared':    True,
            'seeded':     True,
        }
    }


# ── Terminal usage ────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys
    size = sys.argv[1] if len(sys.argv) > 1 else '1k'
    print(f"Running seed from terminal with size: {size}")
    result = run_seed(size)
    print(f"\nSeed complete. Summary:")
    print(f"  MySQL   -- {result['mysql']['passengers']} passengers | {result['mysql']['flights']} flights | {result['mysql']['bookings']} bookings")
    print(f"  MongoDB -- {result['mongodb']['passengers']} passengers | {result['mongodb']['flights']} flights | {result['mongodb']['bookings']} bookings")