import os
import django
import random
import string
from datetime import timedelta
from decimal import Decimal
from datetime import datetime

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'benchmark_project.settings')
django.setup()

from faker import Faker
from benchmark.models import Airport, Aircraft, Flight, Seat, Passenger, Booking
from pymongo import MongoClient
from bson import ObjectId, Decimal128

fake = Faker()

# Dataset size configurations.
# Flights are intentionally higher than passengers to avoid seat conflicts
# at large scales. With 20 aircraft types averaging 250 seats each,
# 400,000 flights gives roughly 25 bookings per flight at 10m scale
# which is well within the capacity of any aircraft type in our list.
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

# 100 airports spread across every continent to ensure route variety.
# More airports means more possible origin-destination combinations
# which makes flight search queries more realistic and varied.
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

# 20 aircraft types covering the full range from regional jets to
# wide-body long-haul aircraft. The variety in seat capacity is
# intentional -- it makes seat availability queries more interesting
# because the number of available seats per flight varies significantly.
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


def random_booking_ref():
    return 'SK' + ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))


def generate_seats_mysql(aircraft, total_seats):
    # Seat classes are assigned by row position.
    # First class gets the first two rows, business gets the next 20%,
    # and everything after that is economy. This mirrors real aircraft layouts.
    rows = total_seats // 6
    for row in range(1, rows + 1):
        for col in ['A', 'B', 'C', 'D', 'E', 'F']:
            if row <= 2:
                seat_class = 'first'
            elif row <= int(rows * 0.2):
                seat_class = 'business'
            else:
                seat_class = 'economy'
            Seat.objects.get_or_create(
                aircraft=aircraft,
                seat_number=f"{row}{col}",
                defaults={'seat_class': seat_class}
            )


# ── Clear functions ───────────────────────────────────────────────────────────
# These are called by the Django API when the user clicks
# the Clear Databases button on the dashboard. Deletion order
# matters in MySQL because of foreign key constraints -- bookings
# must be deleted before passengers and flights, and flights before
# airports and aircraft.

def clear_mysql():
    print("Clearing MySQL...")
    Booking.objects.all().delete()
    Passenger.objects.all().delete()
    Flight.objects.all().delete()
    Seat.objects.all().delete()
    Aircraft.objects.all().delete()
    Airport.objects.all().delete()
    print("MySQL cleared successfully.")
    return {
        'database': 'MySQL',
        'status':   'cleared',
        'success':  True
    }


def clear_mongo():
    print("Clearing MongoDB...")
    client = MongoClient('mongodb://localhost:27018/')
    db     = client['airline_benchmark']
    # Drop each collection individually rather than dropping
    # the entire database -- this preserves any indexes we have set up.
    db.bookings.drop()
    db.passengers.drop()
    db.flights.drop()
    db.aircraft.drop()
    db.airports.drop()
    print("MongoDB cleared successfully.")
    return {
        'database': 'MongoDB',
        'status':   'cleared',
        'success':  True
    }


# ── MySQL seed function ───────────────────────────────────────────────────────

def seed_mysql(num_passengers, num_flights, num_bookings):
    print(f"\nSeeding MySQL -- {num_passengers} passengers, {num_flights} flights, {num_bookings} bookings")

    # Airports -- use get_or_create because airports are reference data
    # that should not be duplicated if the function is called multiple times.
    airport_objs = []
    for code, name, city, country in AIRPORTS:
        obj, _ = Airport.objects.get_or_create(
            code=code,
            defaults={'name': name, 'city': city, 'country': country}
        )
        airport_objs.append(obj)
    print(f"  {len(airport_objs)} airports ready")

    # Aircraft and their seats -- seats are generated once per aircraft
    # type and reused across all flights that use that aircraft.
    aircraft_objs = []
    for model, total in AIRCRAFT_MODELS:
        ac, created = Aircraft.objects.get_or_create(
            model=model,
            defaults={'total_seats': total}
        )
        aircraft_objs.append(ac)
        if created:
            generate_seats_mysql(ac, total)
    print(f"  {len(aircraft_objs)} aircraft ready")

    # Passengers -- each one gets a unique email and passport number.
    # Faker's unique proxy ensures no duplicates within a single run.
    passengers = []
    for _ in range(num_passengers):
        try:
            p = Passenger.objects.create(
                first_name      = fake.first_name(),
                last_name       = fake.last_name(),
                email           = fake.unique.email(),
                phone           = fake.phone_number()[:20],
                passport_number = fake.unique.bothify('??#######'),
                date_of_birth   = fake.date_of_birth(minimum_age=18, maximum_age=80),
            )
            passengers.append(p)
        except Exception:
            # Skip if a duplicate email or passport number is generated.
            # This is rare but possible with very large datasets.
            pass
    print(f"  {len(passengers)} passengers created")

    # Flights -- flight numbers use a 4-digit suffix to support up to
    # 9000 unique flights before collision risk becomes significant.
    flights      = []
    used_numbers = set()
    for _ in range(num_flights):
        origin, destination = random.sample(airport_objs, 2)
        aircraft  = random.choice(aircraft_objs)
        departure = fake.date_time_between(start_date='+1d', end_date='+365d')
        duration  = timedelta(hours=random.randint(1, 16))

        fn = f"SK{random.randint(1000, 9999)}"
        while fn in used_numbers:
            fn = f"SK{random.randint(1000, 9999)}"
        used_numbers.add(fn)

        f = Flight.objects.create(
            flight_number  = fn,
            aircraft       = aircraft,
            origin         = origin,
            destination    = destination,
            departure_time = departure,
            arrival_time   = departure + duration,
            base_price     = Decimal(str(round(random.uniform(49, 1500), 2))),
            status         = 'scheduled',
        )
        flights.append(f)
    print(f"  {len(flights)} flights created")

    # Bookings -- we track which seat has been booked on which flight
    # to prevent duplicate seat assignments on the same flight.
    booked     = 0
    used_refs  = set()
    used_seats = set()

    for _ in range(num_bookings):
        flight    = random.choice(flights)
        passenger = random.choice(passengers)
        seats     = list(Seat.objects.filter(aircraft=flight.aircraft))
        available = [s for s in seats if (flight.id, s.id) not in used_seats]

        if not available:
            # All seats on this flight are taken -- skip and try another flight.
            continue

        seat = random.choice(available)
        ref  = random_booking_ref()
        while ref in used_refs:
            ref = random_booking_ref()

        used_refs.add(ref)
        used_seats.add((flight.id, seat.id))

        # 75% of bookings are confirmed, 25% are cancelled.
        # This ratio gives enough cancelled bookings to make
        # the cancel/update benchmark meaningful.
        Booking.objects.create(
            passenger         = passenger,
            flight            = flight,
            seat              = seat,
            booking_reference = ref,
            status            = random.choice(['confirmed', 'confirmed', 'confirmed', 'cancelled']),
            total_price       = flight.base_price,
        )
        booked += 1

    print(f"  {booked} bookings created")
    print("MySQL seeding complete.")
    return booked


# ── MongoDB seed function ─────────────────────────────────────────────────────

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

    # Aircraft with seats embedded directly in the document.
    # This is the key structural difference from MySQL -- instead of
    # a separate seats table with foreign keys, MongoDB stores seat
    # definitions inside the aircraft document itself.
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

    # Flights with origin and destination embedded as snapshots.
    # Embedding airport info avoids the need for a join when searching
    # for flights by city -- the city name is stored directly in the
    # flight document so MongoDB can filter without touching another collection.
    flight_docs  = []
    used_numbers = set()
    for _ in range(num_flights):
        origin      = random.choice(airport_docs)
        destination = random.choice([a for a in airport_docs if a != origin])
        aircraft    = random.choice(aircraft_docs)
        departure   = fake.date_time_between(start_date='+1d', end_date='+365d')
        duration    = timedelta(hours=random.randint(1, 16))

        fn = f"SK{random.randint(1000, 9999)}"
        while fn in used_numbers:
            fn = f"SK{random.randint(1000, 9999)}"
        used_numbers.add(fn)

        doc = {
            "_id":            ObjectId(),
            "flight_number":  fn,
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
        }
        flight_docs.append(doc)
    db.flights.insert_many(flight_docs)
    print(f"  {len(flight_docs)} flights created")

    # Passengers
    passenger_docs = []
    for _ in range(num_passengers):
        doc = {
            "_id":             ObjectId(),
            "first_name":      fake.first_name(),
            "last_name":       fake.last_name(),
            "email":           fake.unique.email(),
            "phone":           fake.phone_number()[:20],
            "passport_number": fake.unique.bothify('??#######'),
            "date_of_birth":   datetime.combine(
                fake.date_of_birth(minimum_age=18, maximum_age=80),
                datetime.min.time()
            ),
        }
        passenger_docs.append(doc)
    db.passengers.insert_many(passenger_docs)
    print(f"  {len(passenger_docs)} passengers created")

    # Bookings with embedded snapshots of passenger and flight data.
    # The snapshot pattern means a booking document contains everything
    # needed to display a booking confirmation -- no joins required.
    # This is the main reason MongoDB tends to be faster for read-heavy
    # operations like viewing booking history.
    booking_docs = []
    used_refs    = set()
    used_seats   = set()

    for _ in range(num_bookings):
        flight    = random.choice(flight_docs)
        passenger = random.choice(passenger_docs)
        aircraft  = next(a for a in aircraft_docs if a["_id"] == flight["aircraft_id"])
        available = [
            s for s in aircraft["seats"]
            if (str(flight["_id"]), s["seat_number"]) not in used_seats
        ]

        if not available:
            continue

        seat = random.choice(available)
        ref  = random_booking_ref()
        while ref in used_refs:
            ref = random_booking_ref()

        used_refs.add(ref)
        used_seats.add((str(flight["_id"]), seat["seat_number"]))

        doc = {
            "_id":               ObjectId(),
            "passenger_id":      passenger["_id"],
            "flight_id":         flight["_id"],
            "booking_reference": ref,
            "status":            random.choice(['confirmed', 'confirmed', 'confirmed', 'cancelled']),
            "total_price":       flight["base_price"],
            "booked_at":         datetime.utcnow(),
            "seat": {
                "seat_number": seat["seat_number"],
                "seat_class":  seat["seat_class"],
            },
            # Passenger snapshot stored at booking time so the booking
            # record remains accurate even if the passenger updates their details later.
            "passenger_snapshot": {
                "first_name":      passenger["first_name"],
                "last_name":       passenger["last_name"],
                "email":           passenger["email"],
                "passport_number": passenger["passport_number"],
            },
            # Flight snapshot stored at booking time for the same reason.
            "flight_snapshot": {
                "flight_number":  flight["flight_number"],
                "origin":         flight["origin"]["code"],
                "destination":    flight["destination"]["code"],
                "departure_time": flight["departure_time"],
            },
        }
        booking_docs.append(doc)

    if booking_docs:
        db.bookings.insert_many(booking_docs)
    print(f"  {len(booking_docs)} bookings created")
    print("MongoDB seeding complete.")
    return len(booking_docs)


# ── Main entry point called by the Django API ─────────────────────────────────
# The dashboard sends a POST request with the selected size (e.g. '1k', '50k').
# This function clears both databases, resets Faker's unique value tracker,
# then seeds both databases with identical data so the comparison is fair.

def run_seed(size='1k'):
    if size not in DATASET_SIZES:
        raise ValueError(f"Invalid size '{size}'. Valid options are: {list(DATASET_SIZES.keys())}")

    config         = DATASET_SIZES[size]
    num_passengers = config['passengers']
    num_flights    = config['flights']
    num_bookings   = config['bookings']

    # Reset Faker's unique tracker and random seed before each run
    # so the data generated is consistent and reproducible.
    fake.unique.clear()
    random.seed(42)

    print(f"\nStarting seed for dataset size: {size}")
    print(f"Target: {num_passengers} passengers | {num_flights} flights | {num_bookings} bookings")

    # Clear both databases before seeding
    mysql_clear  = clear_mysql()
    mongo_clear  = clear_mongo()

    # Seed both databases with identical configuration
    mysql_bookings = seed_mysql(num_passengers, num_flights, num_bookings)
    mongo_bookings = seed_mongo(num_passengers, num_flights, num_bookings)

    # Return a summary that the API sends back to the dashboard
    # so the frontend can display the verification counts.
    from benchmark.models import Airport, Aircraft, Flight, Passenger, Booking
    from pymongo import MongoClient
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
            'cleared':    mysql_clear['success'],
            'seeded':     True,
        },
        'mongodb': {
            'airports':   mongo_db.airports.count_documents({}),
            'aircraft':   mongo_db.aircraft.count_documents({}),
            'flights':    mongo_db.flights.count_documents({}),
            'passengers': mongo_db.passengers.count_documents({}),
            'bookings':   mongo_db.bookings.count_documents({}),
            'cleared':    mongo_clear['success'],
            'seeded':     True,
        }
    }


# ── Terminal usage ────────────────────────────────────────────────────────────
# The seed can also be triggered from the terminal for testing.
# Usage: python seed.py 1k
# If no argument is given it defaults to 1k.

if __name__ == '__main__':
    import sys
    size = sys.argv[1] if len(sys.argv) > 1 else '1k'
    print(f"Running seed from terminal with size: {size}")
    result = run_seed(size)
    print(f"\nSeed complete. Summary:")
    print(f"  MySQL    -- {result['mysql']['passengers']} passengers | {result['mysql']['flights']} flights | {result['mysql']['bookings']} bookings")
    print(f"  MongoDB  -- {result['mongodb']['passengers']} passengers | {result['mongodb']['flights']} flights | {result['mongodb']['bookings']} bookings")