import psycopg2

def print_table_records(cursor, table_name):
    """
    Helper function to fetch and print the first 10 records of a table
    """
    # Use %s as placeholder in PostgreSQL instead of ? used in SQLite
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 10")
    records = cursor.fetchall()
    
    # Get column names - modified for PostgreSQL
    column_names = [desc[0] for desc in cursor.description]
    
    print(f"\n=== First 5 records from {table_name} ===")
    print("Columns:", ", ".join(column_names))
    print("\nRecords:")
    for record in records:
        print(record)

# Database configuration
DB_CONFIG = {
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': '5433'
}

# Connect to CrashDW database
print("\nQuerying CrashDW database...")
crash_conn = psycopg2.connect(
    database='crashDW',
    user=DB_CONFIG['user'],
    password=DB_CONFIG['password'],
    host=DB_CONFIG['host'],
    port=DB_CONFIG['port']
)
crash_cursor = crash_conn.cursor()

# Query all tables in CrashDW
crash_tables = [
    "DimDateTime_Crash",
    "DimLocation_Crash",
    "DimCondition_Crash",
    "DimCrashType",
    "FactCrash"
]

for table in crash_tables:
    print_table_records(crash_cursor, table)

crash_conn.close()

# Connect to VehicleDW database
print("\nQuerying VehicleDW database...")
vehicle_conn = psycopg2.connect(
    database='vehicleDW',
    user=DB_CONFIG['user'],
    password=DB_CONFIG['password'],
    host=DB_CONFIG['host'],
    port=DB_CONFIG['port']
)
vehicle_cursor = vehicle_conn.cursor()

# Query all tables in VehicleDW
vehicle_tables = [
    "DimDateTime_Veh",
    "DimLocation_Veh",
    "DimDriver",
    "DimVehicle",
    "FactVehicleInvolment"
]

for table in vehicle_tables:
    print_table_records(vehicle_cursor, table)

vehicle_conn.close()