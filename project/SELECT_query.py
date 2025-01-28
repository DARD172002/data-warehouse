import sqlite3

def print_table_records(cursor, table_name):
    """
    Helper function to fetch and print the first 5 records of a table
    """
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 10")
    records = cursor.fetchall()
    
    # Get column names
    column_names = [description[0] for description in cursor.description]
    
    print(f"\n=== First 5 records from {table_name} ===")
    print("Columns:", ", ".join(column_names))
    print("\nRecords:")
    for record in records:
        print(record)

# Connect to CrashDW database
print("\nQuerying CrashDW database...")
crash_conn = sqlite3.connect("data/crashDW.db")
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
vehicle_conn = sqlite3.connect("data/vehicleDW.db")
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