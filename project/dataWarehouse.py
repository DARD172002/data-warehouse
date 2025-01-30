import pandas as pd
import sqlite3
from datetime import datetime
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def clean_string_value(value):
    """Clean string values by handling NaN, None, and standardizing missing value indicators"""
    if pd.isna(value) or value is None or value == '':
        return ''
    return str(value).strip()

def clean_numeric_value(value):
    """Clean numeric values by handling NaN, None, and invalid values"""
    if pd.isna(value) or value is None or str(value).strip() == '':
        return 0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0
    
def clean_vehicle_year(x):
    """Clean vehicle year values by handling invalid years"""
    try:
        # First convert to float (handles NaN) then to int
        year = int(float(x)) if pd.notna(x) else 0
        # Check if year is in valid range
        if year < 1900 or year > datetime.now().year:
            return 0
        return year
    except (ValueError, TypeError):
        return 0
    
# Define data types explicitly for each column in the crash data CSV
dtypes = {
    # Identifiers and Reference Numbers - All should be strings to preserve leading zeros and special characters
    'Report Number': str,
    'Local Case Number': str,
    'Person ID': str,
    'Vehicle ID': str,
    
    # Text Fields - Agency and Location Information
    'Agency Name': str,
    'ACRS Report Type': str,
    'Route Type': str,
    'Road Name': str,
    'Cross-Street Name': str,
    'Off-Road Description': str,
    'Municipality': str,
    
    # Date and Time
    'Crash Date/Time': str,  # Will be parsed later using datetime
    
    # Categorical Fields - Crash Details
    'Related Non-Motorist': str,
    'Collision Type': str,
    'Weather': str,
    'Surface Condition': str,
    'Light': str,
    'Traffic Control': str,
    
    # Categorical Fields - Driver and Vehicle Information
    'Driver Substance Abuse': str,
    'Non-Motorist Substance Abuse': str,
    'Driver At Fault': str,
    'Injury Severity': str,
    'Circumstance': str,
    'Driver Distracted By': str,
    'Drivers License State': str,
    'Vehicle Damage Extent': str,
    'Vehicle First Impact Location': str,
    'Vehicle Body Type': str,
    'Vehicle Movement': str,
    'Vehicle Going Dir': str,
    'Vehicle Make': str,
    'Vehicle Model': str,
    
    # Numeric Fields - Use Int64 for nullable integers
    'Speed Limit': 'Int64',
    'Vehicle Year': 'Int64',
    
    # Boolean Fields - Will be standardized to Y/N
    'Driverless Vehicle': str,
    'Parked Vehicle': str,
    
    # Geographic Coordinates - Use float for decimal precision
    'Latitude': float,
    'Longitude': float,
    
    # Combined Location Field (appears to be a string representation of coordinates)
    'Location': str
}

# Additional data cleaning configurations
na_values = [
    '', 'N/A', 'NA', 'UNKNOWN', 'NULL',  # Standard missing value indicators
    'NONE', 'None', '0000', '0',         # Additional missing value variations
    'NOT REPORTED', 'UNSPECIFIED'         # Domain-specific missing value indicators
]

# Date parsing format for reference
date_format = "%m/%d/%Y %I:%M:%S %p"  # Example: "05/27/2021 07:40:00 PM"



# --------------------------------------------------------------------
# 1. Lee el CSV con pandas
# --------------------------------------------------------------------
csv_path = "data/Crash_Reporting_-_Drivers_Data.csv"
df = pd.read_csv(
    csv_path,
    dtype=dtypes,
    keep_default_na=False,
    na_values=na_values,
    encoding='utf-8',
    on_bad_lines='warn',
    #parse_dates=['Crash Date/Time'],
    #date_format="%m/%d/%Y %I:%M:%S %p"  # Use date_format instead of date_parser
)

# Clean and standardize date/time values
def clean_datetime(date_str):
    """Convert date string to standard format, handling invalid dates"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")
    except (ValueError, TypeError):
        return None

# Apply cleaning functions to relevant columns
for col in df.columns:
    #if col == 'Crash Date/Time':
     #   df[col] = df[col].apply(clean_datetime)
    #el
    
    if df[col].dtype in ['int64', 'float64']:
        df[col] = df[col].apply(clean_numeric_value)
    else:
        df[col] = df[col].apply(clean_string_value)

# Handle special cases for Vehicle Year
df['Vehicle Year'] = df['Vehicle Year'].apply(clean_vehicle_year)

# Normalize boolean fields
def normalize_boolean(value):
    """Convert various boolean indicators to Y/N"""
    if pd.isna(value) or value is None or value == '':
        return 'N'
    return 'Y' if str(value).upper() in ['Y', 'YES', 'TRUE', '1'] else 'N'

for bool_col in ['Driverless Vehicle', 'Parked Vehicle']:
    df[bool_col] = df[bool_col].apply(normalize_boolean)

# --------------------------------------------------------------------
# 2. Conexiones a las dos bases de datos (SQLite como ejemplo)
#    - crash_conn: para el esquema de Crash
#    - vehicle_conn: para el esquema de Vehicle Involvement
# --------------------------------------------------------------------
def create_database_if_not_exists(dbname, user, password, host, port):
    """
    Creates a PostgreSQL database if it doesn't exist.
    Returns True if database was created, False if it already existed.
    """
    # Connect to PostgreSQL server to check/create database
    conn = psycopg2.connect(
        database="postgres",  # Connect to default postgres database first
        user=user,
        password=password,
        host=host,
        port=port
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    # Check if database exists
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (dbname,))
    exists = cursor.fetchone()
    
    if not exists:
        try:
            cursor.execute(f'CREATE DATABASE "{dbname}"')
            print(f"Database {dbname} created successfully")
            created = True
        except Exception as e:
            print(f"Error creating database {dbname}: {str(e)}")
            raise
    else:
        print(f"Database {dbname} already exists")
        created = False
    
    cursor.close()
    conn.close()
    return created

def get_db_connection(dbname, user, password, host, port):
    """
    Creates a connection to the specified database.
    Creates the database if it doesn't exist.
    """
    try:
        # First ensure database exists
        create_database_if_not_exists(dbname, user, password, host, port)
        
        # Connect to the specified database
        conn = psycopg2.connect(
            database=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        print(f"Successfully connected to database {dbname}")
        return conn
    
    except Exception as e:
        print(f"Error connecting to database {dbname}: {str(e)}")
        raise

# Database configuration
DB_CONFIG = {
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': '5433'
}

# Create connections for both databases
try:
    # Setup crash database connection
    crash_conn = get_db_connection(
        dbname='crashDW',
        **DB_CONFIG
    )
    
    # Setup vehicle database connection
    vehicle_conn = get_db_connection(
        dbname='vehicleDW',
        **DB_CONFIG
    )
    
    # Get cursors
    crash_cursor = crash_conn.cursor()
    vehicle_cursor = vehicle_conn.cursor()
    
except Exception as e:
    print(f"Error during database setup: {str(e)}")
    raise

# --------------------------------------------------------------------
# 3. Crear las tablas correspondientes en cada DB
# --------------------------------------------------------------------
# 3.1. Tablas del CrashDW
# Modify the table creation statements for PostgreSQL
# Note: Remove AUTOINCREMENT and use SERIAL instead
def create_crash_tables(cursor):
    """Creates tables for the crash database"""
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS DimDateTime_Crash (
        date_key_crash INTEGER PRIMARY KEY,
        date_value TEXT,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        hour INTEGER,
        day_of_week TEXT,
        am_pm TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS DimLocation_Crash (
        location_key_crash SERIAL PRIMARY KEY,
        route_type TEXT,
        road_name TEXT,
        cross_street_name TEXT,
        off_road_description TEXT,
        municipality TEXT,
        latitude REAL,
        longitude REAL
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS DimCondition_Crash (
        condition_key_crash SERIAL PRIMARY KEY,
        weather TEXT,
        surface_condition TEXT,
        light TEXT,
        traffic_control TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS DimCrashType (
        crash_type_key SERIAL PRIMARY KEY,
        acrs_report_type TEXT,
        collision_type TEXT,
        related_non_motorist TEXT,
        agency_name TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS FactCrash (
        fact_crash_id SERIAL PRIMARY KEY,
        date_key_crash INTEGER REFERENCES DimDateTime_Crash(date_key_crash),
        location_key_crash INTEGER REFERENCES DimLocation_Crash(location_key_crash),
        condition_key_crash INTEGER REFERENCES DimCondition_Crash(condition_key_crash),
        crash_type_key INTEGER REFERENCES DimCrashType(crash_type_key),
        num_vehicles_involved INTEGER,
        num_injuries INTEGER,
        num_fatalities INTEGER,
        report_number TEXT
    )
    """)

def create_vehicle_tables(cursor):
    """Creates tables for the vehicle database"""
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS DimDateTime_Veh (
        date_key_vehicle INTEGER PRIMARY KEY,
        date_value TEXT,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        hour INTEGER
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS DimLocation_Veh (
        location_key_vehicle SERIAL PRIMARY KEY,
        route_type TEXT,
        road_name TEXT,
        cross_street_name TEXT,
        municipality TEXT,
        latitude REAL,
        longitude REAL
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS DimDriver (
        driver_key SERIAL PRIMARY KEY,
        driver_substance_abuse TEXT,
        non_motorist_substance_abuse TEXT,
        driver_distracted_by TEXT,
        drivers_license_state TEXT,
        person_id TEXT,
        driver_at_fault TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS DimVehicle (
        vehicle_key SERIAL PRIMARY KEY,
        vehicle_id TEXT,
        vehicle_damage_extent TEXT,
        vehicle_first_impact_location TEXT,
        vehicle_body_type TEXT,
        vehicle_movement TEXT,
        vehicle_going_dir TEXT,
        speed_limit INTEGER,
        driverless_vehicle TEXT,
        parked_vehicle TEXT,
        vehicle_year INTEGER,
        vehicle_make TEXT,
        vehicle_model TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS FactVehicleInvolment (
        fact_vehicle_id SERIAL PRIMARY KEY,
        date_key_vehicle INTEGER REFERENCES DimDateTime_Veh(date_key_vehicle),
        location_key_vehicle INTEGER REFERENCES DimLocation_Veh(location_key_vehicle),
        driver_key INTEGER REFERENCES DimDriver(driver_key),
        vehicle_key INTEGER REFERENCES DimVehicle(vehicle_key),
        injury_security TEXT,
        drive_at_fault_flag TEXT,
        circumstance TEXT
    )
    """)

# Create the tables
try:
    # Create tables in crash database
    create_crash_tables(crash_cursor)
    crash_conn.commit()
    print("Crash tables created successfully")
    
    # Create tables in vehicle database
    create_vehicle_tables(vehicle_cursor)
    vehicle_conn.commit()
    print("Vehicle tables created successfully")
    
except Exception as e:
    print(f"Error creating tables: {str(e)}")
    # Rollback in case of error
    crash_conn.rollback()
    vehicle_conn.rollback()
    raise

# --------------------------------------------------------------------
# 4. Funciones helpers para Dimensions (Lookups)
#    - Usan diccionarios en memoria para 'cachear' y no duplicar valores.
#    - Insertan si no existe y devuelven la PK.
# --------------------------------------------------------------------
# Helper functions for Crash DW
dimDateCrashDict = {}
def get_date_key_crash(crash_date_str, cursor):
    """
    Converts date/time to integer and creates record in DimDateTime_Crash if it doesn't exist.
    """
    if not crash_date_str:
        return None
    
    dt = datetime.strptime(crash_date_str, "%m/%d/%Y %I:%M:%S %p")
    date_key = int(dt.strftime("%Y%m%d%H"))

    if date_key not in dimDateCrashDict:
        date_value = dt.strftime("%Y-%m-%d %H:%M:%S")
        year = dt.year
        month = dt.month
        day = dt.day
        hour = dt.hour
        day_of_week = dt.strftime("%A")
        am_pm = dt.strftime("%p")

        cursor.execute("""
            INSERT INTO DimDateTime_Crash(date_key_crash, date_value, year, month, day, hour, day_of_week, am_pm)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_key_crash) DO NOTHING
            RETURNING date_key_crash
        """, (date_key, date_value, year, month, day, hour, day_of_week, am_pm))
        dimDateCrashDict[date_key] = True

    return date_key

dimLocCrashDict = {}
def get_location_key_crash(row, cursor):
    """
    Handles location dimension for crash data warehouse
    """
    loc_tuple = (
        row["Route Type"], row["Road Name"], row["Cross-Street Name"],
        row["Off-Road Description"], row["Municipality"],
        row["Latitude"], row["Longitude"]
    )
    
    if loc_tuple not in dimLocCrashDict:
        cursor.execute("""
            INSERT INTO DimLocation_Crash(route_type, road_name, cross_street_name, 
                off_road_description, municipality, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING location_key_crash
        """, loc_tuple)
        location_id = cursor.fetchone()[0]
        dimLocCrashDict[loc_tuple] = location_id
    
    return dimLocCrashDict[loc_tuple]

dimCondCrashDict = {}
def get_condition_key_crash(row, cursor):
    """
    Handles condition dimension for crash data warehouse
    """
    cond_tuple = (
        row["Weather"],
        row["Surface Condition"],
        row["Light"],
        row["Traffic Control"]
    )
    
    if cond_tuple not in dimCondCrashDict:
        cursor.execute("""
            INSERT INTO DimCondition_Crash(weather, surface_condition, light, traffic_control)
            VALUES (%s, %s, %s, %s)
            RETURNING condition_key_crash
        """, cond_tuple)
        cond_id = cursor.fetchone()[0]
        dimCondCrashDict[cond_tuple] = cond_id
    
    return dimCondCrashDict[cond_tuple]

dimCrashTypeDict = {}
def get_crash_type_key(row, cursor):
    """
    Handles crash type dimension
    """
    ctype_tuple = (
        row["ACRS Report Type"],
        row["Collision Type"],
        row["Related Non-Motorist"],
        row["Agency Name"]
    )
    
    if ctype_tuple not in dimCrashTypeDict:
        cursor.execute("""
            INSERT INTO DimCrashType(acrs_report_type, collision_type, related_non_motorist, agency_name)
            VALUES (%s, %s, %s, %s)
            RETURNING crash_type_key
        """, ctype_tuple)
        ctype_id = cursor.fetchone()[0]
        dimCrashTypeDict[ctype_tuple] = ctype_id
    
    return dimCrashTypeDict[ctype_tuple]

# Helper functions for Vehicle DW
dimDateVehDict = {}
def get_date_key_vehicle(crash_date_str, cursor):
    """
    Handles date dimension for vehicle data warehouse
    """
    if not crash_date_str:
        return None
        
    dt = datetime.strptime(crash_date_str, "%m/%d/%Y %I:%M:%S %p")
    date_key = int(dt.strftime("%Y%m%d%H"))
    
    if date_key not in dimDateVehDict:
        date_value = dt.strftime("%Y-%m-%d %H:%M:%S")
        year = dt.year
        month = dt.month
        day = dt.day
        hour = dt.hour

        cursor.execute("""
            INSERT INTO DimDateTime_Veh(date_key_vehicle, date_value, year, month, day, hour)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_key_vehicle) DO NOTHING
            RETURNING date_key_vehicle
        """, (date_key, date_value, year, month, day, hour))
        dimDateVehDict[date_key] = True
    
    return date_key

dimLocVehDict = {}
def get_location_key_vehicle(row, cursor):
    """
    Handles location dimension for vehicle data warehouse
    """
    loc_tuple = (
        row["Route Type"], row["Road Name"], row["Cross-Street Name"],
        row["Municipality"], row["Latitude"], row["Longitude"]
    )
    
    if loc_tuple not in dimLocVehDict:
        cursor.execute("""
            INSERT INTO DimLocation_Veh(route_type, road_name, cross_street_name,
                municipality, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING location_key_vehicle
        """, loc_tuple)
        location_id = cursor.fetchone()[0]
        dimLocVehDict[loc_tuple] = location_id
    
    return dimLocVehDict[loc_tuple]

dimDriverDict = {}
def get_driver_key(row, cursor):
    """
    Handles driver dimension
    """
    driver_tuple = (
        row["Driver Substance Abuse"],
        row["Non-Motorist Substance Abuse"],
        row["Driver Distracted By"],
        row["Drivers License State"],
        row["Person ID"],
        row["Driver At Fault"]
    )
    
    if driver_tuple not in dimDriverDict:
        cursor.execute("""
            INSERT INTO DimDriver(driver_substance_abuse, non_motorist_substance_abuse,
                driver_distracted_by, drivers_license_state, person_id, driver_at_fault)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING driver_key
        """, driver_tuple)
        driver_id = cursor.fetchone()[0]
        dimDriverDict[driver_tuple] = driver_id
    
    return dimDriverDict[driver_tuple]

dimVehicleDict = {}
def get_vehicle_key(row, cursor):
    """
    Handles vehicle dimension
    """
    # Ensure numeric values are properly handled
    speed_limit = clean_numeric_value(row["Speed Limit"])
    vehicle_year = clean_vehicle_year(row["Vehicle Year"])
    
    vehicle_tuple = (
        clean_string_value(row["Vehicle ID"]),
        clean_string_value(row["Vehicle Damage Extent"]),
        clean_string_value(row["Vehicle First Impact Location"]),
        clean_string_value(row["Vehicle Body Type"]),
        clean_string_value(row["Vehicle Movement"]),
        clean_string_value(row["Vehicle Going Dir"]),
        speed_limit,  # Using cleaned numeric value
        clean_string_value(row["Driverless Vehicle"]),
        clean_string_value(row["Parked Vehicle"]),
        vehicle_year,  # Using cleaned vehicle year
        clean_string_value(row["Vehicle Make"]),
        clean_string_value(row["Vehicle Model"])
    )
    
    if vehicle_tuple not in dimVehicleDict:
        cursor.execute("""
            INSERT INTO DimVehicle(vehicle_id, vehicle_damage_extent, vehicle_first_impact_location,
                vehicle_body_type, vehicle_movement, vehicle_going_dir, speed_limit,
                driverless_vehicle, parked_vehicle, vehicle_year, vehicle_make, vehicle_model)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING vehicle_key
        """, vehicle_tuple)
        veh_id = cursor.fetchone()[0]
        dimVehicleDict[vehicle_tuple] = veh_id
    
    return dimVehicleDict[vehicle_tuple]
# --------------------------------------------------------------------
# 5. Llenar Dimensiones + FactCrash
#    Para FactCrash, necesitamos agrupar por "Report Number".
#    calculamos: num_vehicles_involved, num_injuries, num_fatalities (si aplica)
# --------------------------------------------------------------------

# 5.1. Generamos un DF agrupado por "Report Number"
#     Suponiendo que:
#         num_vehicles_involved = COUNT(distinct Vehicle ID)
#         num_injuries = COUNT(rows donde "Injury Severity" != "NO APPARENT INJURY")
#         num_fatalities = COUNT(rows donde "Injury Severity" indica algo fatal) – si tuviéramos esa info
#
#     Ajustar según tu propia lógica.
# --------------------------------------------------------------------
def is_injury(sev):
    # Por ejemplo, consideramos lesión toda que no sea "NO APPARENT INJURY"
    return sev.strip().upper() != "NO APPARENT INJURY"

def is_fatal(sev):
    # Ejemplo si el CSV tuviera "FATAL INJURY"
    return "FATAL" in sev.strip().upper()

grouped = df.groupby("Report Number")
summary_list = []

for report_number, group in grouped:
    num_veh_involved = group["Vehicle ID"].nunique()
    num_injuries = sum(group["Injury Severity"].apply(is_injury))
    num_fatalities = sum(group["Injury Severity"].apply(is_fatal))
    # Tomamos la primera fila para extraer datos "globales" del crash
    first_row = group.iloc[0]

    summary_list.append({
        "report_number": report_number,
        "Crash Date/Time": first_row["Crash Date/Time"],
        "Route Type": first_row["Route Type"],
        "Road Name": first_row["Road Name"],
        "Cross-Street Name": first_row["Cross-Street Name"],
        "Off-Road Description": first_row["Off-Road Description"],
        "Municipality": first_row["Municipality"],
        "Latitude": first_row["Latitude"],
        "Longitude": first_row["Longitude"],
        "Weather": first_row["Weather"],
        "Surface Condition": first_row["Surface Condition"],
        "Light": first_row["Light"],
        "Traffic Control": first_row["Traffic Control"],
        "ACRS Report Type": first_row["ACRS Report Type"],
        "Collision Type": first_row["Collision Type"],
        "Related Non-Motorist": first_row["Related Non-Motorist"],
        "Agency Name": first_row["Agency Name"],
        "num_vehicles_involved": num_veh_involved,
        "num_injuries": num_injuries,
        "num_fatalities": num_fatalities
    })

factCrashDF = pd.DataFrame(summary_list)

# 5.2. Insertar en tablas Dim y luego FactCrash
for idx, row in factCrashDF.iterrows():
    date_key = get_date_key_crash(row["Crash Date/Time"], crash_cursor)
    loc_key = get_location_key_crash(row, crash_cursor)
    cond_key = get_condition_key_crash(row, crash_cursor)
    ctype_key = get_crash_type_key(row, crash_cursor)

    crash_cursor.execute("""
        INSERT INTO FactCrash(date_key_crash, location_key_crash, condition_key_crash,
            crash_type_key, num_vehicles_involved, num_injuries,
            num_fatalities, report_number)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        date_key, loc_key, cond_key, ctype_key,
        row["num_vehicles_involved"], row["num_injuries"], row["num_fatalities"],
        row["report_number"]
    ))

crash_conn.commit()

# --------------------------------------------------------------------
# 6. Llenar Dimensiones + FactVehicleInvolment
#    Aquí insertamos registro por cada fila del CSV (cada vehículo).
# --------------------------------------------------------------------
for idx, row in df.iterrows():
    date_key = get_date_key_vehicle(row["Crash Date/Time"], vehicle_cursor)
    loc_key = get_location_key_vehicle(row, vehicle_cursor)
    drv_key = get_driver_key(row, vehicle_cursor)
    veh_key = get_vehicle_key(row, vehicle_cursor)

    injury_security = row["Injury Severity"]
    drive_at_fault_flag = row["Driver At Fault"]
    circumstance = row["Circumstance"]

    vehicle_cursor.execute("""
        INSERT INTO FactVehicleInvolment(date_key_vehicle, location_key_vehicle,
            driver_key, vehicle_key,
            injury_security, drive_at_fault_flag, circumstance)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        date_key, loc_key, drv_key, veh_key,
        injury_security, drive_at_fault_flag, circumstance
    ))

vehicle_conn.commit()
# --------------------------------------------------------------------
# 7. ¡Listo! Cerramos conexiones
# --------------------------------------------------------------------
crash_conn.close()
vehicle_conn.close()

print("ETL completado. Datos cargados en crashDW.db y vehicleDW.db.")
