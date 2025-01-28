import pandas as pd
import sqlite3
from datetime import datetime
import os

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
    parse_dates=['Crash Date/Time'],
    date_format="%m/%d/%Y %I:%M:%S %p"  # Use date_format instead of date_parser
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
    if col == 'Crash Date/Time':
        df[col] = df[col].apply(clean_datetime)
    elif df[col].dtype in ['int64', 'float64']:
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
if os.path.exists('data/crashDW.db'):
    os.remove('data/crashDW.db')  # OPCIONAL: eliminar para recrear DB limpia
if os.path.exists('data/vehicleDW.db'):
    os.remove('data/vehicleDW.db') 

crash_conn = sqlite3.connect("data/crashDW.db")
vehicle_conn = sqlite3.connect("data/vehicleDW.db")

crash_cursor = crash_conn.cursor()
vehicle_cursor = vehicle_conn.cursor()

# --------------------------------------------------------------------
# 3. Crear las tablas correspondientes en cada DB
# --------------------------------------------------------------------
# 3.1. Tablas del CrashDW
crash_cursor.execute("""
CREATE TABLE DimDateTime_Crash (
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

crash_cursor.execute("""
CREATE TABLE DimLocation_Crash (
    location_key_crash INTEGER PRIMARY KEY AUTOINCREMENT,
    route_type TEXT,
    road_name TEXT,
    cross_street_name TEXT,
    off_road_description TEXT,
    municipality TEXT,
    latitude REAL,
    longitude REAL
)
""")

crash_cursor.execute("""
CREATE TABLE DimCondition_Crash (
    condition_key_crash INTEGER PRIMARY KEY AUTOINCREMENT,
    weather TEXT,
    surface_condition TEXT,
    light TEXT,
    traffic_control TEXT
)
""")

crash_cursor.execute("""
CREATE TABLE DimCrashType (
    crash_type_key INTEGER PRIMARY KEY AUTOINCREMENT,
    acrs_report_type TEXT,
    collision_type TEXT,
    related_non_motorist TEXT,
    agency_name TEXT
)
""")

crash_cursor.execute("""
CREATE TABLE FactCrash (
    fact_crash_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_key_crash INTEGER,
    location_key_crash INTEGER,
    condition_key_crash INTEGER,
    crash_type_key INTEGER,
    num_vehicles_involved INTEGER,
    num_injuries INTEGER,
    num_fatalities INTEGER,
    report_number TEXT,

    FOREIGN KEY (date_key_crash) REFERENCES DimDateTime_Crash(date_key_crash),
    FOREIGN KEY (location_key_crash) REFERENCES DimLocation_Crash(location_key_crash),
    FOREIGN KEY (condition_key_crash) REFERENCES DimCondition_Crash(condition_key_crash),
    FOREIGN KEY (crash_type_key) REFERENCES DimCrashType(crash_type_key)
)
""")

crash_conn.commit()

# 3.2. Tablas del VehicleDW
vehicle_cursor.execute("""
CREATE TABLE DimDateTime_Veh (
    date_key_vehicle INTEGER PRIMARY KEY,
    date_value TEXT,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER
)
""")

vehicle_cursor.execute("""
CREATE TABLE DimLocation_Veh (
    location_key_vehicle INTEGER PRIMARY KEY AUTOINCREMENT,
    route_type TEXT,
    road_name TEXT,
    cross_street_name TEXT,
    municipality TEXT,
    latitude REAL,
    longitude REAL
)
""")

vehicle_cursor.execute("""
CREATE TABLE DimDriver (
    driver_key INTEGER PRIMARY KEY AUTOINCREMENT,
    driver_substance_abuse TEXT,
    non_motorist_substance_abuse TEXT,
    driver_distracted_by TEXT,
    drivers_license_state TEXT,
    person_id TEXT,
    driver_at_fault TEXT
)
""")

vehicle_cursor.execute("""
CREATE TABLE DimVehicle (
    vehicle_key INTEGER PRIMARY KEY AUTOINCREMENT,
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

vehicle_cursor.execute("""
CREATE TABLE FactVehicleInvolment (
    fact_vehicle_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_key_vehicle INTEGER,
    location_key_vehicle INTEGER,
    driver_key INTEGER,
    vehicle_key INTEGER,
    injury_security TEXT,
    drive_at_fault_flag TEXT,
    circumstance TEXT,

    FOREIGN KEY (date_key_vehicle) REFERENCES DimDateTime_Veh(date_key_vehicle),
    FOREIGN KEY (location_key_vehicle) REFERENCES DimLocation_Veh(location_key_vehicle),
    FOREIGN KEY (driver_key) REFERENCES DimDriver(driver_key),
    FOREIGN KEY (vehicle_key) REFERENCES DimVehicle(vehicle_key)
)
""")

vehicle_conn.commit()

# --------------------------------------------------------------------
# 4. Funciones helpers para Dimensions (Lookups)
#    - Usan diccionarios en memoria para 'cachear' y no duplicar valores.
#    - Insertan si no existe y devuelven la PK.
# --------------------------------------------------------------------
dimDateCrashDict = {}
def get_date_key_crash(crash_date_str, cursor):
    """
    Convierte la fecha/hora en un entero (y crea registro en DimDateTime_Crash si no existe).
    Por ejemplo, un "date_key_crash" = YYYYMMDDHH (o cualquier convención).
    """
    if not crash_date_str:
        return None
    # Intentar parsear la fecha/hora
    dt = datetime.strptime(crash_date_str, "%m/%d/%Y %I:%M:%S %p")  # 05/27/2021 07:40:00 PM
    date_key = int(dt.strftime("%Y%m%d%H"))  # por ejemplo 2021052720

    if date_key not in dimDateCrashDict:
        # Insertamos en la tabla DimDateTime_Crash
        date_value = dt.strftime("%Y-%m-%d %H:%M:%S")
        year = dt.year
        month = dt.month
        day = dt.day
        hour = dt.hour
        day_of_week = dt.strftime("%A")
        am_pm = dt.strftime("%p")

        cursor.execute("""
            INSERT INTO DimDateTime_Crash(date_key_crash, date_value, year, month, day, hour, day_of_week, am_pm)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (date_key, date_value, year, month, day, hour, day_of_week, am_pm))
        dimDateCrashDict[date_key] = True

    return date_key

dimLocCrashDict = {}
def get_location_key_crash(row, cursor):
    """
    route_type, road_name, cross_street_name, off_road_description, municipality, latitude, longitude
    Hacemos un hash o tupla para no duplicar.
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
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, loc_tuple)
        location_id = cursor.lastrowid
        dimLocCrashDict[loc_tuple] = location_id
    return dimLocCrashDict[loc_tuple]

dimCondCrashDict = {}
def get_condition_key_crash(row, cursor):
    cond_tuple = (
        row["Weather"],
        row["Surface Condition"],
        row["Light"],
        row["Traffic Control"]
    )
    if cond_tuple not in dimCondCrashDict:
        cursor.execute("""
            INSERT INTO DimCondition_Crash(weather, surface_condition, light, traffic_control)
            VALUES (?, ?, ?, ?)
        """, cond_tuple)
        cond_id = cursor.lastrowid
        dimCondCrashDict[cond_tuple] = cond_id
    return dimCondCrashDict[cond_tuple]

dimCrashTypeDict = {}
def get_crash_type_key(row, cursor):
    ctype_tuple = (
        row["ACRS Report Type"],
        row["Collision Type"],
        row["Related Non-Motorist"],
        row["Agency Name"]
    )
    if ctype_tuple not in dimCrashTypeDict:
        cursor.execute("""
            INSERT INTO DimCrashType(acrs_report_type, collision_type, related_non_motorist, agency_name)
            VALUES (?, ?, ?, ?)
        """, ctype_tuple)
        ctype_id = cursor.lastrowid
        dimCrashTypeDict[ctype_tuple] = ctype_id
    return dimCrashTypeDict[ctype_tuple]

# ---------------------------
# Para VehicleDW
# ---------------------------
dimDateVehDict = {}
def get_date_key_vehicle(crash_date_str, cursor):
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
            VALUES (?, ?, ?, ?, ?, ?)
        """, (date_key, date_value, year, month, day, hour))
        dimDateVehDict[date_key] = True
    return date_key

dimLocVehDict = {}
def get_location_key_vehicle(row, cursor):
    loc_tuple = (
        row["Route Type"], row["Road Name"], row["Cross-Street Name"],
        row["Municipality"], row["Latitude"], row["Longitude"]
    )
    if loc_tuple not in dimLocVehDict:
        cursor.execute("""
            INSERT INTO DimLocation_Veh(route_type, road_name, cross_street_name,
                                        municipality, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?)
        """, loc_tuple)
        location_id = cursor.lastrowid
        dimLocVehDict[loc_tuple] = location_id
    return dimLocVehDict[loc_tuple]

dimDriverDict = {}
def get_driver_key(row, cursor):
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
            VALUES (?, ?, ?, ?, ?, ?)
        """, driver_tuple)
        driver_id = cursor.lastrowid
        dimDriverDict[driver_tuple] = driver_id
    return dimDriverDict[driver_tuple]

dimVehicleDict = {}
def get_vehicle_key(row, cursor):
    vehicle_tuple = (
        row["Vehicle ID"],
        row["Vehicle Damage Extent"],
        row["Vehicle First Impact Location"],
        row["Vehicle Body Type"],
        row["Vehicle Movement"],
        row["Vehicle Going Dir"],
        row["Speed Limit"],
        row["Driverless Vehicle"],
        row["Parked Vehicle"],
        row["Vehicle Year"],
        row["Vehicle Make"],
        row["Vehicle Model"]
    )
    if vehicle_tuple not in dimVehicleDict:
        cursor.execute("""
            INSERT INTO DimVehicle(vehicle_id, vehicle_damage_extent, vehicle_first_impact_location,
                                   vehicle_body_type, vehicle_movement, vehicle_going_dir, speed_limit,
                                   driverless_vehicle, parked_vehicle, vehicle_year, vehicle_make, vehicle_model)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, vehicle_tuple)
        veh_id = cursor.lastrowid
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
    loc_key  = get_location_key_crash(row, crash_cursor)
    cond_key = get_condition_key_crash(row, crash_cursor)
    ctype_key= get_crash_type_key(row, crash_cursor)

    crash_cursor.execute("""
        INSERT INTO FactCrash(date_key_crash, location_key_crash, condition_key_crash,
                              crash_type_key, num_vehicles_involved, num_injuries,
                              num_fatalities, report_number)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
    loc_key  = get_location_key_vehicle(row, vehicle_cursor)
    drv_key  = get_driver_key(row, vehicle_cursor)
    veh_key  = get_vehicle_key(row, vehicle_cursor)

    injury_security = row["Injury Severity"]
    drive_at_fault_flag = row["Driver At Fault"]
    circumstance = row["Circumstance"]

    vehicle_cursor.execute("""
        INSERT INTO FactVehicleInvolment(date_key_vehicle, location_key_vehicle,
                                         driver_key, vehicle_key,
                                         injury_security, drive_at_fault_flag, circumstance)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (date_key, loc_key, drv_key, veh_key,
          injury_security, drive_at_fault_flag, circumstance))

vehicle_conn.commit()

# --------------------------------------------------------------------
# 7. ¡Listo! Cerramos conexiones
# --------------------------------------------------------------------
crash_conn.close()
vehicle_conn.close()

print("ETL completado. Datos cargados en crashDW.db y vehicleDW.db.")
