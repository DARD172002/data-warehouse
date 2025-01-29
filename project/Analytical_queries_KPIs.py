import sqlite3

# --------------------------------------------------------------------
# 1. Primera consulta analítica
# --------------------------------------------------------------------
def fetch_accidents_by_collision_and_weather(cursor):
    """
    Fetch and print accident counts grouped by collision type and weather condition.
    """
    query = """
    SELECT 
        ct.collision_type, 
        c.weather, 
        COUNT(*) AS Accident_Count
    FROM FactCrash f
    JOIN DimCrashType ct ON f.crash_type_key = ct.crash_type_key
    JOIN DimCondition_Crash c ON f.condition_key_crash = c.condition_key_crash
    GROUP BY ct.collision_type, c.weather
    ORDER BY Accident_Count DESC
    LIMIT 10;
    """
    
    cursor.execute(query)
    records = cursor.fetchall()
    
    print("\n=== Top 10 Accident Types by Collision and Weather ===")
    print("Collision Type | Weather | Accident Count")
    print("-" * 50)
    for record in records:
        print(f"{record[0]} | {record[1]} | {record[2]}")

# Connect to CrashDW database
print("\nQuerying CrashDW database...")
crash_conn = sqlite3.connect("data/crashDW.db")
crash_cursor = crash_conn.cursor()

# Execute the query
fetch_accidents_by_collision_and_weather(crash_cursor)

# Close the connection
crash_conn.close()


# --------------------------------------------------------------------
# 2. Segunda consulta analítica
# --------------------------------------------------------------------
def accidentes_por_mes():
    """
    Consulta el número total de accidentes por mes en la base de datos CrashDW.
    """
    # Conectar a la base de datos
    crash_conn = sqlite3.connect("data/crashDW.db")
    crash_cursor = crash_conn.cursor()

    # Ejecutar la consulta
    query = """
    SELECT dt.year, dt.month, COUNT(*) AS total_accidentes
    FROM DimDateTime_Crash dt
    JOIN FactCrash fc ON dt.date_key_crash = fc.date_key_crash
    GROUP BY dt.year, dt.month
    ORDER BY dt.year, dt.month;
    """
    
    crash_cursor.execute(query)
    records = crash_cursor.fetchall()

    # Imprimir resultados
    print("\n=== Número total de accidentes por mes ===")
    print("Año | Mes | Total de Accidentes")
    for record in records:
        print(f"{record[0]} | {record[1]} | {record[2]}")

    # Cerrar conexión
    crash_conn.close()

# Ejecutar la función
accidentes_por_mes()


# --------------------------------------------------------------------
# 3. Tercera consulta analítica
# --------------------------------------------------------------------
def promedio_vehiculos_por_accidente():
    """
    Calcula el promedio de vehículos involucrados en cada accidente en la base de datos CrashDW.
    """
    # Conectar a la base de datos
    crash_conn = sqlite3.connect("data/crashDW.db")
    crash_cursor = crash_conn.cursor()

    # Ejecutar la consulta
    query = """
    SELECT AVG(num_vehicles_involved) AS promedio_vehiculos
    FROM FactCrash;
    """
    
    crash_cursor.execute(query)
    record = crash_cursor.fetchone()

    # Imprimir resultado
    print("\n=== Promedio de vehículos involucrados por accidente ===")
    print(f"Promedio de vehículos por accidente: {record[0]:.2f}")

    # Cerrar conexión
    crash_conn.close()

# Ejecutar la función
promedio_vehiculos_por_accidente()


# --------------------------------------------------------------------
# 4. Cuarta consulta analítica
# --------------------------------------------------------------------
def heridos_y_fallecidos_por_anio():
    """
    Calcula el número total de heridos y fallecidos por año en la base de datos CrashDW.
    """
    # Conectar a la base de datos
    crash_conn = sqlite3.connect("data/crashDW.db")
    crash_cursor = crash_conn.cursor()

    # Ejecutar la consulta
    query = """
    SELECT dt.year, 
           SUM(fc.num_injuries) AS total_heridos, 
           SUM(fc.num_fatalities) AS total_fallecidos
    FROM DimDateTime_Crash dt
    JOIN FactCrash fc ON dt.date_key_crash = fc.date_key_crash
    GROUP BY dt.year
    ORDER BY dt.year;
    """
    
    crash_cursor.execute(query)
    records = crash_cursor.fetchall()

    # Imprimir resultados
    print("\n=== Número total de heridos y fallecidos por año ===")
    print("Año | Total Heridos | Total Fallecidos")
    for record in records:
        print(f"{record[0]} | {record[1]} | {record[2]}")

    # Cerrar conexión
    crash_conn.close()

# Ejecutar la función
heridos_y_fallecidos_por_anio()

# --------------------------------------------------------------------
# 5. Quinta consulta analítica
# --------------------------------------------------------------------

def vehiculos_mas_involucrados():
    """
    Obtiene los modelos de vehículos más involucrados en accidentes en la base de datos VehicleDW.
    """
    # Conectar a la base de datos
    vehicle_conn = sqlite3.connect("data/vehicleDW.db")
    vehicle_cursor = vehicle_conn.cursor()

    # Ejecutar la consulta
    query = """
    SELECT v.vehicle_make, v.vehicle_model, COUNT(fv.fact_vehicle_id) AS total_accidentes
    FROM DimVehicle v
    JOIN FactVehicleInvolment fv ON v.vehicle_key = fv.vehicle_key
    GROUP BY v.vehicle_make, v.vehicle_model
    ORDER BY total_accidentes DESC
    LIMIT 10;
    """
    
    vehicle_cursor.execute(query)
    records = vehicle_cursor.fetchall()

    # Imprimir resultados
    print("\n=== Vehículos más involucrados en accidentes ===")
    print("Marca | Modelo | Total de Accidentes")
    for record in records:
        print(f"{record[0]} | {record[1]} | {record[2]}")

    # Cerrar conexión
    vehicle_conn.close()

# Ejecutar la función
vehiculos_mas_involucrados()
