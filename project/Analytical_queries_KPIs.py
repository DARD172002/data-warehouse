import sqlite3

def accidentes_por_tipo_colision():
    """
    Calcula la cantidad total de accidentes por tipo de colisión en la base de datos CrashDW.
    """
    # Conectar a la base de datos
    crash_conn = sqlite3.connect("data/crashDW.db")
    crash_cursor = crash_conn.cursor()

    # Ejecutar la consulta
    query = """
    SELECT ct.collision_type, COUNT(fc.fact_crash_id) AS total_accidentes
    FROM DimCrashType ct
    JOIN FactCrash fc ON ct.crash_type_key = fc.crash_type_key
    GROUP BY ct.collision_type
    ORDER BY total_accidentes DESC;
    """
    
    crash_cursor.execute(query)
    records = crash_cursor.fetchall()

    # Imprimir resultados
    print("\n=== Cantidad de accidentes por tipo de colisión ===")
    print("Tipo de Colisión | Total de Accidentes")
    for record in records:
        print(f"{record[0]} | {record[1]}")

    # Cerrar conexión
    crash_conn.close()

# Ejecutar la función
accidentes_por_tipo_colision()


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
