from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import pandas as pd
import psycopg2  # Ejemplo con Postgres, ajusta según tu BD

# Configuración básica del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='etl_crash',
    description='ETL para cargar datos de Crash Reporting en modelo estrella',
    default_args=default_args,
    schedule_interval=None,  # o la que desees: '@daily', etc.
    start_date=datetime(2025, 1, 25, 0, 0, 0),  # año, mes, día, hora, minuto, segundo
    catchup=False
) as dag:

    def extract_data(**kwargs):
        """
        1. Leer el archivo CSV/Excel original y almacenarlo en staging (DataFrame o tabla temporal).
        """
        # Ruta al archivo local o remota (S3, etc.)
        file_path = 'Crash_Reporting_-_Drivers_Data.csv'  
        df = pd.read_csv(file_path)

        # Pequeña limpieza inicial (ejemplo):
        df.columns = [c.strip() for c in df.columns]  # Quitar espacios de columnas
        # Reemplazar 'N/A' o '' vacíos por None
        df.replace(['N/A', 'UNKNOWN', ''], [None, None, None], inplace=True)

        # Se puede retornar el DataFrame completo con XCom (cuidado con tamaños grandes),
        # o guardarlo en una tabla staging en la BD. Aquí mostramos XCom para demo:
        return df.to_dict(orient='records')

    def transform_dimensions(**kwargs):
        """
        2. Transformar y cargar las dimensiones:
           - DimDateTime_Crash
           - DimLocation_Crash
           - DimDriver
           - DimVehicle
           - DimCondition_Crash
           - DimCrashType
        """
        # Recuperar los registros desde XCom
        ti = kwargs['ti']
        records = ti.xcom_pull(task_ids='extract_data')
        df = pd.DataFrame(records)

        # -------------------------------------------------
        #   EJEMPLO DE CONEXIÓN A BD (Postgres)
        # -------------------------------------------------
        conn = psycopg2.connect(
            dbname='mi_bd',
            user='mi_usuario',
            password='mi_password',
            host='mi_host',
            port='5432'
        )
        conn.autocommit = True
        cur = conn.cursor()

        # -------------------------------------------------
        #   DimDateTime_Crash (y/o DimDateTime_Veh)
        # -------------------------------------------------
        # Parseamos la fecha/hora de "Crash Date/Time"
        df['Crash Datetime'] = pd.to_datetime(df['Crash Date/Time'], errors='coerce')

        # Aquí un ejemplo rápido de cómo generar columnas
        df['year'] = df['Crash Datetime'].dt.year
        df['month'] = df['Crash Datetime'].dt.month
        df['day'] = df['Crash Datetime'].dt.day
        df['hour'] = df['Crash Datetime'].dt.hour
        df['day_of_week'] = df['Crash Datetime'].dt.dayofweek
        df['am_pm'] = df['Crash Datetime'].dt.strftime('%p')  # AM/PM

        # Insertar/Upsert en la dimensión. Por simplicidad mostramos un INSERT:
        for index, row in df.iterrows():
            date_value_str = row['Crash Datetime'].strftime('%Y-%m-%d %H:%M:%S') if row['Crash Datetime'] else None

            # Si tu PK es un surrogate autoincrement, no necesitas generarlo aquí.
            # Ejemplo de INSERT para DimDateTime_Crash:
            insert_dim_datetime = """
            INSERT INTO DimDateTime_Crash(date_value, year, month, day, hour, day_of_week, am_pm)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_value) DO NOTHING;  -- o manejo de upsert distinto
            """
            cur.execute(insert_dim_datetime, (
                date_value_str, 
                row['year'],
                row['month'],
                row['day'],
                row['hour'],
                row['day_of_week'],
                row['am_pm']
            ))
        
        # -------------------------------------------------
        #   DimLocation_Crash
        # -------------------------------------------------
        # Ejemplo: Insertar "Route Type", "Road Name", "Cross-Street Name", "Municipality", lat/long, etc.
        # Mismo patrón. Aquí, solamente un ejemplo de 1 fila:
        # En la práctica, querrás agrupar filas únicas (evitar duplicados).
        # ...
        # Supón que "Location" sea la columna con lat/long o "Municipality" etc.

        # -------------------------------------------------
        #   DimDriver
        # -------------------------------------------------
        # Supongamos que la "natural key" es "Person ID"
        # ...
        # Ejemplo:
        # for idx, row in df.iterrows():
        #     insert_driver = """
        #       INSERT INTO DimDriver(person_id, driver_substance_abuse, ...)
        #       VALUES (%s, %s, ...)
        #       ON CONFLICT (person_id) DO UPDATE ...  -- si deseas upsert
        #     """
        #     cur.execute(insert_driver, (row['Person ID'], row['Driver Substance Abuse'], ...))

        # Continúa con Vehicle, Condition, CrashType, etc.
        # ...

        cur.close()
        conn.close()

        # Retornamos el DataFrame completo (con las columnas parseadas) para uso posterior
        return df.to_dict(orient='records')

    def transform_fact_crash(**kwargs):
        """
        3. Cargar la tabla de hechos FactCrash.
           Aquí unimos las claves de dimensiones (buscándolas en base).
        """
        ti = kwargs['ti']
        records = ti.xcom_pull(task_ids='transform_dimensions')
        df = pd.DataFrame(records)

        conn = psycopg2.connect(
            dbname='mi_bd',
            user='mi_usuario',
            password='mi_password',
            host='mi_host',
            port='5432'
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Ejemplo: para cada "Report Number", inserta un registro en FactCrash.
        # Debes buscar/armar: date_key_crash, location_key_crash, condition_key_crash, crash_type_key, etc.
        # Por simplificar, asumimos que harás sub-consultas o JOIN con la BD para obtener los surrogate keys.

        for index, row in df.iterrows():
            # 1) Hallar la key de fecha
            #    Por ej., si DimDateTime_Crash tiene un PK identity "date_key_crash"
            #    y la "natural key" era "date_value", hacemos un lookup:
            select_datekey = """
                SELECT date_key_crash 
                FROM DimDateTime_Crash 
                WHERE date_value = %s
            """
            date_value_str = row['Crash Datetime'].strftime('%Y-%m-%d %H:%M:%S') if row['Crash Datetime'] else None
            cur.execute(select_datekey, (date_value_str,))
            res = cur.fetchone()
            date_key_crash = res[0] if res else None

            # 2) location_key_crash, condition_key_crash, crash_type_key, etc. con lógica similar
            # ...
            location_key_crash = 1  # placeholder
            condition_key_crash = 1  # placeholder
            crash_type_key = 1       # placeholder

            # Contar num_vehicles_involved, injuries, etc.
            # De momento ponemos placeholders.
            num_vehicles_involved = 1
            num_injuries = 0
            num_fatalities = 0

            insert_fact_crash = """
            INSERT INTO FactCrash(
                date_key_crash, location_key_crash, condition_key_crash, crash_type_key,
                num_vehicles_involved, num_injuries, num_fatalities, report_number
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(insert_fact_crash, (
                date_key_crash,
                location_key_crash,
                condition_key_crash,
                crash_type_key,
                num_vehicles_involved,
                num_injuries,
                num_fatalities,
                row['Report Number']
            ))

        cur.close()
        conn.close()

    def transform_fact_vehicle(**kwargs):
        """
        4. Carga de la tabla FactVehicleInvolment.
        """
        ti = kwargs['ti']
        records = ti.xcom_pull(task_ids='transform_dimensions')
        df = pd.DataFrame(records)

        conn = psycopg2.connect(
            dbname='mi_bd',
            user='mi_usuario',
            password='mi_password',
            host='mi_host',
            port='5432'
        )
        conn.autocommit = True
        cur = conn.cursor()

        for index, row in df.iterrows():
            # 1) Buscar date_key_vehicle en DimDateTime_Veh (similar a lo anterior)
            date_key_vehicle = 1
            # 2) location_key_vehicle en DimLocation_Veh, etc.
            location_key_vehicle = 1
            # 3) driver_key (lookup en DimDriver usando Person ID)
            driver_key = 1
            # 4) vehicle_key (lookup en DimVehicle, usando Vehicle ID o (Year, Make, Model)...)

            # 5) Extraer drive_at_fault_flag, circumstance, injury_security, etc.
            drive_at_fault_flag = 1 if row['Driver At Fault'] == 'Yes' else 0
            injury_security = row['Injury Severity'] or None
            circumstance = row['Circumstance'] or None

            insert_fact_vehicle = """
            INSERT INTO FactVehicleInvolment(
                date_key_vehicle, location_key_vehicle, driver_key, vehicle_key, 
                injury_security, drive_at_fault_flag, circumstance
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(insert_fact_vehicle, (
                date_key_vehicle,
                location_key_vehicle,
                driver_key,
                1, # vehicle_key placeholder
                injury_security,
                drive_at_fault_flag,
                circumstance
            ))

        cur.close()
        conn.close()

    # =========================================================================
    # TAREAS (TASKS) DE NUESTRO DAG
    # =========================================================================
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True
    )

    transform_dim_task = PythonOperator(
        task_id='transform_dimensions',
        python_callable=transform_dimensions,
        provide_context=True
    )

    transform_fact_crash_task = PythonOperator(
        task_id='transform_fact_crash',
        python_callable=transform_fact_crash,
        provide_context=True
    )

    transform_fact_vehicle_task = PythonOperator(
        task_id='transform_fact_vehicle',
        python_callable=transform_fact_vehicle,
        provide_context=True
    )

    # Definir la secuencia:
    # 1) extract_data -> 2) transform_dimensions -> 3) fact_crash y fact_vehicle
    extract_task >> transform_dim_task >> [transform_fact_crash_task, transform_fact_vehicle_task]

