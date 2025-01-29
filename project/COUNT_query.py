import psycopg2
import pandas as pd

def analyze_database_tables(db_name, db_config):
    """
    Analiza todas las tablas en una base de datos PostgreSQL y retorna sus conteos
    
    Args:
        db_name (str): Nombre de la base de datos
        db_config (dict): Configuración de conexión a la base de datos
        
    Returns:
        list: Lista de diccionarios con información de cada tabla
    """
    # Creamos la conexión a PostgreSQL
    conn = psycopg2.connect(
        database=db_name,
        user=db_config['user'],
        password=db_config['password'],
        host=db_config['host'],
        port=db_config['port']
    )
    cursor = conn.cursor()
    
    # Obtenemos todas las tablas de la base de datos
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    tables = cursor.fetchall()
    
    table_stats = []
    
    for table in tables:
        table_name = table[0]
        
        # Contamos registros
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        
        # Obtenemos información de columnas
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = %s
        """, (table_name,))
        num_columns = cursor.fetchone()[0]
        
        # Guardamos estadísticas
        table_stats.append({
            'database': db_name,
            'table_name': table_name,
            'record_count': count,
            'column_count': num_columns
        })
    
    conn.close()
    return table_stats

def print_database_summary(stats):
    """
    Imprime un resumen formateado de las estadísticas de la base de datos
    
    Args:
        stats (list): Lista de estadísticas de tablas
    """
    # Creamos un DataFrame para mejor visualización
    df = pd.DataFrame(stats)
    
    # Agrupamos por base de datos para mostrar totales
    db_totals = df.groupby('database').agg({
        'record_count': 'sum',
        'table_name': 'count'
    }).rename(columns={'table_name': 'total_tables'})
    
    # Imprimimos el resumen general
    print("\n=== RESUMEN GENERAL DE BASES DE DATOS ===")
    for db_name, row in db_totals.iterrows():
        print(f"\n{db_name}:")
        print(f"  Total de tablas: {row['total_tables']}")
        print(f"  Total de registros: {row['record_count']:,}")
    
    # Imprimimos el detalle por tabla
    print("\n=== DETALLE POR TABLA ===")
    for db_name in df['database'].unique():
        print(f"\n{db_name}:")
        db_tables = df[df['database'] == db_name]
        for _, row in db_tables.iterrows():
            print(f"  {row['table_name']}:")
            print(f"    Registros: {row['record_count']:,}")
            print(f"    Columnas: {row['column_count']}")

def main():
    # Configuración de la base de datos
    DB_CONFIG = {
        'user': 'postgres',
        'password': '1234',
        'host': 'localhost',
        'port': '5433'
    }
    
    # Definimos las bases de datos a analizar
    databases = ['crashDW', 'vehicleDW']
    
    # Recolectamos estadísticas de todas las bases de datos
    all_stats = []
    for db_name in databases:
        try:
            stats = analyze_database_tables(db_name, DB_CONFIG)
            all_stats.extend(stats)
        except Exception as e:
            print(f"Error al analizar {db_name}: {str(e)}")
    
    # Imprimimos el resumen
    print_database_summary(all_stats)

if __name__ == "__main__":
    main()