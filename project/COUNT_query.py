import sqlite3
import pandas as pd

def analyze_database_tables(db_path, db_name):
    """
    Analiza todas las tablas en una base de datos SQLite y retorna sus conteos
    
    Args:
        db_path (str): Ruta al archivo de la base de datos
        db_name (str): Nombre descriptivo de la base de datos
        
    Returns:
        list: Lista de diccionarios con información de cada tabla
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Obtenemos todas las tablas de la base de datos
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    table_stats = []
    
    for table in tables:
        table_name = table[0]
        
        # Contamos registros
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        
        # Obtenemos información de columnas
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        num_columns = len(columns)
        
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
    # Definimos las bases de datos a analizar
    databases = [
        {
            'path': 'data/crashDW.db',
            'name': 'CrashDW'
        },
        {
            'path': 'data/vehicleDW.db',
            'name': 'VehicleDW'
        }
    ]
    
    # Recolectamos estadísticas de todas las bases de datos
    all_stats = []
    for db in databases:
        try:
            stats = analyze_database_tables(db['path'], db['name'])
            all_stats.extend(stats)
        except Exception as e:
            print(f"Error al analizar {db['name']}: {str(e)}")
    
    # Imprimimos el resumen
    print_database_summary(all_stats)

if __name__ == "__main__":
    main()