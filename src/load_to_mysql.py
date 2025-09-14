import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text, types
from dotenv import load_dotenv

# --- CONFIGURACIÓN DE RUTAS ---
# Esto hace el script robusto, encontrando la raíz del proyecto automáticamente
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

def main():
    """
    Función principal para conectar a MySQL, limpiar y cargar los datos de salarios.
    """
    # --- 1. CARGAR CONFIGURACIÓN SEGURA ---
    print("Cargando credenciales desde el archivo .env...")
    dotenv_path = os.path.join(project_root, '.env')
    load_dotenv(dotenv_path=dotenv_path)

    db_host = os.getenv("DB_HOST")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_name = os.getenv("DB_NAME")

    # Verificación de que las credenciales se cargaron correctamente
    if not all([db_host, db_user, db_pass, db_name]):
        print("\n*** ERROR: Faltan variables de entorno. Asegúrate de que tu archivo .env está en la raíz del proyecto y contiene todas las credenciales. ***")
        return

    # --- 2. LEER Y PREPARAR DATOS ---
    csv_path = os.path.join(project_root, 'data', 'processed', 'cleaned_cyber.csv')
    print(f"Leyendo archivo de datos desde: {csv_path}")

    try:
        df = pd.read_csv(csv_path)
        # CORRECCIÓN: Renombramos las columnas del CSV para que coincidan con el estándar de la BD
        # Esto soluciona los errores 'raw_id' y 'pay_in_'
        df.rename(columns={'raw_id': 'row_id', 'pay_in_': 'pay_in'}, inplace=True)
        print("Archivo CSV leído y columnas renombradas exitosamente.")
    except FileNotFoundError:
        print(f"\n*** ERROR: No se encontró el archivo CSV en la ruta: {csv_path} ***")
        return
    except Exception as e:
        print(f"\n*** ERROR inesperado al leer el CSV: {e} ***")
        return
        
    # --- 3. CONECTAR Y CARGAR A MYSQL ---
    try:
        connection_string = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
        engine = create_engine(connection_string)
        
        table_name = 'cyber_salaries'
        print(f"Conectando a la base de datos '{db_name}'...")

        # Definimos los tipos de datos para la tabla SQL para un control preciso
        # CORRECCIÓN: 'row_id' se define como VARCHAR para aceptar texto como 'J2000'
        sql_types = {
            'row_id': types.VARCHAR(255),
            'rank_title': types.VARCHAR(255),
            'role_normalized': types.VARCHAR(255),
            'company_std': types.VARCHAR(255),
            'location': types.VARCHAR(255),
            'location_std': types.VARCHAR(255),
            'pay_in': types.VARCHAR(50),
            'salary_clean': types.DECIMAL(15, 2),
            'salary_band': types.VARCHAR(50),
            'experience_level': types.VARCHAR(50),
            'years_experience_num': types.INTEGER
        }

        # Usamos df.to_sql - la forma profesional de subir DataFrames a SQL
        print(f"Cargando {len(df)} filas en la tabla '{table_name}'. Esto puede tardar un momento...")
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace', # Reemplaza la tabla si ya existe (ideal para desarrollo)
            index=False,         # No incluir el índice de Pandas como columna
            dtype=sql_types      # Aplica nuestros tipos de datos definidos
        )
        
        print("\n¡Carga completada exitosamente!")

        # Verificación final: Contar las filas insertadas
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name};"))
            count = result.scalar()
            print(f"Verificación: La tabla '{table_name}' ahora contiene {count} filas.")

    except Exception as e:
        print(f"\n*** ERROR durante la conexión o carga a MySQL: {e} ***")


if __name__ == '__main__':
    main()