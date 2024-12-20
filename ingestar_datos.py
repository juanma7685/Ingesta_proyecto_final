import dlt
import pandas as pd
import glob
import toml

# Base de datos y esquema definidos directamente en el script
DATABASE = "ALUMNO11_PRO_BRONZE_DB"

# Cargar las credenciales desde secrets.toml
def load_credentials():
    """
    Carga las credenciales desde el archivo secrets.toml y las almacena como secretas en DLT.
    """
    secrets_path = "secrets.toml"  # Asegúrate de que este archivo esté en el mismo directorio
    try:
        secrets = toml.load(secrets_path)
        snowflake_credentials = secrets["snowflake"]
        # Configurar las credenciales de destino como secretas en DLT
        dlt.secrets["destination.snowflake.credentials"] = snowflake_credentials
        print(f"Credenciales cargadas. Base de datos: {DATABASE}")
    except Exception as e:
        raise Exception(f"Error cargando credenciales desde {secrets_path}: {e}")

# Ruta a los archivos CSV generados
csv_folder = "./"  # Cambia si los archivos están en otra carpeta
csv_files = glob.glob(csv_folder + "*.csv")

# Diccionario para mapear archivos a tablas en los esquemas correspondientes
table_mapping = {
    "addresses.csv": "addresses",
    "users.csv": "users",
    "products.csv": "products",
    "promos.csv": "promos",
    "orders.csv": "orders",
    "events.csv": "events",
    "order_items.csv": "order_items",
    "budget.csv": "budget",
    "reviews.csv": "reviews",
    "returns.csv": "returns"
}

def load_csv_data():
    """
    Lee y devuelve los datos de los archivos CSV como recursos procesables.
    """
    resources = []
    for file_path in csv_files:
        # Obtener el nombre del archivo sin la ruta
        file_name = file_path.split("/")[-1]
        table_name = table_mapping.get(file_name)
        
        if table_name:
            print(f"Procesando archivo {file_name} para la tabla {table_name}...")
            
            # Leer el archivo CSV con pandas
            df = pd.read_csv(file_path)
            
            # Crear un recurso de DLT con los datos
            resources.append((df, table_name))
        else:
            print(f"Archivo {file_name} no mapeado. Skipping...")
    return resources

# Configuración del pipeline
def create_pipeline(table_name):
    """
    Configura y devuelve el pipeline de DLT.
    Si la tabla es `budget`, `reviews` o `returns`, se usa el dataset `google_sheets`.
    De lo contrario, se usa el dataset `sql_server_dbo`.
    """
    datasetName = "sql_server_dbo"
    datasetName = "google_sheets" if table_name in ["budget", "reviews", "returns"] else "sql_server_dbo"
    return dlt.pipeline(
        pipeline_name="bronze_ingestion_pipeline",
        destination="snowflake",  # Destino configurado para Snowflake
        dataset_name=datasetName  # Nombre del dataset dinámico
    )

if __name__ == "__main__":
    try:
        # Cargar las credenciales desde secrets.toml
        load_credentials()

        # Leer y procesar los archivos CSV
        csv_data = load_csv_data()

        # Procesar cada archivo y cargarlo en el pipeline
        for df, table_name in csv_data:
            try:
                # Crear el pipeline dinámicamente según la tabla
                pipeline = create_pipeline(table_name)  # Pasamos el nombre de la tabla
                
                # Crear un recurso con el nombre de la tabla
                resource = dlt.resource(
                    df.to_dict(orient="records"),  # Datos
                    name=table_name  # Nombre completo de la tabla
                )
                
                # Ejecutar el pipeline con el recurso
                pipeline.run([resource])
                print(f"Ingesta completada con éxito para la tabla {table_name} (Dataset: {pipeline.dataset_name})")
            except Exception as e:
                print(f"Error durante la ingesta de la tabla {table_name}: {e}")
    except Exception as e:
        print(f"Error durante la ingesta: {e}")
