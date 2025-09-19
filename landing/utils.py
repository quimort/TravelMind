import pyspark
from pyspark.sql import SparkSession,DataFrame
import requests
import json 
from io import BytesIO
import pandas as pd
import os
import sys
from io import StringIO

def create_context() -> SparkSession:

    # Usa el mismo intérprete que el kernel del notebook
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark = SparkSession.builder\
        .appName("IcebergWritedata") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "./data/warehouse") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")\
        .config("spark.hadoop.parquet.enable.summary-metadata", "false")\
        .config("spark.hadoop.fs.localfile.impl.disable.cache", "true")\
        .getOrCreate()
    
    return spark


def get_api_endpoint_excel(spark:SparkSession,path:str,filter:str = None) -> DataFrame:

    if filter:
        response = requests.get(f"{path}?{filter}")
    else:
        response = requests.get(f"{path}")
    if response.status_code == 200:
        # Leer el archivo Excel con pandas desde memoria
        excel_file = BytesIO(response.content)
        df_pandas = pd.read_excel(excel_file)

        # Convertir el DataFrame de pandas a Spark
        df_spark = spark.createDataFrame(df_pandas)

        # Mostrar los primeros registros
        return df_spark
    else:
        print(f"Error {response.status_code}: no se pudo obtener el archivo.")

def get_api_endpoint_data(spark: SparkSession, path: str, filter: str = None) -> 'DataFrame':

    # Construir URL
    url = f"{path}?{filter}" if filter else path
    response = requests.get(url)

    # Verificar éxito
    if response.status_code != 200:
        raise Exception(f"Error {response.status_code}: no se pudo obtener los datos. Detalles: {response.text}")

    # Decodificar como texto con encoding adecuado
    try:
        text = response.content.decode('latin-1')  # Asume encoding ISO-8859-1 para caracteres acentuados
    except Exception as e:
        raise ValueError(f"Error al decodificar contenido: {e}")

    # Leer CSV con Pandas
    try:
        df_pandas = pd.read_csv(
            StringIO(text),
            sep=';',              # separador punto y coma
            decimal=',',          # coma como decimal
            encoding='latin-1',   # asegurar lectura de caracteres especiales
            parse_dates=False     # desactivar parse automático de fechas
        )
    except Exception as e:
        raise ValueError(f"Error al leer CSV: {e}")

    # Convertir a Spark DataFrame
    try:
        df_spark = spark.createDataFrame(df_pandas)
        return df_spark
    except Exception as e:
        raise ValueError(f"Error al convertir a Spark DataFrame: {e}")

def get_api_endpoint_excel_data(spark: SparkSession, path: str, filter: str = None) -> 'DataFrame':
    """
    Fetch data from an API endpoint that returns an Excel (.xlsx) file,
    and convert it to a Spark DataFrame.
    
    Parameters:
        spark : SparkSession
            The Spark session to use.
        path : str
            The API endpoint URL.
        filter : str, optional
            Optional query string to append to the URL.
    
    Returns:
        Spark DataFrame
    """

    # Build the full URL
    url = f"{path}?{filter}" if filter else path
    response = requests.get(url)

    # Check for successful response
    if response.status_code != 200:
        raise Exception(f"Error {response.status_code}: no se pudo obtener los datos. Detalles: {response.text}")

    # Load Excel into pandas
    try:
        excel_file = BytesIO(response.content)
        df_pandas = pd.read_excel(excel_file, engine='openpyxl')  # explicitly use openpyxl engine
    except Exception as e:
        raise ValueError(f"Error al leer Excel: {e}")

    # Convert to Spark DataFrame
    try:
        df_spark = spark.createDataFrame(df_pandas)
        return df_spark
    except Exception as e:
        raise ValueError(f"Error al convertir a Spark DataFrame: {e}")


def overwrite_iceberg_table(spark:SparkSession,df:DataFrame,db_name:str,table_name:str):

    spark.sql(f"CREATE DATABASE IF NOT EXISTS spark_catalog.{db_name}")
    # Guardar tabla Iceberg
    df.writeTo(f"spark_catalog.{db_name}.{table_name}").using("iceberg").createOrReplace()

# def append_iceberg_table(spark:SparkSession,df:DataFrame,db_name:str,table_name:str):

#     if check_table_exists(spark,db_name,table_name):
#         spark.sql(f"CREATE DATABASE IF NOT EXISTS spark_catalog.{db_name}")
#         # Guardar tabla Iceberg
#         df.writeTo(f"spark_catalog.{db_name}.{table_name}").using("iceberg").append()
#     else:
#         overwrite_iceberg_table(spark,df,db_name,table_name)

def append_iceberg_table(spark: SparkSession, df:DataFrame, db_name: str, table_name: str):
    """
    Inserta datos en una tabla Iceberg. 
    - Si la tabla existe → hace append. 
    - Si no existe → la crea.
    """
    full_name = f"spark_catalog.{db_name}.{table_name}"
    #Crear base de datos si no existe
    spark.sql(f"CREATE DATABASE IF NOT EXISTS spark_catalog.{db_name}")

    if check_table_exists(spark, db_name, table_name):
        print(f"\n📥 Tabla '{full_name}' existe → haciendo APPEND...")
        df.writeTo(full_name).using("iceberg").append()
    else:
        #tabla no existe -> crearla
        print(f"\n🆕 Tabla '{full_name}' no existe → creando tabla...")
        overwrite_iceberg_table(spark,df,db_name,table_name)
    
    print(f"\n✅ Datos guardados en la tabla Iceberg: {full_name}")

def merge_iceberg_table(spark:SparkSession,df:DataFrame,db_name:str,table_name:str,primary_key:list):

    if check_table_exists(spark,db_name,table_name):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS spark_catalog.{db_name}")
        df.createOrReplaceTempView(f"{table_name}_updates")
        merge_condition = create_merge_condition(primary_key)
        # Guardar tabla Iceberg
        spark.sql(f"""
            MERGE INTO spark_catalog.{db_name}.{table_name} AS target
            USING {table_name}_updates AS source
            ON {merge_condition}
            WHEN MATCHED THEN
            UPDATE SET *
            WHEN NOT MATCHED THEN
            INSERT *
        """)
    else:
        overwrite_iceberg_table(spark,df,db_name,table_name)

def read_iceberg_table(spark:SparkSession,db_name:str,table_name:str)-> DataFrame:

    df = spark.read.table(f"spark_catalog.{db_name}.{table_name}")

    return df

def create_merge_condition(primary_key:list) -> str:

    compare = "target.# = source.#"

    condition = "( "
    for pk in primary_key:
        condition = condition + compare.replace("#",pk) + ") AND ("
    condition = condition[:-5]
    return condition

# def check_table_exists(spark:SparkSession,db_name:str,table_name:str)->bool:

#     return spark.catalog.tableExists(tableName=table_name,dbName=db_name)

def check_table_exists(spark: SparkSession, db_name: str, table_name: str) -> bool:
    """
    Verifica si una tabla Iceberg existe en el catálogo de Spark.
    Retorna True si existe, False en caso contrario.
    """
    full_name = f"{db_name}.{table_name}"
    try:
        return spark.catalog.tableExists(full_name)
    except Exception as e:
        print(f"⚠️ No se pudo verificar la tabla {full_name}: {e}")
        return False