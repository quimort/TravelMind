import pyspark
from pyspark.sql import SparkSession,DataFrame
import requests
import json 
from io import BytesIO
import pandas as pd
import os
import sys
from io import BytesIO, StringIO

def create_context() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TrustedCSVLoad")
        .config("spark.master", "local[*]")        # or your cluster master
        .config("spark.driver.memory", "2g")       # enough for CSV
        .getOrCreate()
    )

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



def overwrite_iceberg_table(spark:SparkSession,df:DataFrame,db_name:str,table_name:str):

    spark.sql(f"CREATE DATABASE IF NOT EXISTS spark_catalog.{db_name}")
    # Guardar tabla Iceberg
    df.writeTo(f"spark_catalog.{db_name}.{table_name}").using("iceberg").createOrReplace()

def append_iceberg_table(spark:SparkSession,df:DataFrame,db_name:str,table_name:str):

    if check_table_exists(spark,db_name,table_name):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS spark_catalog.{db_name}")
        # Guardar tabla Iceberg
        df.writeTo(f"spark_catalog.{db_name}.{table_name}").using("iceberg").append()
    else:
        overwrite_iceberg_table(spark,df,db_name,table_name)

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

def check_table_exists(spark:SparkSession,db_name:str,table_name:str)->bool:

    return spark.catalog.tableExists(tableName=table_name,dbName=db_name)