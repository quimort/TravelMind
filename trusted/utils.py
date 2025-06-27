import pyspark
from pyspark.sql import SparkSession,DataFrame
import requests
import json 
from io import BytesIO
import pandas as pd
import os
import sys

def create_context() -> SparkSession:

    # Usa el mismo intérprete que el kernel del notebook
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark = SparkSession.builder\
        .appName("IcebergWritedata") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "../data/warehouse") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
        .getOrCreate()
    
    return spark

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