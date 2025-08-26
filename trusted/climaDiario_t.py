from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import DoubleType, IntegerType
import utils as utils
import sys
import os

# --------------------------
# Función para reemplazar comas por puntos en columnas string numéricas
# --------------------------
def replace_commas_with_dots(df, columns):
    """
    Reemplaza ',' por '.' en columnas especificadas y convierte a DoubleType.
    """
    for c in columns:
        if c in df.columns:
            df = df.withColumn(c, regexp_replace(col(c), ",", ".").cast(DoubleType()))
    return df

if __name__ == "__main__":
    # 1. Crear sesión Spark
    spark = utils.create_context()

    # Tablas (ojo al catálogo)
    # Ajusta estos nombres según tu configuración
    db_name = "trusted_db"
    tbl_trusted = "aemetTrustedDiario"

    #nombre de la base de datos y tabla raw
    landing_db = "landing_db"
    tbl_landing = "aemetRawDiario"
    print(f" Leyendo tabla RAW: spark_catalog.{landing_db}.{tbl_landing}")

    #Leemos los datos de la tabla iceberg
    ##df_raw= utils.read_iceberg_table(spark,db_name=landing_db,table_name=tbl_landing)
    df_raw= spark.read.format("iceberg").load(f"spark_catalog.{landing_db}.{tbl_landing}")

    #hacemos una limpieza básica
    print("Reemplazando comas a puntos en columnas numéricas...")
    df_clean = replace_commas_with_dots(df_raw, ["tmed", "prec", "tmin", "tmax", "dir", "velmedia", "racha"])
    #print("Mostrando 10 registros de CLEAN:")
    #df_clean.show(10, truncate=False)


    # Escribir la tabla limpia en Iceberg
    print(f"Guardando datos limpios en Iceberg: {db_name}.{tbl_trusted}")
    utils.overwrite_iceberg_table(spark, df_clean, db_name=db_name, table_name=tbl_trusted)

    spark.stop()
    # print(" Proceso completado")