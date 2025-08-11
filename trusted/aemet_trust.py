from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace
from pyspark.sql.types import DoubleType, IntegerType
import re
import utils as utils

def to_camel_case(snake_str):
    # Convierte snake_case o lowercase con guiones/bajos a camelCase
    components = re.split('[_ -]', snake_str)
    if not components:
        return snake_str
    return components[0].lower() + ''.join(x.title() for x in components[1:])

def clean_and_cast_trusted_zone(df):
    # Columnas numéricas con coma decimal
    numeric_cols_comma = [
        "tmed", "prec", "tmin", "tmax", "velmedia", "racha",
        "hrMedia", "hrMax", "hrMin", "altitud"
    ]
    
    # Columnas que pueden tener valores no numéricos ("Varias") y deben ser convertidas a null
    problematic_values = ["Varias"]
    
    for col_name in numeric_cols_comma:
        if col_name in df.columns:
            # Primero, limpiar valores problemáticos
            df = df.withColumn(
                col_name,
                when(~col(col_name).isin(problematic_values) & col(col_name).isNotNull(),
                     regexp_replace(col(col_name), ",", ".").cast(DoubleType())
                ).otherwise(None)
            )
    
    # Cambiar nombres a camelCase
    for old_name in df.columns:
        new_name = to_camel_case(old_name)
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)
    
    return df

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("TrustedZoneCleaner") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "C:/Users/varga/Desktop/MasterBIGDATA_BCN/Aulas/Proyecto/TFM/TravelMind/data/warehouse") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
        .getOrCreate()

    # Cargar tabla raw
    db_name = "local_db"
    raw_table = "aemetRawDiario"
    trusted_table = "aemetTrustedDiario"
    
    print(f"Cargando tabla raw: {db_name}.{raw_table}")
    df_raw = spark.table(f"{db_name}.{raw_table}")

    print("Limpiando y transformando datos para trusted zone...")
    df_trusted = clean_and_cast_trusted_zone(df_raw)

    print(f"Guardando tabla transformada en trusted zone: {db_name}.{trusted_table}")
    # Sobrescribir la tabla trusted
    df_trusted.write.format("iceberg").mode("overwrite").saveAsTable(f"{db_name}.{trusted_table}")

    print("Proceso completado.")
    spark.stop()
