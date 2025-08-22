from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import DoubleType, IntegerType
import utils as utils
import sys
import os
def check_spark_alive(spark):
    try:
        _ = spark.version  # Fuerza acceso a la sesión
        return True
    except Exception as e:
        print(f"❌ Spark no está disponible: {e}")
        return False

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
    # 1️⃣ Crear sesión Spark
    spark = utils.create_context_trusted()  # 👈 usa el nuevo contexto con catálogo propio y rutas cortas

    # Tablas (ojo al catálogo)
    # Tablas (ojo al catálogo)
    # RAW: si tu RAW está en otro catálogo, accédelo con su nombre completo (ajústalo a tu caso real).
    # Ejemplo si el RAW está en spark_catalog: spark.table("local_db.aemetRawDiario")
    # o si también está en trustedcat: spark.table("trustedcat.local_db.aemetRawDiario")
    print("📥 Leyendo tabla RAW: spark_catalog.local_db.aemetRawDiario")
    # 1️⃣ Crear la DB en el catálogo spark_catalog si no existe
    spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.local_db")

    spark.sql("""
    CREATE TABLE IF NOT EXISTS spark_catalog.local_db.aemetRawDiario
    USING ICEBERG
    LOCATION './data/warehouse/local_db/aemetRawDiario'
    """)

    df_raw = spark.read.table("spark_catalog.local_db.aemetRawDiario")  # si es spark_catalog por defecto
    # Si no, usa: df_raw = spark.table("trustedcat.local_db.aemetRawDiario")
    print(f"📊 Registros RAW: {df_raw.count()}")
    #confirmar que la tabla existe
    spark.sql("SHOW DATABASES IN spark_catalog").show()
    spark.sql("SHOW TABLES IN spark_catalog.local_db").show()

    # numeric_cols = ["tmed","prec","tmin","tmax","velmedia","racha","hrMedia","hrMax","hrMin","altitud"]
    # print("🔄 Normalizando decimales y casteando a Double...")
    # df_clean = replace_commas_with_dots(df_raw, numeric_cols)

    # # Test previo con subset en Parquet (ruta corta)
    # TEST_PATH = r"C:\parq_test_trusted"
    # os.makedirs(TEST_PATH, exist_ok=True)
    # print(f"📝 Test Parquet (1000 filas) en: {TEST_PATH}")
    # df_sample = df_clean.limit(1000)
    # df_sample.repartition(4).write.mode("overwrite").parquet(TEST_PATH)
    # print("✅ Test Parquet OK")

    # # Repartición segura antes de escribir Iceberg
    # df_trusted = df_clean.repartition(8)

    # # Escribe la tabla Iceberg en el catálogo 'trustedcat'
    # db_name = "trusted"
    # tbl_name = "aemetTrustedDiario"
    # full_tbl = f"trustedcat.{db_name}.{tbl_name}"

    # print(f"💾 Sobrescribiendo tabla Iceberg: {full_tbl}")
    # # Opción 1: DataFrameWriter V2 (recomendado)
    # (
    #     df_trusted.writeTo(full_tbl)
    #     .option("overwrite-mode", "dynamic")
    #     .createOrReplace()
    # )
    # print("✅ Tabla Iceberg guardada correctamente")

    spark.stop()
    # print("🏁 Proceso completado")