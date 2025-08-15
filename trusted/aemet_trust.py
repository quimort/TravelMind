from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import DoubleType, IntegerType
import utils as utils
import sys
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
    spark = utils.create_context()

    # Parámetros de entrada y salida
    db_landing = "local_db"
    table_name_raw = "aemetRawDiario"
    db_name_trusted = "trusted"
    table_name_trusted = "aemetTrustedDiario"

    # 2️⃣ Leer tabla desde Iceberg (RAW)
    print(f"📥 Leyendo tabla RAW: {db_landing}.{table_name_raw}")
    df_raw = utils.read_iceberg_table(spark, db_landing, table_name_raw)

    # Lista de columnas a procesar (con comas decimales)
    numeric_cols_comma = [
        "tmed", "prec", "tmin", "tmax", "velmedia", "racha",
        "hrMedia", "hrMax", "hrMin", "altitud"
    ]

    # 3️⃣ Reemplazar comas por puntos
    print("🔄 Reemplazando comas por puntos en columnas numéricas...")
    df_clean = replace_commas_with_dots(df_raw, numeric_cols_comma)

    # 3. Confirmar Spark y tipo de DataFrame
    if not check_spark_alive(spark):
        print("⚠️ Abortando: Spark no está vivo")
        sys.exit(1)
        
    # Verificar que es DataFrame de Spark antes de guardar
    if isinstance(df_clean, DataFrame):
        print("✅ df_clean es un DataFrame de Spark")
    else:
        raise TypeError("❌ df_clean NO es un DataFrame de Spark")
    
    #mostrar esquema del DataFrame
    print("\nEsquema del DataFrame:")
    df_clean.printSchema()
    print(f"Número de filas: {df_clean.count()}")
    
    # desactivar Arrow para paqruet para evitar problemas de serialización
    print("🔧 Desactivando Arrow para evitar problemas de serialización..." )
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    # ---------------------------
    # 4. TEST DE ESCRITURA EN PARQUET
    # ---------------------------
    print("📝 Test de escritura en Parquet temporal...")
    try:
        df_clean.write.mode("overwrite").csv("tmp_test_csv")
        print("✅ Escritura correcta.")
    except Exception as e:
        print(f"❌ Error escribiendo datos: {e}")
        raise SystemExit("⚠️ Abortando por fallo en prueba local de escritura")

    # ---------------------------
    # 5. REDUCIR PARTICIONES ANTES DE GUARDAR
    # ---------------------------
    df_trusted = df_clean.coalesce(4)  # Puedes aumentar si la tabla es muy grande


    # 4️⃣ Confirmar que Spark sigue vivo antes de escribir en Iceberg
    try:
        spark_version = spark.version
        print(f"💡 Spark sigue activo. Versión: {spark_version}")
    except Exception as e:
        printutils.overwrite_iceberg_table(spark, df_clean, db_name=db_name_trusted, table_name=table_name_trusted)(f"❌ Spark no está disponible: {e}")
        raise SystemExit("⚠️ Abortando: sesión Spark finalizada.")
    # ---------------------------
    # 6. GUARDAR EN ICEBERG (tb_name_trusted)
    # ---------------------------
    print(f"💾 Guardando tabla TRUSTED: {db_name_trusted}.{table_name_trusted}")
    try:
        utils.overwrite_iceberg_table(spark, df_trusted,
                                    db_name=db_name_trusted,
                                    table_name=table_name_trusted)
        print("✅ Tabla Iceberg guardada correctamente.")
    except Exception as e:
        print(f"❌ Error escribiendo en Iceberg: {e}")
        raise
    
    #print(f"💾 Guardando tabla TRUSTED: {db_name_trusted}.{table_name_trusted}")
    #
    
    spark.stop()
    print("✅ Proceso completado.")
