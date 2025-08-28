from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import utilsJoaquim as utils

# 1) Re-use the same Iceberg-aware session for read & write
spark = utils.create_context()
spark.sparkContext.setLogLevel("ERROR")

# Read landing-zone Iceberg table
db_name = "landing"
table_name = "puertos_turismo"
print(f"→ Reading spark_catalog.{db_name}.{table_name}")

df = utils.read_iceberg_table(spark, db_name, table_name)

# 2) Clean & normalize
df_clean = df.dropna()

# 🔹 Replace values in 'Autoridad portuaria'
df_clean = df_clean.withColumn(
    "AUT_PORTUARIA",
    when(col("AUT_PORTUARIA") == "BARCELONA", "Barcelona")
    .when(col("AUT_PORTUARIA") == "VALENCIA", "Valencia")
    .when(col("AUT_PORTUARIA") == "SEVILLA", "Sevilla")
    .when(col("AUT_PORTUARIA") == "BALEARES", "Illes Balears")
    .otherwise(col("AUT_PORTUARIA"))
)

# 4) Write into trusted zone (single table with only these provinces)
tgt_db, tgt_tbl = "trusted", "puertos_turismo_selected"
print(f"→ Writing spark_catalog.{tgt_db}.{tgt_tbl}")
utils.overwrite_iceberg_table(spark, df_clean, tgt_db, tgt_tbl)

df_clean.show()

print("✅ Trusted load complete.")

spark.stop()
