from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import utilsJoaquim as utils

# 1) Re-use the same Iceberg-aware session for read & write
spark = utils.create_context()
spark.sparkContext.setLogLevel("ERROR")

# Read landing-zone Iceberg tables
db_name = "landing"
tbl_main = "turismo_Emisor"
tbl_balears = "turismo_Emisor_Balears"

print(f"→ Reading spark_catalog.{db_name}.{tbl_main}")
df_main = utils.read_iceberg_table(spark, db_name, tbl_main)

print(f"→ Reading spark_catalog.{db_name}.{tbl_balears}")
df_balears = utils.read_iceberg_table(spark, db_name, tbl_balears)

# 2) Clean & normalize (drop nulls)
df_main_clean = df_main.dropna()
df_balears_clean = df_balears.dropna()

# Normalize València to "Valencia"
df_main_clean = df_main_clean.withColumn(
    "MUNICIPIO_ORIGEN",
    when(col("MUNICIPIO_ORIGEN") == "València", "Valencia")
    .otherwise(col("MUNICIPIO_ORIGEN"))
)

# Normalize Palma to "Palma de Mallorca"
df_balears_clean = df_balears_clean.withColumn(
    "MUNICIPIO_ORIGEN",
    when(col("MUNICIPIO_ORIGEN") == "Palma", "Palma de Mallorca")
    .otherwise(col("MUNICIPIO_ORIGEN"))
)

# 3) Union of both DataFrames
df_union = df_main_clean.unionByName(df_balears_clean)

# 4) Municipalities of interest
municipios = ["Madrid", "Barcelona", "Sevilla", "Palma de Mallorca", "Valencia"]

# Check municipality by municipality
for mun in municipios:
    count = df_union.filter(col("MUNICIPIO_ORIGEN") == mun).count()
    if count > 0:
        print(f"✅ Found {count} rows for {mun}")
    else:
        print(f"⚠️ No data found for {mun}")

# 5) Create final filtered DataFrame
df_filtered = df_union.filter(col("MUNICIPIO_ORIGEN").isin(municipios))

df_filtered.show()

# 6) Write into trusted zone
tgt_db, tgt_tbl = "trusted", "turismo_Emisor_selected"
print(f"→ Writing spark_catalog.{tgt_db}.{tgt_tbl}")
utils.overwrite_iceberg_table(spark, df_filtered, tgt_db, tgt_tbl)

print("✅ Trusted load complete.")

spark.stop()
