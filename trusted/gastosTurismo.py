from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, col, when
import utils

# 1) Re-use the same Iceberg-aware session for read & write
spark = utils.create_context()
spark.sparkContext.setLogLevel("ERROR")

# Read landing-zone Iceberg table
db_name = "landing"
table_name = "gastos_Turismo"
print(f"→ Reading spark_catalog.{db_name}.{table_name}")

df = utils.read_iceberg_table(spark, db_name, table_name)

# 2) Clean & normalize
df_clean = df.dropna()

# Normalize province names (fix Valencia)
df_clean = df_clean.withColumn(
    "PROVINCIA_DESTINO",
    when(col("PROVINCIA_DESTINO") == "Valencia/València", "Valencia")
    .otherwise(col("PROVINCIA_DESTINO"))
)

df_clean = df_clean.withColumn(
    "PROVINCIA_ORIGEN",
    when(col("PROVINCIA_ORIGEN") == "Valencia/València", "Valencia")
    .otherwise(col("PROVINCIA_ORIGEN"))
)

# 3) Provinces of interest
provinces = ["Madrid", "Barcelona", "Sevilla", "Illes Balears", "Valencia"]

# Check province by province
for prov in provinces:
    count = df_clean.filter(col("PROVINCIA_DESTINO") == prov).count()
    if count > 0:
        print(f"✅ Found {count} rows for {prov}")
    else:
        print(f"⚠️ No data found for {prov}")

# Create final filtered DataFrame with all provinces
df_filtered = df_clean.filter(col("PROVINCIA_DESTINO").isin(provinces))

df_filtered.show()

# 4) Write into trusted zone (single table with only these provinces)
tgt_db, tgt_tbl = "trusted", "gastos_Turismo_selected"
print(f"→ Writing spark_catalog.{tgt_db}.{tgt_tbl}")
utils.overwrite_iceberg_table(spark, df_filtered, tgt_db, tgt_tbl)

print("✅ Trusted load complete.")

spark.stop()
