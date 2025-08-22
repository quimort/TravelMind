from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, col, when
import utils
from functools import reduce

# 1) Re-use the same Iceberg-aware session for read & write
spark = utils.create_context()
spark.sparkContext.setLogLevel("ERROR")

# Read landing-zone Iceberg table
db_name = "landing"
table_name = "apartamentosTuristicos_ocupacion"
print(f"→ Reading spark_catalog.{db_name}.{table_name}")

df = utils.read_iceberg_table(spark, db_name, table_name)

# 2) Clean & normalize
df_clean = df.dropna()

# Normalize province names (fix Valencia)
df_clean = df_clean.withColumn(
    "PROVINCIA",
    when(col("PROVINCIA") == "Valencia/València", "Valencia")
    .otherwise(col("PROVINCIA"))
)

df_clean = df_clean.withColumn(
    "MUNICIPIO",
    when(col("MUNICIPIO") == "València", "Valencia")
    .otherwise(col("MUNICIPIO"))
)

df_clean = df_clean.withColumn(
    "MUNICIPIO",
    when(col("MUNICIPIO") == "Palma", "Palma de Mallorca")
    .otherwise(col("MUNICIPIO"))
)


# 3) Provinces of interest
# Define mapping of province → municipio
province_municipio_map = {
    "Barcelona": "Barcelona",
    "Valencia": "Valencia",
    "Sevilla": "Sevilla",
    "Illes Balears": "Palma de Mallorca"
}

# Check province & municipio counts
for prov, muni in province_municipio_map.items():
    count = df_clean.filter(
        (col("PROVINCIA") == prov) & (col("MUNICIPIO") == muni)
    ).count()
    
    if count > 0:
        print(f"✅ Found {count} rows for PROVINCIA={prov}, MUNICIPIO={muni}")
    else:
        print(f"⚠️ No data found for PROVINCIA={prov}, MUNICIPIO={muni}")

# 4) Apply final filter with (PROVINCIA, MUNICIPIO) pairs
conditions = [
    (col("PROVINCIA") == prov) & (col("MUNICIPIO") == muni)
    for prov, muni in province_municipio_map.items()
]

df_filtered = df_clean.filter(reduce(lambda a, b: a | b, conditions))

df_filtered.show()

# 5) Write into trusted zone
tgt_db, tgt_tbl = "trusted", "apartamentosTuristicos_ocupacion_selected"
print(f"→ Writing spark_catalog.{tgt_db}.{tgt_tbl}")
utils.overwrite_iceberg_table(spark, df_filtered, tgt_db, tgt_tbl)

print("✅ Trusted load complete.")

spark.stop()