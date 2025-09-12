from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, lit
import utilsJoaquim as utils

# 1) Re-use the same Iceberg-aware session for read & write
spark = utils.create_context()
spark.sparkContext.setLogLevel("ERROR")

# Read landing-zone Iceberg table
db_name = "landing"
table_name = "actividades_Ocio"
print(f"→ Reading spark_catalog.{db_name}.{table_name}")

df = utils.read_iceberg_table(spark, db_name, table_name)

# 2) Normalize province names (fix Valencia)
df_norm = df.withColumn(
    "PROVINCIA",
    when(col("PROVINCIA") == "Valencia/València", "Valencia")
    .otherwise(col("PROVINCIA"))
)

# Cleaned dataset (drop nulls)
df_clean = df_norm.dropna()

# Provinces of interest
provinces = ["Madrid", "Barcelona", "Sevilla", "Illes Balears", "Valencia"]

print("\n=== Province Data Quality Report ===")
for prov in provinces:
    df_prov = df_norm.filter(col("PROVINCIA") == prov)
    count_total = df_prov.count()

    df_clean_prov = df_clean.filter(col("PROVINCIA") == prov)
    count_clean = df_clean_prov.count()

    count_nulls = count_total - count_clean

    if count_clean > 0:
        # First and last instance
        first_row = df_clean_prov.select("AÑO", "MES").orderBy("AÑO", "MES").first()
        last_row = df_clean_prov.select("AÑO", "MES").orderBy(col("AÑO").desc(), col("MES").desc()).first()

        # Build expected month count
        first_year, first_month = first_row["AÑO"], int(first_row["MES"])
        last_year, last_month = last_row["AÑO"], int(last_row["MES"])

        total_months_expected = (last_year - first_year) * 12 + (last_month - first_month + 1)
        months_missing = total_months_expected - count_clean

        print(f"✅ {prov}")
        print(f"   - Total rows before cleaning: {count_total}")
        print(f"   - Rows dropped (nulls): {count_nulls}")
        print(f"   - Rows after cleaning: {count_clean}")
        print(f"   - First instance → AÑO={first_year}, MES={first_month:02d}")
        print(f"   - Last instance  → AÑO={last_year}, MES={last_month:02d}")
        print(f"   - Missing months in series: {months_missing}\n")
    else:
        print(f"⚠️ {prov}: No data found\n")

# 3) Write final filtered cleaned data
df_filtered = df_clean.filter(col("PROVINCIA").isin(provinces))
df_filtered.show()
tgt_db, tgt_tbl = "trusted", "actividades_Ocio_selected"
print(f"→ Writing spark_catalog.{tgt_db}.{tgt_tbl}")
utils.overwrite_iceberg_table(spark, df_filtered, tgt_db, tgt_tbl)

print("✅ Trusted load complete.")

spark.stop()
