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

print("\n=== Municipality Data Quality Report (after dropping null-columns) ===")
for mun in municipios:
    df_mun = df_main.filter(col("MUNICIPIO_ORIGEN") == mun)
    count_total = df_mun.count()

    df_clean_prov = df_union.filter(col("MUNICIPIO_ORIGEN") == mun)
    count_clean = df_clean_prov.count()

    if count_clean > 0:
        # Distinct months in the cleaned data
        df_months = df_clean_prov.select("AÑO", "MES").distinct()

        first_row = df_months.orderBy("AÑO", "MES").first()
        last_row = df_months.orderBy(col("AÑO").desc(), col("MES").desc()).first()

        first_year, first_month = first_row["AÑO"], int(first_row["MES"])
        last_year, last_month = last_row["AÑO"], int(last_row["MES"])

        total_months_expected = (last_year - first_year) * 12 + (last_month - first_month + 1)
        total_months_present = df_months.count()

        missing_months = total_months_expected - total_months_present

        print(f"✅ {mun}")
        print(f"   - Total rows before cleaning: {count_total}")
        print(f"   - Rows dropped (VIAJEROS=0): {count_total - count_clean}")
        print(f"   - Rows after cleaning: {count_clean}")
        print(f"   - Distinct months present: {total_months_present}")
        print(f"   - Expected months: {total_months_expected}")
        print(f"   - Missing months: {missing_months}")
        print(f"   - First instance → AÑO={first_year}, MES={first_month:02d}")
        print(f"   - Last instance  → AÑO={last_year}, MES={last_month:02d}\n")
    else:
        print(f"⚠️ {mun}: No data after cleaning\n")

# 5) Create final filtered DataFrame
df_filtered = df_union.filter(col("MUNICIPIO_ORIGEN").isin(municipios))

df_filtered.show()

# 6) Write into trusted zone
tgt_db, tgt_tbl = "trusted", "turismo_Emisor_selected"
print(f"→ Writing spark_catalog.{tgt_db}.{tgt_tbl}")
utils.overwrite_iceberg_table(spark, df_filtered, tgt_db, tgt_tbl)

print("✅ Trusted load complete.")

spark.stop()
