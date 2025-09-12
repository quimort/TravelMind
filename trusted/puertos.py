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

print("\n=== Province Data Quality Report ===")

# 3) Provinces of interest
provinces = ["Barcelona", "Sevilla", "Illes Balears", "Valencia"]

for prov in provinces:
    df_prov = df.filter(col("AUT_PORTUARIA") == prov)
    count_total = df_prov.count()

    df_prov_clean = df_clean.filter(col("AUT_PORTUARIA") == prov)
    count_clean = df_prov_clean.count()

    count_dropped = count_total - count_clean

    if count_clean > 0:
        # Distinct months in the cleaned data
        df_months = df_prov_clean.select("AÑO", "MES").distinct()

        first_row = df_months.orderBy("AÑO", "MES").first()
        last_row = df_months.orderBy(col("AÑO").desc(), col("MES").desc()).first()

        first_year, first_month = first_row["AÑO"], int(first_row["MES"])
        last_year, last_month = last_row["AÑO"], int(last_row["MES"])

        total_months_expected = (last_year - first_year) * 12 + (last_month - first_month + 1)
        total_months_present = df_months.count()

        missing_months = total_months_expected - total_months_present

        print(f"✅ {prov}")
        print(f"   - Total rows before cleaning: {count_total}")
        print(f"   - Rows dropped (LUGAR_RESIDENCIA='Total'): {count_dropped}")
        print(f"   - Rows after cleaning: {count_clean}")
        print(f"   - Distinct months present: {total_months_present}")
        print(f"   - Expected months: {total_months_expected}")
        print(f"   - Missing months: {missing_months}")
        print(f"   - First instance → AÑO={first_year}, MES={first_month:02d}")
        print(f"   - Last instance  → AÑO={last_year}, MES={last_month:02d}\n")
    else:
        print(f"⚠️ {prov}: No data after cleaning\n")

# 4) Write into trusted zone (single table with only these provinces)
tgt_db, tgt_tbl = "trusted", "puertos_turismo_selected"
print(f"→ Writing spark_catalog.{tgt_db}.{tgt_tbl}")
utils.overwrite_iceberg_table(spark, df_clean, tgt_db, tgt_tbl)

df_clean.show()

print("✅ Trusted load complete.")

spark.stop()
