from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
import utilsJoaquim as utils

# 1) Spark session
spark = utils.create_context()
spark.sparkContext.setLogLevel("ERROR")

# 2) Read landing table
db_name, table_name = "landing", "apartamentos_ocupacion"
print(f"→ Reading spark_catalog.{db_name}.{table_name}")
df = utils.read_iceberg_table(spark, db_name, table_name)

# 3) Basic cleaning
df_clean = df.fillna(0)

# Normalize province names
df_clean = df_clean.withColumn(
    "PROVINCIA",
    when(col("PROVINCIA") == "Islas Baleares", "Illes Balears").otherwise(col("PROVINCIA"))
)

# Provinces of interest
provinces = ["Madrid", "Barcelona", "Sevilla", "Illes Balears", "Valencia"]
df_clean = df_clean.filter(col("PROVINCIA").isin(provinces))

# 4) Select rows from March 2020
march2020 = df_clean.filter((col("AÑO") == 2020) & (col("MES") == 3))

# Duplicate for April (MES=4)
april2020 = march2020.withColumn("MES", lit(4))

# Duplicate for May (MES=5)
may2020 = march2020.withColumn("MES", lit(5))

# Union everything
df_final = df_clean.unionByName(april2020).unionByName(may2020)

# 5) Write to trusted zone
tgt_db, tgt_tbl = "trusted", "apartamentos_ocupacion_selected"
print(f"→ Writing spark_catalog.{tgt_db}.{tgt_tbl}")
utils.overwrite_iceberg_table(spark, df_final, tgt_db, tgt_tbl)

print("\n=== Province Data Quality Report (after dropping null-columns) ===")
for prov in provinces:
    df_prov = df_final.filter(col("PROVINCIA") == prov)
    count_total = df_prov.count()

    df_clean_prov = df_clean.filter(col("PROVINCIA") == prov)
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

        print(f"✅ {prov}")
        print(f"   - Total rows before cleaning: {count_total}")
        print(f"   - Rows dropped (VIAJEROS=0): {count_total - count_clean}")
        print(f"   - Rows after cleaning: {count_clean}")
        print(f"   - Distinct months present: {total_months_present}")
        print(f"   - Expected months: {total_months_expected}")
        print(f"   - Missing months: {missing_months}")
        print(f"   - First instance → AÑO={first_year}, MES={first_month:02d}")
        print(f"   - Last instance  → AÑO={last_year}, MES={last_month:02d}\n")
    else:
        print(f"⚠️ {prov}: No data after cleaning\n")

print("✅ Trusted load complete. Rows for April & May 2020 were filled with March values.")

spark.stop()
