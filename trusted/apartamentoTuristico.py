from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import utilsJoaquim_airflow as utils
from functools import reduce

# 1) Re-use the same Iceberg-aware session for read & write
spark = utils.create_context()
spark.sparkContext.setLogLevel("ERROR")

# Read landing-zone Iceberg table
db_name = "landing"
table_name = "apartamentosTuristicos_ocupacion"
print(f"→ Reading spark_catalog.{db_name}.{table_name}")

df = utils.read_iceberg_table(spark, db_name, table_name)

# 2) Clean & normalize (drop nulls)
df_clean = df.dropna()

# Normalize province names (fix Valencia)
df_clean = df_clean.withColumn(
    "PROVINCIA",
    when(col("PROVINCIA") == "Valencia/València", "Valencia")
    .otherwise(col("PROVINCIA"))
)

# Normalize municipality names
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

# 3) Provinces of interest with their municipalities
province_municipio_map = {
    "Barcelona": "Barcelona",
    "Valencia": "Valencia",
    "Sevilla": "Sevilla",
    "Illes Balears": "Palma de Mallorca"
}

print("\n=== Province & Municipality Data Quality Report (apartamentosTuristicos_ocupacion) ===")

for prov, muni in province_municipio_map.items():
    # Rows before cleaning
    df_pair = df.filter((col("PROVINCIA") == prov) & (col("MUNICIPIO") == muni))
    count_total = df_pair.count()

    # Rows after cleaning
    df_pair_clean = df_clean.filter((col("PROVINCIA") == prov) & (col("MUNICIPIO") == muni))
    count_clean = df_pair_clean.count()

    count_nulls = count_total - count_clean

    if count_clean > 0:
        # Distinct months present
        df_months = df_pair_clean.select("AÑO", "MES").distinct()

        first_row = df_months.orderBy("AÑO", "MES").first()
        last_row = df_months.orderBy(col("AÑO").desc(), col("MES").desc()).first()

        first_year, first_month = first_row["AÑO"], int(first_row["MES"])
        last_year, last_month = last_row["AÑO"], int(last_row["MES"])

        total_months_expected = (last_year - first_year) * 12 + (last_month - first_month + 1)
        total_months_present = df_months.count()

        missing_months = total_months_expected - total_months_present

        print(f"✅ {prov}, {muni}")
        print(f"   - Total rows before cleaning: {count_total}")
        print(f"   - Rows dropped (nulls): {count_nulls}")
        print(f"   - Rows after cleaning: {count_clean}")
        print(f"   - Distinct months present: {total_months_present}")
        print(f"   - Expected months: {total_months_expected}")
        print(f"   - Missing months: {missing_months}")
        print(f"   - First instance → AÑO={first_year}, MES={first_month:02d}")
        print(f"   - Last instance  → AÑO={last_year}, MES={last_month:02d}\n")
    else:
        print(f"⚠️ {prov}, {muni}: No data after cleaning\n")

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
