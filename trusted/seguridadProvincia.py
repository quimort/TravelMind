from pyspark.sql.functions import col, when, lit
import utilsJoaquim as utils

# 1) Re-use the same Iceberg-aware session for read & write
spark = utils.create_context()
spark.sparkContext.setLogLevel("ERROR")

# Read landing-zone Iceberg table
db_name = "landing"
table_name = "seguridad_provincia"
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

# 3) Provinces of interest
provinces = ["Madrid", "Barcelona", "Sevilla", "Illes Balears", "Valencia"]

# === Expand quarterly data → monthly ===
# Create mappings: 3→[1,2], 6→[4,5], 9→[7,8], 12→[10,11]
def expand_month(df_quarter, quarter, fill_months):
    return [df_quarter.filter(col("MES") == quarter).withColumn("MES", lit(m)) for m in fill_months]

# Collect expansions
expansions = []
expansions += expand_month(df_clean, 3, [1, 2])
expansions += expand_month(df_clean, 6, [4, 5])
expansions += expand_month(df_clean, 9, [7, 8])
expansions += expand_month(df_clean, 12, [10, 11])

# Union everything
df_filled = df_clean
for e in expansions:
    df_filled = df_filled.unionByName(e)

# 4) Province checks (use df_filled consistently!)
print("\n=== Province Data Quality Report ===")

for prov in provinces:
    df_prov_original = df.filter(col("PROVINCIA") == prov)
    count_total = df_prov_original.count()

    df_prov_clean = df_filled.filter(col("PROVINCIA") == prov)
    count_clean = df_prov_clean.count()

    if count_clean > 0:
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
        print(f"   - Rows after monthly expansion: {count_clean}")
        print(f"   - Distinct months present: {total_months_present}")
        print(f"   - Expected months: {total_months_expected}")
        print(f"   - Missing months: {missing_months}")
        print(f"   - First instance → AÑO={first_year}, MES={first_month:02d}")
        print(f"   - Last instance  → AÑO={last_year}, MES={last_month:02d}\n")
    else:
        print(f"⚠️ {prov}: No data after cleaning\n")

# 5) Save final table
df_filtered = df_filled.filter(col("PROVINCIA").isin(provinces)).orderBy("AÑO", "MES")

df_filtered.show()

tgt_db, tgt_tbl = "trusted", "seguridad_provincia_selected"
print(f"→ Writing spark_catalog.{tgt_db}.{tgt_tbl}")
utils.overwrite_iceberg_table(spark, df_filtered, tgt_db, tgt_tbl)

print("✅ Trusted load complete (monthly data expanded).")

spark.stop()
