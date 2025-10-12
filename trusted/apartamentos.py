from pyspark.sql.functions import col, lit, when
import utilsJoaquim_airflow as utils


def process_apartamentos_trusted(
	spark=None,
	src_db: str = "landing",
	src_table: str = "apartamentos_ocupacion",
	tgt_db: str = "trusted",
	tgt_table: str = "apartamentos_ocupacion_selected",
	show_rows: int = 10):
	"""Process apartamentos ocupación data from landing to trusted zone.

	This function:
	1. Reads the Iceberg table from the landing zone.
	2. Cleans and normalizes the data.
	3. Filters selected provinces.
	4. Duplicates March 2020 data to fill April & May 2020.
	5. Writes the final dataset to the trusted zone.
	6. Prints a simple data quality report per province.

	Parameters
	- spark: optional SparkSession. If not provided, a local session will be created.
	- src_db/src_table: source Iceberg database and table.
	- tgt_db/tgt_table: target Iceberg database and table.
	- show_rows: if >0, shows sample rows from the trusted table.

	Returns the Spark DataFrame written to the trusted zone.
	"""
	created_spark = False
	if spark is None:
		spark = utils.create_context()
		created_spark = True
		spark.sparkContext.setLogLevel("ERROR")

	try:
		print(f"→ Reading spark_catalog.{src_db}.{src_table}")
		df = utils.read_iceberg_table(spark, src_db, src_table)

		# --- Basic cleaning ---
		df_clean = df.fillna(0)
		df_clean = df_clean.withColumn(
			"PROVINCIA",
			when(col("PROVINCIA") == "Islas Baleares", "Illes Balears").otherwise(col("PROVINCIA"))
		)

		# --- Filter provinces of interest ---
		provinces = ["Madrid", "Barcelona", "Sevilla", "Illes Balears", "Valencia"]
		df_clean = df_clean.filter(col("PROVINCIA").isin(provinces))

		# --- Duplicate March 2020 rows for April and May ---
		march2020 = df_clean.filter((col("AÑO") == 2020) & (col("MES") == 3))
		april2020 = march2020.withColumn("MES", lit(4))
		may2020 = march2020.withColumn("MES", lit(5))
		df_final = df_clean.unionByName(april2020).unionByName(may2020)

		# --- Write to trusted zone ---
		print(f"→ Writing spark_catalog.{tgt_db}.{tgt_table}")
		utils.overwrite_iceberg_table(spark, df_final, tgt_db, tgt_table)

		# --- Optional preview ---
		if show_rows > 0:
			df_preview = utils.read_iceberg_table(spark, tgt_db, tgt_table)
			df_preview.show(show_rows)

		# --- Province Data Quality Report ---
		print("\n=== Province Data Quality Report ===")
		for prov in provinces:
			df_prov = df_final.filter(col("PROVINCIA") == prov)
			count_total = df_prov.count()

			df_clean_prov = df_clean.filter(col("PROVINCIA") == prov)
			count_clean = df_clean_prov.count()

			if count_clean > 0:
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

	finally:
		if created_spark:
			try:
				spark.stop()
			except Exception:
				pass


if __name__ == "__main__":
	# Local quick-run for development / debugging
	process_apartamentos_trusted(show_rows=10)
