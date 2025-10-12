from pyspark.sql.functions import col, when
import utilsJoaquim_airflow as utils

def process_calidad_aire_selected(
	spark=None,
	src_db: str = "landing",
	src_table: str = "calidad_aire",
	tgt_db: str = "trusted",
	tgt_table: str = "calidad_aire_selected",
	show_rows: int = 10):
	"""Process calidad del aire data from landing to trusted zone.

	This function:
	1. Reads the Iceberg table from the landing zone.
	2. Cleans and normalizes data (fixes province names, removes nulls).
	3. Filters selected provinces.
	4. Writes the cleaned subset into the trusted zone.
	5. Prints a data quality report by province.

	Parameters
	- spark: optional SparkSession. If not provided, a local session will be created.
	- src_db/src_table: source Iceberg database and table.
	- tgt_db/tgt_table: target Iceberg database and table.
	- show_rows: if >0, show sample rows for quick debugging.
	"""
	created_spark = False
	if spark is None:
		spark = utils.create_context()
		created_spark = True
		spark.sparkContext.setLogLevel("ERROR")

	try:
		print(f"→ Reading spark_catalog.{src_db}.{src_table}")
		df = utils.read_iceberg_table(spark, src_db, src_table)

		# --- Cleaning and normalization ---
		df_clean = df.dropna()
		df_clean = df_clean.withColumn(
			"PROVINCIA",
			when(col("PROVINCIA") == "Valencia/València", "Valencia").otherwise(col("PROVINCIA"))
		)

		# --- Provinces of interest ---
		provinces = ["Madrid", "Barcelona", "Sevilla", "Illes Balears", "Valencia"]

		print("\n=== Province Data Quality Report (calidad_aire) ===")
		for prov in provinces:
			df_prov = df.filter(col("PROVINCIA") == prov)
			count_total = df_prov.count()

			df_prov_clean = df_clean.filter(col("PROVINCIA") == prov)
			count_clean = df_prov_clean.count()

			count_nulls = count_total - count_clean

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
				print(f"   - Rows dropped (nulls): {count_nulls}")
				print(f"   - Rows after cleaning: {count_clean}")
				print(f"   - Distinct months present: {total_months_present}")
				print(f"   - Expected months: {total_months_expected}")
				print(f"   - Missing months: {missing_months}")
				print(f"   - First instance → AÑO={first_year}, MES={first_month:02d}")
				print(f"   - Last instance  → AÑO={last_year}, MES={last_month:02d}\n")
			else:
				print(f"⚠️ {prov}: No data after cleaning\n")

		# --- Filter final dataset and write to trusted zone ---
		df_filtered = df_clean.filter(col("PROVINCIA").isin(provinces))
		if show_rows > 0:
			df_filtered.show(show_rows)

		print(f"→ Writing spark_catalog.{tgt_db}.{tgt_table}")
		utils.overwrite_iceberg_table(spark, df_filtered, tgt_db, tgt_table)

		print("✅ Trusted load complete.")

	finally:
		# Stop Spark if it was created inside this function
		if created_spark:
			try:
				spark.stop()
			except Exception:
				pass


if __name__ == "__main__":
	# Local quick-run for development / debugging
	process_calidad_aire_selected(show_rows=10)