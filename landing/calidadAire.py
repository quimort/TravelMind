import utilsJoaquim_airflow as utils

def process_calidad_aire(
	path: str = "https://dataestur.azure-api.net/API-SEGITTUR-v1/CALIDAD_AIRE_DL?CCAA=Todos&Provincia=Todos",
	spark=None,
	db_name: str = "landing",
	table_name: str = "calidad_aire",
	show_rows: int = 10):
	"""Fetch calidad del aire data and write to Iceberg.

	Parameters
	- path: API endpoint to fetch the data from.
	- spark: optional SparkSession. If not provided, a local session will be created.
	- db_name / table_name: destination Iceberg database/table.
	- show_rows: if >0, call .show(show_rows) on the saved table for quick debugging.
	"""
	created_spark = False
	if spark is None:
		spark = utils.create_context()
		created_spark = True
		spark.sparkContext.setLogLevel("ERROR")

	try:
		df = utils.get_api_endpoint_excel_data(spark, path)
		utils.overwrite_iceberg_table(spark, df, db_name, table_name)
		if show_rows > 0:
			df2 = utils.read_iceberg_table(spark, db_name, table_name)
			df2.show(show_rows)

	finally:
		# If we created the Spark session here, stop it to free resources.
		if created_spark:
			try:
				spark.stop()
			except Exception:
				pass


if __name__ == "__main__":
	# Local quick-run for development / debugging
	process_calidad_aire(show_rows=10)