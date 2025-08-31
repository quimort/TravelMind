import logging
import os
from typing import Optional

import utilsJoaquim as utils

logger = logging.getLogger(__name__)


def process_actividades(
	path: str,
	spark=None,
	db_name: str = "landing",
	table_name: str = "actividades_Ocio",
	show_rows: int = 0,
) -> dict:
	"""Descargar datos de ACTIVIDADES_OCIO_DL, escribir la tabla Iceberg y devolver conteo.

	Args:
		path: URL o ruta al fichero Excel/endpoint.
		spark: SparkSession opcional. Si no se pasa, se crea y se cierra al terminar.
		db_name: nombre del catálogo/db Iceberg.
		table_name: nombre de la tabla Iceberg.
		show_rows: si >0, hace df.show(show_rows) sobre la tabla resultante.

	Returns:
		dict con meta: {'rows': int, 'table': 'db.table'}
	"""
	created_spark = False
	if spark is None:
		spark = utils.create_context()
		created_spark = True
		spark.sparkContext.setLogLevel("ERROR")

	logger.info("Leyendo datos desde %s", path)
	df = utils.get_api_endpoint_excel_data(spark, path)

	logger.info("Escribiendo tabla Iceberg %s.%s", db_name, table_name)
	utils.overwrite_iceberg_table(spark, df, db_name, table_name)

	df_read = utils.read_iceberg_table(spark, db_name, table_name)
	if show_rows:
		df_read.show(show_rows)

	try:
		count = df_read.count()
	except Exception:
		# Si count falla por ser un DataFrame grande, devolvemos -1
		logger.exception("No se pudo contar filas de la tabla %s.%s", db_name, table_name)
		count = -1

	if created_spark:
		try:
			spark.stop()
		except Exception:
			logger.exception("Error al detener SparkSession")

	return {"rows": count, "table": f"{db_name}.{table_name}"}


if __name__ == "__main__":
	# Runner para uso local / debugging
	logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
	default_path = (
		"https://dataestur.azure-api.net/API-SEGITTUR-v1/ACTIVIDADES_OCIO_DL?CCAA=Todos&Provincia=Todos"
	)
	path = os.getenv("ACTIVIDADES_PATH", default_path)
	result = process_actividades(path, show_rows=10)
	print(result)