from pyspark.sql import SparkSession
import utilsJoaquim_airflow as utils

def process_trafico_semana():
    """
    ETL job to extract weekly traffic data from the API,
    store it in the landing Iceberg zone, and verify the load.
    """

    # 1️⃣ Initialize Spark context
    spark = utils.create_context()
    spark.sparkContext.setLogLevel("ERROR")

    # 2️⃣ Define source and target
    path = "https://dataestur.azure-api.net/API-SEGITTUR-v1/INTENSIDAD_TRAFICO_SEMANA_DL?CCAA=Todos&Provincia=Todos"
    db_name = "landing"
    table_name = "trafico_semana"

    print(f"→ Extracting data from API endpoint: {path}")
    df = utils.get_api_endpoint_data(spark, path)

    print(f"→ Writing to Iceberg table: spark_catalog.{db_name}.{table_name}")
    utils.overwrite_iceberg_table(spark, df, db_name, table_name)

    print(f"→ Reading data back from Iceberg to validate load...")
    df_read = utils.read_iceberg_table(spark, db_name, table_name)
    df_read.show(5, truncate=False)

    print("✅ ETL process for 'trafico_semana' completed successfully.")

    spark.stop()


if __name__ == "__main__":
    process_trafico_semana()
