import utilsJoaquim as utils

spark = utils.create_context()
spark.sparkContext.setLogLevel("ERROR")

path = "https://dataestur.azure-api.net/API-SEGITTUR-v1/INTENSIDAD_TRAFICO_SEMANA_DL?CCAA=Todos&Provincia=Todos"
db_name = "landing"
table_name = "trafico_semana"
df = utils.get_api_endpoint_data(spark, path)

utils.overwrite_iceberg_table(spark, df, db_name, table_name)
df = utils.read_iceberg_table(spark,db_name,table_name)
df.show()