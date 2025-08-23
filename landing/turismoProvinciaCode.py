import utilsJoaquim as utils

spark = utils.create_context()
spark.sparkContext.setLogLevel("ERROR")

path  = "https://dataestur.azure-api.net/API-SEGITTUR-v1/TURISMO_INTERNO_PROV_CCAA_DL"
query = "CCAA%20origen=Todos&Provincia%20origen=Todos&CCAA%20destino=Todos&Provincia%20destino=Todos"
db_name = "landing"
table_name = "turismo_Provincia"
df = utils.get_api_endpoint_data(spark, path, filter)
utils.overwrite_iceberg_table(spark, df, db_name, table_name)

df = utils.read_iceberg_table(spark,db_name,table_name)
df.show()