from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, col
import os
import utils
import pyspark
from pyspark.sql import SparkSession,DataFrame
import requests
import json 
from io import BytesIO
import pandas as pd
import os
import sys
import utils as utils


# 1) Re-use the same Iceberg-aware session for read & write
spark = utils.create_context()
spark.sparkContext.setLogLevel("ERROR")

# Read landing-zone Iceberg table
db_name = "landing"
table_name = "turismo_Provincia"
print(f"→ Reading spark_catalog.{db_name}.{table_name}")

#
df = utils.read_iceberg_table(spark, db_name, table_name)
# 5) Clean & normalize
df_clean = df.dropna()
for c in ["CCAA_ORIGEN","PROVINCIA_ORIGEN","CCAA_DESTINO","PROVINCIA_DESTINO"]:
    df_clean = df_clean.withColumn(c, upper(col(c)))
df_clean.show()
# Write into trusted zone
tgt_db, tgt_tbl = "trusted", "turismo_Provincia"
print(f"→ Writing spark_catalog.{tgt_db}.{tgt_tbl}")
utils.overwrite_iceberg_table(spark, df_clean, tgt_db, tgt_tbl)
print("✅ Trusted load complete.")

spark.stop()