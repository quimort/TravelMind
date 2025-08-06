import os
os.chdir("c:/Users/Joaquim Balletbo/OneDrive/Documents/AAmaster_UPC/TFM/TravelMind/landing/")
print(os.getcwd())

import pyspark
from pyspark.sql import SparkSession,DataFrame
import requests
import json 
from io import BytesIO
import pandas as pd
import os
import sys
import utils as utils

spark = utils.create_context()

path  = "https://dataestur.azure-api.net/API-SEGITTUR-v1/TURISMO_INTERNO_PROV_CCAA_DL"
query = "CCAA%20origen=Todos&Provincia%20origen=Todos&CCAA%20destino=Todos&Provincia%20destino=Todos"
db_name = "landing"
table_name = "turismo_Provincia"
df = utils.get_api_endpoint(spark, path, filter)
utils.overwrite_iceberg_table(spark, df, db_name, table_name)