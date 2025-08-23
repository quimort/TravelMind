import utilsJoaquim as utils
from pyspark.sql import functions as F

# Create Spark context
spark = utils.create_context()
spark.sparkContext.setLogLevel("ERROR")

# Base URL
base_url = "https://dataestur.azure-api.net/API-SEGITTUR-v1/AENA_DESTINOS_DL?Aeropuerto%20AENA="

# Airports and their mapped cities
airports = {
    "JT%20Barcelona-El%20Prat": "Barcelona",
    "Girona-Costa%20Brava": "Barcelona",
    "AS%20Madrid-Barajas": "Madrid",
    "Sevilla": "Sevilla",
    "Jerez%20de%20la%20Frontera": "Sevilla",
    "P.%20Mallorca": "Palma de Mallorca",
    "Valencia": "Valencia"
}

# Collect all airport DataFrames
dfs = []

for airport, ciudad in airports.items():
    print(f"Fetching data for {airport} ({ciudad}) ...")
    
    # Build endpoint URL
    url = base_url + airport
    
    # Get data
    df_tmp = utils.get_api_endpoint_excel_data(spark, url)
    
    if df_tmp is not None and df_tmp.count() > 0:
        # Add CIUDAD column
        df_tmp = df_tmp.withColumn("CIUDAD", F.lit(ciudad))
        dfs.append(df_tmp)
    else:
        print(f"⚠️ No data returned for {airport}")

# Union all DataFrames
if dfs:
    df_final = dfs[0]
    for df in dfs[1:]:
        df_final = df_final.unionByName(df, allowMissingColumns=True)
else:
    raise ValueError("❌ No data retrieved from any airport")

# Define DB and table name
db_name = "landing"
table_name = "aena_destinos"

# Store in Iceberg table
utils.overwrite_iceberg_table(spark, df_final, db_name, table_name)

# Read back and show results
df = utils.read_iceberg_table(spark, db_name, table_name)
df.show()