import utils as utils

# Create Spark session
spark = utils.create_context()
spark.sparkContext.setLogLevel("ERROR")

# Base URL for ports
base_url = "https://dataestur.azure-api.net/API-SEGITTUR-v1/PUERTOS_DL?Autoridad%20portuaria={}&CCAA=Todos"

# List of Autoridades Portuarias
autoridades = ["BARCELONA", "BALEARES", "SEVILLA", "VALENCIA"]

# Database and table
db_name = "landing"
table_name = "puertos_turismo"

dfs = []

# Loop over each Autoridad portuaria
for autoridad in autoridades:
    url = base_url.format(autoridad)
    print(f"📥 Fetching data for Autoridad Portuaria: {autoridad}")
    
    df_tmp = utils.get_api_endpoint_excel_data(spark, url)
    
    if df_tmp is not None and df_tmp.count() > 0:
        dfs.append(df_tmp)
    else:
        print(f"⚠️ No data for {autoridad}")

# Union all DataFrames
if dfs:
    df_final = dfs[0]
    for df in dfs[1:]:
        df_final = df_final.unionByName(df, allowMissingColumns=True)
else:
    raise ValueError("❌ No data retrieved from any port")

# Save into Iceberg
utils.overwrite_iceberg_table(spark, df_final, db_name, table_name)

# Read back and show
df = utils.read_iceberg_table(spark, db_name, table_name)
df.show()
