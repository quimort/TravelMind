from pyspark.sql import SparkSession

def main():
    # The SparkSession is configured via command line parameters
    # We don't need to set Iceberg configs here
    spark = SparkSession.builder \
        .appName("sparkIceberg") \
        .getOrCreate()
    
    # Print Spark configuration for debugging
    print("Spark Configuration:")
    print(f"Warehouse dir: {spark.conf.get('spark.sql.warehouse.dir')}")
    
    # Read data from S3 bucket with header option
    print("Reading from S3...")
    data = spark.read.option("header", "true").csv("s3a://bucket1/sample.csv")
    
    # Print schema to verify data is loaded correctly
    print("Schema of loaded data:")
    data.printSchema()
    
    # Create a database if it doesn't exist
    print("Creating database...")
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg_db")
    
    # Write data to Iceberg table
    print("Writing to Iceberg table...")
    data.writeTo("iceberg_db.sample_table") \
        .using("iceberg") \
        .createOrReplace()
    
    # Verify the table was created
    print("Verifying table creation:")
    tables = spark.sql("SHOW TABLES IN iceberg_db").collect()
    for table in tables:
        print(f"Found table: {table['tableName']}")
    
    print("Data successfully written to Iceberg table: iceberg_db.sample_table")

if __name__ == "__main__":
    main()