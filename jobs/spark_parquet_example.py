from pyspark.sql import SparkSession

def main():
    # Create a simple Spark session
    spark = SparkSession.builder \
        .appName("sparkParquet") \
        .getOrCreate()
    
    # Read data from S3 bucket with header option
    print("Reading from S3...")
    data = spark.read.option("header", "true").csv("s3a://bucket1/sample.csv")
    
    # Print schema to verify data is loaded correctly
    print("Schema of loaded data:")
    data.printSchema()
    
    # Write data to Parquet format
    print("Writing to Parquet...")
    data.write.mode("overwrite").parquet("s3a://bucket1/sample_parquet2")
    
    print("Data successfully written to Parquet: s3a://bucket1/sample_parquet2")

if __name__ == "__main__":
    main()