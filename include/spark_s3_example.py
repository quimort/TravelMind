from pyspark.sql import SparkSession

def main():
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("sparkS3") \
        .getOrCreate()
    
    # Read data from S3 bucket with header option
    data = spark.read.option("header", "true").csv("s3a://bucket1/sample.csv")
    
    # Write data to S3 in Parquet format with overwrite mode
    data.write.mode("overwrite").parquet("s3a://bucket1/sample_parquet")

if __name__ == "__main__":
    main()