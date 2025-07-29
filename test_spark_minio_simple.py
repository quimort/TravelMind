from pyspark.sql import SparkSession

def main():
    # No need for S3A config - it's in spark-defaults.conf!
    spark = SparkSession.builder.appName("sparkS3").getOrCreate()
    
    try:
        # Read data and print
        data = spark.read.option("header", "true").csv("s3a://bucket1/sample.csv")
        data.show()
        
        # Write to parquet
        data.write.mode("overwrite").parquet("s3a://bucket1/sample_parquet")
        
        print("✅ Successfully converted CSV to Parquet!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()