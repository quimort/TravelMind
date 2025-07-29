from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os

def test_spark_minio_integration():
    print("🚀 Starting Spark-MinIO S3 Integration Test...")
    
    # Create Spark session with S3 configuration
    spark = SparkSession.builder \
        .appName("SparkMinIOTest") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    try:
        print("✅ Spark session created successfully")
        
        # Create sample data
        print("📊 Creating sample travel data...")
        schema = StructType([
            StructField("destination", StringType(), True),
            StructField("country", StringType(), True),
            StructField("visitors", IntegerType(), True),
            StructField("avg_temperature", DoubleType(), True),
            StructField("season", StringType(), True)
        ])
        
        sample_data = [
            ("Barcelona", "Spain", 15000000, 22.5, "Summer"),
            ("Madrid", "Spain", 12000000, 25.0, "Summer"),
            ("Paris", "France", 18000000, 20.0, "Summer"),
            ("Rome", "Italy", 14000000, 28.0, "Summer"),
            ("Amsterdam", "Netherlands", 8000000, 18.5, "Summer"),
            ("Berlin", "Germany", 10000000, 19.0, "Summer"),
            ("Vienna", "Austria", 6000000, 21.0, "Summer"),
            ("Prague", "Czech Republic", 7000000, 20.5, "Summer")
        ]
        
        df = spark.createDataFrame(sample_data, schema)
        print(f"📈 Created DataFrame with {df.count()} rows")
        
        # Show sample data
        print("\n🔍 Sample data:")
        df.show()
        
        # Write to MinIO S3 as Parquet
        s3_path = "s3a://travelmind/test-data/destinations.parquet"
        print(f"💾 Writing data to MinIO S3: {s3_path}")
        
        df.write \
            .mode("overwrite") \
            .parquet(s3_path)
        
        print("✅ Successfully wrote Parquet file to MinIO S3")
        
        # Read back from MinIO S3
        print(f"📖 Reading data back from MinIO S3: {s3_path}")
        df_read = spark.read.parquet(s3_path)
        
        print(f"📊 Read {df_read.count()} rows from S3")
        print("\n🔍 Data read from S3:")
        df_read.show()
        
        # Perform some analytics
        print("\n📈 Performing analytics on S3 data...")
        
        # Top destinations by visitors
        print("🏆 Top 5 destinations by visitors:")
        df_read.orderBy(df_read.visitors.desc()).select("destination", "country", "visitors").show(5)
        
        # Average temperature by country
        print("🌡️ Average temperature by country:")
        df_read.groupBy("country").avg("avg_temperature").show()
        
        # Total visitors
        total_visitors = df_read.agg({"visitors": "sum"}).collect()[0][0]
        print(f"👥 Total visitors across all destinations: {total_visitors:,}")
        
        print("\n🎉 Spark-MinIO S3 integration test completed successfully!")
        print("🔥 Your data pipeline is ready for production!")
        
    except Exception as e:
        print(f"❌ Error during test: {str(e)}")
        raise e
    finally:
        spark.stop()
        print("🛑 Spark session stopped")

if __name__ == "__main__":
    test_spark_minio_integration()