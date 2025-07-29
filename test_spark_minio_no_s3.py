from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from minio import Minio
from minio.error import S3Error
import os
import io

def test_spark_minio_no_s3():
    print("🚀 Testing Spark with MinIO bucket1/test (PySpark only)...")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("SparkMinIOPySparkOnlyTest") \
        .getOrCreate()
    
    # MinIO client configuration - use Docker network hostname
    minio_client = Minio(
        "minio:9000",  # Use Docker service name instead of localhost
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False  # HTTP, not HTTPS
    )
    
    try:
        print("✅ Spark session and MinIO client created")
        
        # Ensure bucket1 exists
        bucket_name = "bucket1"
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"📁 Created bucket: {bucket_name}")
        else:
            print(f"📁 Bucket {bucket_name} already exists")
        
        # Create sample travel data
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
            ("Paris", "France", 18000000, 20.0, "Spring"),
            ("Rome", "Italy", 14000000, 28.0, "Summer"),
            ("Amsterdam", "Netherlands", 8000000, 18.5, "Spring"),
            ("Vienna", "Austria", 7500000, 16.0, "Spring"),
            ("Prague", "Czech Republic", 9000000, 19.0, "Spring"),
            ("Lisbon", "Portugal", 6000000, 24.0, "Summer")
        ]
        
        df = spark.createDataFrame(sample_data, schema)
        print(f"📊 Created DataFrame with {df.count()} rows")
        
        # Show sample data
        print("\n🔍 Sample travel data:")
        df.show()
        
        # Write to local Parquet
        local_parquet_path = "/tmp/travel_data.parquet"
        df.write.mode("overwrite").parquet(local_parquet_path)
        print(f"💾 Wrote Parquet to local path: {local_parquet_path}")
        
        # Write to local CSV using PySpark
        local_csv_path = "/tmp/travel_data.csv"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(local_csv_path)
        print(f"💾 Wrote CSV to local path: {local_csv_path}")
        
        # Upload Parquet files to MinIO bucket1/test/
        parquet_files = [f for f in os.listdir(local_parquet_path) if f.endswith('.parquet')]
        
        for i, parquet_file in enumerate(parquet_files):
            file_path = os.path.join(local_parquet_path, parquet_file)
            minio_object_name = f"test/travel_data_part_{i}.parquet"
            
            minio_client.fput_object(
                bucket_name,
                minio_object_name,
                file_path
            )
            print(f"📤 Uploaded {parquet_file} to bucket1/{minio_object_name}")
        
        # Upload CSV file to MinIO
        csv_files = [f for f in os.listdir(local_csv_path) if f.endswith('.csv')]
        if csv_files:
            csv_file_path = os.path.join(local_csv_path, csv_files[0])
            minio_client.fput_object(
                bucket_name,
                "test/travel_data.csv",
                csv_file_path
            )
            print("📤 Uploaded CSV to bucket1/test/travel_data.csv")
        
        # Perform analytics using PySpark
        print("\n📈 Performing analytics with PySpark...")
        
        # Country summary
        country_summary = df.groupBy("country") \
            .agg({"visitors": "sum", "avg_temperature": "avg"}) \
            .withColumnRenamed("sum(visitors)", "total_visitors") \
            .withColumnRenamed("avg(avg_temperature)", "avg_temperature")
        
        print("🌍 Country summary:")
        country_summary.show()
        
        # Season summary
        season_summary = df.groupBy("season").agg({"visitors": "sum", "destination": "count"}) \
            .withColumnRenamed("sum(visitors)", "total_visitors") \
            .withColumnRenamed("count(destination)", "destination_count")
        
        print("🌸 Season summary:")
        season_summary.show()
        
        # Write analytics to local CSV and upload
        analytics_csv_path = "/tmp/country_analytics.csv"
        country_summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(analytics_csv_path)
        
        # Upload analytics CSV
        analytics_csv_files = [f for f in os.listdir(analytics_csv_path) if f.endswith('.csv')]
        if analytics_csv_files:
            analytics_file_path = os.path.join(analytics_csv_path, analytics_csv_files[0])
            minio_client.fput_object(
                bucket_name,
                "test/country_analytics.csv",
                analytics_file_path
            )
            print("📤 Uploaded country analytics to bucket1/test/country_analytics.csv")
        
        # Write season analytics
        season_csv_path = "/tmp/season_analytics.csv"
        season_summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(season_csv_path)
        
        season_csv_files = [f for f in os.listdir(season_csv_path) if f.endswith('.csv')]
        if season_csv_files:
            season_file_path = os.path.join(season_csv_path, season_csv_files[0])
            minio_client.fput_object(
                bucket_name,
                "test/season_analytics.csv",
                season_file_path
            )
            print("📤 Uploaded season analytics to bucket1/test/season_analytics.csv")
        
        # List objects in bucket1/test/ to verify
        print("\n📁 Files in bucket1/test/:")
        objects = minio_client.list_objects(bucket_name, prefix="test/")
        for obj in objects:
            print(f"   - {obj.object_name} ({obj.size} bytes)")
        
        # Calculate final statistics using PySpark
        total_visitors = df.agg({"visitors": "sum"}).collect()[0][0]
        avg_temp = df.agg({"avg_temperature": "avg"}).collect()[0][0]
        max_visitors = df.agg({"visitors": "max"}).collect()[0][0]
        min_visitors = df.agg({"visitors": "min"}).collect()[0][0]
        
        print(f"\n📊 Final Statistics (calculated with PySpark):")
        print(f"👥 Total visitors: {total_visitors:,}")
        print(f"🌡️ Average temperature: {avg_temp:.1f}°C")
        print(f"🏙️ Number of destinations: {df.count()}")
        print(f"🌍 Number of countries: {df.select('country').distinct().count()}")
        print(f"📈 Max visitors (single destination): {max_visitors:,}")
        print(f"📉 Min visitors (single destination): {min_visitors:,}")
        
        # Top destination
        top_destination = df.orderBy(df.visitors.desc()).select("destination", "visitors").first()
        print(f"🏆 Top destination: {top_destination['destination']} ({top_destination['visitors']:,} visitors)")
        
        print("\n🎉 MinIO bucket1/test integration completed successfully (PySpark only)!")
        print("📁 Files created in bucket1/test/:")
        print("   - travel_data_part_*.parquet (Parquet files)")
        print("   - travel_data.csv (Main dataset)")
        print("   - country_analytics.csv (Country summaries)")
        print("   - season_analytics.csv (Season summaries)")
        
    except S3Error as e:
        print(f"❌ MinIO Error: {e}")
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        spark.stop()
        print("🛑 Spark session stopped")

if __name__ == "__main__":
    test_spark_minio_no_s3()