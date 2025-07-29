from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import sys

def test_spark_cluster():
    print("🚀 Starting Spark cluster test...")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("SparkClusterTest") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "1") \
        .getOrCreate()
    
    print(f"✅ Spark session created successfully!")
    print(f"📊 Spark version: {spark.version}")
    print(f"🎯 Master URL: {spark.sparkContext.master}")
    print(f"📱 App Name: {spark.sparkContext.appName}")
    
    # Test 1: Create a simple DataFrame
    print("\n🧪 Test 1: Creating DataFrame...")
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Diana", 28)]
    columns = ["name", "age"]
    df = spark.createDataFrame(data, columns)
    
    print("✅ DataFrame created:")
    df.show()
    
    # Test 2: Perform transformations
    print("\n🧪 Test 2: Performing transformations...")
    adult_df = df.filter(col("age") >= 30)
    print("✅ Filtered adults (age >= 30):")
    adult_df.show()
    
    # Test 3: Aggregations
    print("\n🧪 Test 3: Performing aggregations...")
    avg_age = df.agg({"age": "avg"}).collect()[0][0]
    total_count = df.count()
    print(f"✅ Average age: {avg_age:.2f}")
    print(f"✅ Total records: {total_count}")
    
    # Test 4: Parallel operations
    print("\n🧪 Test 4: Testing parallel operations...")
    large_data = spark.range(1, 1000000).toDF("number")
    sum_result = large_data.agg({"number": "sum"}).collect()[0][0]
    print(f"✅ Sum of numbers 1-999999: {sum_result}")
    
    # Test 5: Write to output (optional)
    print("\n🧪 Test 5: Writing test output...")
    try:
        df.write.mode("overwrite").parquet("/tmp/spark_test_output")
        print("✅ Successfully wrote test data to /tmp/spark_test_output")
    except Exception as e:
        print(f"⚠️ Write test failed: {e}")
    
    # Cleanup
    spark.stop()
    print("\n🎉 All tests completed successfully!")
    print("🔥 Your Spark cluster is working perfectly!")

if __name__ == "__main__":
    try:
        test_spark_cluster()
    except Exception as e:
        print(f"❌ Test failed: {e}")
        sys.exit(1)