#!/usr/bin/env python3
"""
TravelMind Stack Integration Tests
Tests all components and their interactions
"""

import requests
import time
import json
import subprocess
import sys
from datetime import datetime, timedelta

class TravelMindTester:
    def __init__(self):
        self.base_urls = {
            'airflow': 'http://localhost:8090',
            'spark': 'http://localhost:8080',
            'spark_history': 'http://localhost:18080',
            'minio': 'http://localhost:9000',
            'minio_console': 'http://localhost:9001'
        }
        self.auth = ('airflow', 'airflow')
        self.results = []
    
    def log_result(self, test_name, passed, message=""):
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{test_name}: {status} {message}")
        self.results.append({
            'test': test_name,
            'passed': passed,
            'message': message,
            'timestamp': datetime.now().isoformat()
        })
    
    def test_service_health(self):
        """Test basic service health endpoints"""
        print("\n=== Service Health Tests ===")
        
        # Test Airflow health
        try:
            response = requests.get(f"{self.base_urls['airflow']}/health", timeout=10)
            self.log_result("Airflow Health", response.status_code == 200)
        except Exception as e:
            self.log_result("Airflow Health", False, str(e))
        
        # Test Spark Master
        try:
            response = requests.get(self.base_urls['spark'], timeout=10)
            self.log_result("Spark Master", "Spark Master" in response.text)
        except Exception as e:
            self.log_result("Spark Master", False, str(e))
        
        # Test MinIO health
        try:
            response = requests.get(f"{self.base_urls['minio']}/minio/health/live", timeout=10)
            self.log_result("MinIO Health", response.status_code == 200)
        except Exception as e:
            self.log_result("MinIO Health", False, str(e))
    
    def test_airflow_api(self):
        """Test Airflow API functionality"""
        print("\n=== Airflow API Tests ===")
        
        # Test API health
        try:
            response = requests.get(
                f"{self.base_urls['airflow']}/api/v1/health",
                auth=self.auth,
                timeout=10
            )
            health_data = response.json()
            db_healthy = health_data.get('metadatabase', {}).get('status') == 'healthy'
            self.log_result("Airflow API Health", db_healthy)
        except Exception as e:
            self.log_result("Airflow API Health", False, str(e))
        
        # Test DAGs endpoint
        try:
            response = requests.get(
                f"{self.base_urls['airflow']}/api/v1/dags",
                auth=self.auth,
                timeout=10
            )
            self.log_result("Airflow DAGs API", response.status_code == 200)
        except Exception as e:
            self.log_result("Airflow DAGs API", False, str(e))
        
        # Test connections
        try:
            response = requests.get(
                f"{self.base_urls['airflow']}/api/v1/connections",
                auth=self.auth,
                timeout=10
            )
            self.log_result("Airflow Connections API", response.status_code == 200)
        except Exception as e:
            self.log_result("Airflow Connections API", False, str(e))
    
    def test_spark_functionality(self):
        """Test Spark job execution"""
        print("\n=== Spark Functionality Tests ===")
        
        try:
            # Submit a simple Spark job
            cmd = [
                'docker', 'exec', 'spark-master',
                '/opt/spark/bin/spark-submit',
                '--class', 'org.apache.spark.examples.SparkPi',
                '--master', 'spark://spark-master:7077',
                '/opt/spark/examples/jars/spark-examples_2.12-3.5.5.jar',
                '10'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            success = "Pi is roughly" in result.stdout
            self.log_result("Spark Job Execution", success)
            
        except subprocess.TimeoutExpired:
            self.log_result("Spark Job Execution", False, "Timeout")
        except Exception as e:
            self.log_result("Spark Job Execution", False, str(e))
    
    def test_minio_functionality(self):
        """Test MinIO object storage"""
        print("\n=== MinIO Functionality Tests ===")
        
        try:
            # Test bucket listing (requires mc client)
            cmd = [
                'docker', 'exec', 'mc',
                'mc', 'ls', 'myminio/bucket1'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            success = result.returncode == 0
            self.log_result("MinIO Bucket Access", success)
            
        except Exception as e:
            self.log_result("MinIO Bucket Access", False, str(e))
    
    def test_spark_minio_integration(self):
        """Test Spark-MinIO integration"""
        print("\n=== Spark-MinIO Integration Tests ===")
        
        # Create a simple Spark job that reads from MinIO
        spark_code = '''
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MinIO Test") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "testuser") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

try:
    df = spark.read.csv("s3a://bucket1/sample-data.csv", header=True)
    print(f"Successfully read {df.count()} rows from MinIO")
    df.show()
except Exception as e:
    print(f"Error reading from MinIO: {e}")
finally:
    spark.stop()
'''
        
        try:
            # Write the test script to a temporary file in the container
            with open('/tmp/minio_test.py', 'w') as f:
                f.write(spark_code)
            
            # Copy to container and run
            subprocess.run(['docker', 'cp', '/tmp/minio_test.py', 'spark-master:/tmp/'], check=True)
            
            cmd = [
                'docker', 'exec', 'spark-master',
                '/opt/spark/bin/spark-submit',
                '--master', 'spark://spark-master:7077',
                '/tmp/minio_test.py'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            success = "Successfully read" in result.stdout and result.returncode == 0
            self.log_result("Spark-MinIO Integration", success)
            
        except Exception as e:
            self.log_result("Spark-MinIO Integration", False, str(e))
    
    def test_airflow_spark_connection(self):
        """Test Airflow-Spark connection"""
        print("\n=== Airflow-Spark Connection Tests ===")
        
        try:
            # Check if Spark connection exists in Airflow
            response = requests.get(
                f"{self.base_urls['airflow']}/api/v1/connections/spark_standalone_client",
                auth=self.auth,
                timeout=10
            )
            
            if response.status_code == 200:
                conn_data = response.json()
                is_spark = conn_data.get('conn_type') == 'spark'
                self.log_result("Airflow Spark Connection", is_spark)
            else:
                self.log_result("Airflow Spark Connection", False, f"HTTP {response.status_code}")
                
        except Exception as e:
            self.log_result("Airflow Spark Connection", False, str(e))
    
    def generate_report(self):
        """Generate test report"""
        print("\n" + "="*50)
        print("TEST SUMMARY REPORT")
        print("="*50)
        
        total_tests = len(self.results)
        passed_tests = sum(1 for r in self.results if r['passed'])
        failed_tests = total_tests - passed_tests
        
        print(f"Total Tests: {total_tests}")
        print(f"Passed: {passed_tests}")
        print(f"Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
        
        if failed_tests > 0:
            print("\nFAILED TESTS:")
            for result in self.results:
                if not result['passed']:
                    print(f"  - {result['test']}: {result['message']}")
        
        # Save detailed report
        with open('test_report.json', 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'summary': {
                    'total': total_tests,
                    'passed': passed_tests,
                    'failed': failed_tests,
                    'success_rate': (passed_tests/total_tests)*100
                },
                'results': self.results
            }, f, indent=2)
        
        print("\nDetailed report saved to: test_report.json")
        return failed_tests == 0
    
    def run_all_tests(self):
        """Run all tests"""
        print("Starting TravelMind Stack Integration Tests...")
        print(f"Timestamp: {datetime.now()}")
        
        # Wait for services to be ready
        print("\nWaiting for services to start...")
        time.sleep(30)
        
        # Run all test suites
        self.test_service_health()
        self.test_airflow_api()
        self.test_spark_functionality()
        self.test_minio_functionality()
        self.test_spark_minio_integration()
        self.test_airflow_spark_connection()
        
        # Generate report
        success = self.generate_report()
        
        return success

if __name__ == "__main__":
    tester = TravelMindTester()
    success = tester.run_all_tests()
    sys.exit(0 if success else 1)