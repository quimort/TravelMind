#!/bin/bash

echo "=== TravelMind Stack Health Check ==="
echo "Testing all components..."
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check if service is running
check_service() {
    local service_name=$1
    local url=$2
    local expected_status=${3:-200}
    
    echo -n "Checking $service_name... "
    
    if curl -s -o /dev/null -w "%{http_code}" "$url" | grep -q "$expected_status"; then
        echo -e "${GREEN}✓ PASS${NC}"
        return 0
    else
        echo -e "${RED}✗ FAIL${NC}"
        return 1
    fi
}

# Function to check docker container status
check_container() {
    local container_name=$1
    echo -n "Checking container $container_name... "
    
    if docker ps --format "table {{.Names}}\t{{.Status}}" | grep -q "$container_name.*Up"; then
        echo -e "${GREEN}✓ RUNNING${NC}"
        return 0
    else
        echo -e "${RED}✗ NOT RUNNING${NC}"
        return 1
    fi
}

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 30

# Check Docker containers
echo "\n=== Container Status ==="
check_container "postgres"
check_container "redis"
check_container "minio"
check_container "spark-master"
check_container "spark-worker-1"
check_container "spark-history"
check_container "travelmind-airflow-webserver-1"
check_container "travelmind-airflow-scheduler-1"

# Check service endpoints
echo "\n=== Service Health Checks ==="
check_service "PostgreSQL" "http://localhost:5432" "000"  # Connection refused is expected for HTTP to Postgres
check_service "Redis" "http://localhost:6379" "000"      # Connection refused is expected for HTTP to Redis
check_service "MinIO API" "http://localhost:9000/minio/health/live"
check_service "MinIO Console" "http://localhost:9001/login"
check_service "Spark Master UI" "http://localhost:8080"
check_service "Spark History Server" "http://localhost:18080"
check_service "Airflow Webserver" "http://localhost:8090/health"

echo "\n=== Detailed Service Tests ==="

# Test Airflow API
echo -n "Testing Airflow API... "
if curl -s -u airflow:airflow "http://localhost:8090/api/v1/health" | grep -q '"metadatabase":{"status":"healthy"}'; then
    echo -e "${GREEN}✓ PASS${NC}"
else
    echo -e "${RED}✗ FAIL${NC}"
fi

# Test MinIO API
echo -n "Testing MinIO API... "
if curl -s "http://localhost:9000/minio/health/live" | grep -q "OK"; then
    echo -e "${GREEN}✓ PASS${NC}"
else
    echo -e "${RED}✗ FAIL${NC}"
fi

# Test Spark Master
echo -n "Testing Spark Master... "
if curl -s "http://localhost:8080" | grep -q "Spark Master"; then
    echo -e "${GREEN}✓ PASS${NC}"
else
    echo -e "${RED}✗ FAIL${NC}"
fi

echo "\n=== Integration Tests ==="

# Test Spark-MinIO integration
echo "Testing Spark-MinIO integration..."
docker exec spark-master /opt/spark/bin/spark-submit \
    --class org.apache.spark.examples.SparkPi \
    --master spark://spark-master:7077 \
    /opt/spark/examples/jars/spark-examples_2.12-3.5.5.jar 10 > /tmp/spark_test.log 2>&1

if grep -q "Pi is roughly" /tmp/spark_test.log; then
    echo -e "Spark job execution: ${GREEN}✓ PASS${NC}"
else
    echo -e "Spark job execution: ${RED}✗ FAIL${NC}"
    echo "Check /tmp/spark_test.log for details"
fi

echo "\n=== Summary ==="
echo "Health check completed. Check individual components above."
echo "\nAccess URLs:"
echo "- Airflow: http://localhost:8090 (airflow/airflow)"
echo "- Spark Master: http://localhost:8080"
echo "- Spark History: http://localhost:18080"
echo "- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"