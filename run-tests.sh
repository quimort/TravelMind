#!/bin/bash

echo "=== TravelMind Stack Testing Suite ==="
echo "Starting comprehensive tests..."
echo ""

# Make scripts executable
chmod +x test-stack.sh

# Start the stack if not running
echo "Ensuring stack is running..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to initialize..."
sleep 60

# Run basic health checks
echo "\n=== Running Health Checks ==="
./test-stack.sh

# Run integration tests
echo "\n=== Running Integration Tests ==="
python3 test_integration.py

# Test Airflow DAG
echo "\n=== Testing Airflow DAG ==="
echo "Triggering test DAG..."
curl -X POST \
  "http://localhost:8090/api/v1/dags/test_travelmind_stack/dagRuns" \
  -H "Content-Type: application/json" \
  -u airflow:airflow \
  -d '{}'

echo "\nWaiting for DAG to complete..."
sleep 30

# Check DAG run status
echo "Checking DAG run status..."
curl -s -u airflow:airflow \
  "http://localhost:8090/api/v1/dags/test_travelmind_stack/dagRuns" | \
  python3 -m json.tool

echo "\n=== Test Suite Complete ==="
echo "Check the outputs above for any failures."
echo "Detailed reports available in test_report.json"