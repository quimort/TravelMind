#!/bin/bash

# Wait for MinIO to be ready
until mc alias set myminio http://minio:9000 minioadmin minioadmin; do
    echo "MinIO is not ready yet. Retrying in 5 seconds..."
    sleep 5
done

echo "MinIO is ready. Setting up buckets and users..."

# Create bucket
mc mb myminio/bucket1

# Upload sample data
mc cp /sample-data.csv myminio/bucket1/sample-data.csv

# Add new user
mc admin user add myminio testuser password

# Create policy
mc admin policy create myminio read-write /policy.json

# Attach policy to user
mc admin policy attach myminio read-write --user testuser

echo "MinIO setup completed successfully!"