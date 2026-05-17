#!/bin/bash
# ================================================
# MinIO Bucket Initialization Script
# Runs once on startup to create required buckets
# ================================================

# Wait for MinIO to be ready
echo "Waiting for MinIO to start..."
sleep 5

# Configure MinIO client
mc alias set local http://minio:9000 admin admin123

# Create buckets if they don't exist
# Why --ignore-existing: safe to run multiple times
mc mb --ignore-existing local/bronze
mc mb --ignore-existing local/silver
mc mb --ignore-existing local/gold

echo "✅ All buckets created successfully!"