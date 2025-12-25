#!/bin/bash

# Deploy bronze layer streaming pipeline

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SPARK_MASTER="spark://node-2:7077"

# Load environment variables
if [ -f "$PROJECT_ROOT/.env" ]; then
    source "$PROJECT_ROOT/.env"
else
    echo "Warning: .env file not found"
fi

# Parse command line arguments
ENVIRONMENT="${1:-dev}"

if [ -z "$ENVIRONMENT" ]; then
    echo "Usage: $0 <environment>"
    echo "Example: $0 dev"
    exit 1
fi

echo "Environment: $ENVIRONMENT"


# Configuration
CONFIG_FILE="${CONFIG_PATH:-$PROJECT_ROOT/config/pipeline_config/}"
PYTHON_FILE="$PROJECT_ROOT/cdc_pipelines/pipelines/bronze/main.py"

echo "Deploying Bronze Layer Pipeline"
echo "================================"
echo "Project root: $PROJECT_ROOT"
echo "Config file: $CONFIG_FILE"
echo "Python file: $PYTHON_FILE"
echo ""


# Package dependencies
cd "$PROJECT_ROOT"
zip -r pipeline.zip cdc_pipelines -x "**/__pycache__/*" "**/*.pyc"

spark-submit \
    --master "${SPARK_MASTER}" \
    --driver-memory "${SPARK_DRIVER_MEMORY:-4g}" \
    --conf spark.dynamicAllocation.enabled=false \
    --num-executors "${SPARK_NUM_EXECUTORS:-1}" \
    --executor-memory "${SPARK_EXECUTOR_MEMORY:-4g}" \
    --executor-cores "${SPARK_EXECUTOR_CORES:-2}" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,io.delta:delta-spark_2.13:4.0.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11 \
    --py-files pipeline.zip \
    "$PYTHON_FILE" \
    --config "$CONFIG_FILE" \
    --env "$ENVIRONMENT" \
    > "$PROJECT_ROOT/logs/bronze/bronze_pipeline.log" 2>&1 &

SPARK_PID=$!
echo "Bronze pipeline started with PID: $SPARK_PID"
echo $SPARK_PID > "$PROJECT_ROOT/logs/bronze/pipeline.pid"

# Cleanup
# rm pipeline.zip

echo ""
echo "Bronze pipeline deployment completed!"