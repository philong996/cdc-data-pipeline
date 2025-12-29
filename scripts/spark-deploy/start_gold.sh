#!/bin/bash

# Deploy gold layer batch pipeline

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
ENVIRONMENT=""
RUN_ONCE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--env)
            ENVIRONMENT="$2"
            shift 2
            ;;
        --run-once)
            RUN_ONCE=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

if [ -z "$ENVIRONMENT" ]; then
    echo "Usage: $0 --env <environment> [--run-once]"
    echo "Example: $0 --env dev"
    echo "         $0 --env dev --run-once"
    exit 1
fi


# Configuration
CONFIG_DIR="${CONFIG_PATH:-$PROJECT_ROOT/config/pipeline_config/}"
CONFIG_FILE="${CONFIG_DIR}config_${ENVIRONMENT}.yaml"
PYTHON_FILE="$PROJECT_ROOT/cdc_pipelines/pipelines/gold/main.py"
LOG_DIR="$PROJECT_ROOT/logs/gold"

echo "Deploying Gold Layer Pipeline"
echo "============================="
echo "Project root: $PROJECT_ROOT"
echo "Config file: $CONFIG_FILE"
echo "Python file: $PYTHON_FILE"
echo "Run once: $RUN_ONCE"
echo ""

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Package dependencies
cd "$PROJECT_ROOT"
echo "Packaging pipeline dependencies..."
zip -q -r pipeline.zip cdc_pipelines -x "**/__pycache__/*" "**/*.pyc"

echo "Submitting Spark job..."

# Build command with optional --run-once flag
CMD_ARGS="--config $CONFIG_DIR --env $ENVIRONMENT"
if [ "$RUN_ONCE" = true ]; then
    CMD_ARGS="$CMD_ARGS --run-once"
fi

spark-submit \
    --master "${SPARK_MASTER}" \
    --driver-memory "${SPARK_DRIVER_MEMORY:-4g}" \
    --conf spark.dynamicAllocation.enabled=false \
    --num-executors "${SPARK_NUM_EXECUTORS:-1}" \
    --executor-memory "${SPARK_EXECUTOR_MEMORY:-4g}" \
    --executor-cores "${SPARK_EXECUTOR_CORES:-2}" \
    --packages io.delta:delta-spark_2.13:4.0.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11 \
    --py-files pipeline.zip \
    "$PYTHON_FILE" \
    $CMD_ARGS \
    > "$LOG_DIR/gold_pipeline.log" 2>&1 &

SPARK_PID=$!
echo "Gold pipeline started with PID: $SPARK_PID"
echo $SPARK_PID > "$LOG_DIR/pipeline.pid"


echo ""
echo "Gold pipeline deployment completed!"
echo "To monitor logs: tail -f $LOG_DIR/gold_pipeline.log"
echo "To stop pipeline: $SCRIPT_DIR/stop_gold.sh"

