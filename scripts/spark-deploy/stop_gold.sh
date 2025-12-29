#!/bin/bash

# Stop gold layer pipeline

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="$PROJECT_ROOT/logs/gold"
PID_FILE="$LOG_DIR/pipeline.pid"

echo "Stopping Gold Layer Pipeline"
echo "============================"

if [ ! -f "$PID_FILE" ]; then
    echo "PID file not found: $PID_FILE"
    echo "Pipeline may not be running or was started manually"
    exit 1
fi

PID=$(cat "$PID_FILE")

if ps -p "$PID" > /dev/null 2>&1; then
    echo "Stopping pipeline with PID: $PID"
    kill "$PID"
    
    # Wait for process to stop
    for i in {1..10}; do
        if ! ps -p "$PID" > /dev/null 2>&1; then
            echo "Pipeline stopped successfully"
            rm "$PID_FILE"
            exit 0
        fi
        sleep 1
    done
    
    # Force kill if still running
    echo "Pipeline did not stop gracefully, force killing..."
    kill -9 "$PID" 2>/dev/null || true
    rm "$PID_FILE"
    echo "Pipeline force stopped"
else
    echo "Process with PID $PID is not running"
    rm "$PID_FILE"
fi

echo "Gold pipeline stopped"
