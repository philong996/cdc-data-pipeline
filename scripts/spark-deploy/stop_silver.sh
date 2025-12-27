#!/bin/bash

# Stop silver layer streaming pipeline

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PID_FILE="$PROJECT_ROOT/logs/silver/pipeline.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "Error: PID file not found at $PID_FILE"
    echo "Silver pipeline may not be running"
    exit 1
fi

PID=$(cat "$PID_FILE")

if ps -p $PID > /dev/null; then
    echo "Stopping silver pipeline (PID: $PID)..."
    kill $PID
    
    # Wait for process to stop
    for i in {1..10}; do
        if ! ps -p $PID > /dev/null; then
            echo "Silver pipeline stopped successfully"
            rm "$PID_FILE"
            exit 0
        fi
        sleep 1
    done
    
    # Force kill if still running
    echo "Force stopping pipeline..."
    kill -9 $PID 2>/dev/null || true
    rm "$PID_FILE"
    echo "Silver pipeline force stopped"
else
    echo "Silver pipeline (PID: $PID) is not running"
    rm "$PID_FILE"
fi
