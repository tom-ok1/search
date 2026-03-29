#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$SCRIPT_DIR/.gosearch.pid"
DATA_DIR="$SCRIPT_DIR/data"

echo "==> Stopping gosearch..."

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID"
        # Wait for graceful shutdown
        for i in $(seq 1 10); do
            if ! kill -0 "$PID" 2>/dev/null; then
                break
            fi
            sleep 0.5
        done
        # Force kill if still running
        if kill -0 "$PID" 2>/dev/null; then
            echo "==> Force killing..."
            kill -9 "$PID"
        fi
        echo "==> Server stopped (PID: $PID)"
    else
        echo "==> Server already stopped"
    fi
    rm -f "$PID_FILE"
else
    echo "==> No PID file found"
fi

echo "==> Cleaning up data..."
rm -rf "$DATA_DIR"
rm -f "$SCRIPT_DIR/gosearch"

echo "==> Done! Everything cleaned up."
