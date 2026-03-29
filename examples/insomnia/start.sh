#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA_DIR="$SCRIPT_DIR/data"
PID_FILE="$SCRIPT_DIR/.gosearch.pid"
PORT=9200
BASE_URL="http://localhost:$PORT"

# Clean up previous data if exists
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

echo "==> Building gosearch..."
cd "$PROJECT_ROOT"
go build -o "$SCRIPT_DIR/gosearch" ./cmd/gosearch

echo "==> Starting gosearch on port $PORT..."
cd "$SCRIPT_DIR"
./gosearch &
SERVER_PID=$!
echo "$SERVER_PID" > "$PID_FILE"

# Wait for server to be ready
echo -n "==> Waiting for server"
for i in $(seq 1 30); do
    if curl -s "$BASE_URL/" >/dev/null 2>&1; then
        echo " ready!"
        break
    fi
    # Check if process is still alive
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo " FAILED (server process died)"
        rm -f "$PID_FILE"
        exit 1
    fi
    echo -n "."
    sleep 0.5
done

# Load sample data
echo "==> Creating 'books' index..."
curl -s -X PUT "$BASE_URL/books" -H 'Content-Type: application/json' -d '{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "title":       {"type": "text"},
      "author":      {"type": "text"},
      "year":        {"type": "long"},
      "description": {"type": "text"},
      "tags":        {"type": "keyword"}
    }
  }
}' | python3 -m json.tool

echo ""
echo "==> Loading sample documents..."
curl -s -X POST "$BASE_URL/_bulk" \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary "@$SCRIPT_DIR/sample_data.ndjson" | python3 -m json.tool

echo ""
echo "==> Refreshing index..."
curl -s -X POST "$BASE_URL/books/_refresh" | python3 -m json.tool

echo ""
echo "============================================"
echo "  gosearch is running!"
echo "  URL:  $BASE_URL"
echo "  PID:  $SERVER_PID"
echo "  Data: $DATA_DIR"
echo ""
echo "  Stop with: ./stop.sh"
echo "============================================"
