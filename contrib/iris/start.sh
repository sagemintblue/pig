#!/bin/bash

WEBROOT=$(dirname "$0")/client
PORT="${1:-8888}"

cd "$WEBROOT"
open "http://localhost:$PORT/index.html?localdata=small"
echo "Starting web server on port $PORT" >&2
exec python -m SimpleHTTPServer "$PORT"
