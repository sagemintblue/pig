#!/bin/bash

PORT="${1:-8888}"

open "http://localhost:$PORT/contrib/iris/sketches/chord/"
echo "Starting web server on port $PORT" >&2
exec python -m SimpleHTTPServer "$PORT"
