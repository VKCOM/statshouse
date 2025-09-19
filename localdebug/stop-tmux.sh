#!/bin/bash

SESSION_NAME="sh-local"

echo "Stopping services in tmux session '$SESSION_NAME'..."

# Check if tmux server is running
if ! pgrep tmux >/dev/null 2>&1; then
  echo "Tmux server not running. Nothing to stop."
  exit 0
fi

# Check if session exists
if ! tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
  echo "Session '$SESSION_NAME' not found. Nothing to stop."
  exit 0
fi

# Gracefully stop services by sending Ctrl+C to each window in order
# First: loadgen (window 5) and api (window 4)
if tmux list-windows -t "$SESSION_NAME" 2>/dev/null | grep -q "^5:"; then
  echo "Stopping loadgen (window 5)..."
  tmux send-keys -t "$SESSION_NAME:5" C-c
  sleep 2  # Brief wait for graceful shutdown
fi

if tmux list-windows -t "$SESSION_NAME" 2>/dev/null | grep -q "^4:"; then
  echo "Stopping API (window 4)..."
  tmux send-keys -t "$SESSION_NAME:4" C-c
  sleep 2
  # just in case: api sometimes stays alive
  pkill statshouse-api
fi

# Then: agent (window 3) and aggregator (window 2)
if tmux list-windows -t "$SESSION_NAME" 2>/dev/null | grep -q "^3:"; then
  echo "Stopping agent (window 3)..."
  tmux send-keys -t "$SESSION_NAME:3" C-c
  sleep 2
fi

if tmux list-windows -t "$SESSION_NAME" 2>/dev/null | grep -q "^2:"; then
  echo "Stopping aggregator (window 2)..."
  tmux send-keys -t "$SESSION_NAME:2" C-c
  sleep 2
fi

# Last: metadata (window 1)
if tmux list-windows -t "$SESSION_NAME" 2>/dev/null | grep -q "^1:"; then
  echo "Stopping metadata (window 1)..."
  tmux send-keys -t "$SESSION_NAME:1" C-c
  sleep 2
fi

# Kill the tmux session
tmux kill-session -t "$SESSION_NAME" 2>/dev/null
echo "Tmux session stopped."

# Stop ClickHouse cluster
echo "Stopping ClickHouse cluster..."
./clickhouse-cluster/stop.sh

echo "All services stopped."
