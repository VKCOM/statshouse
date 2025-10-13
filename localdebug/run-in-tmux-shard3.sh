#!/bin/bash

SESSION_NAME="sh-local-2s"

echo "Adding third shard (aggregators 7, 8, 9) to existing tmux session..."

# Добавляем третий шард в существующую сессию
# Window 12-14: aggregator shard3 replicas 1..3
tmux new-window -t "$SESSION_NAME:13" -n "agg-s3-r1" -k
tmux send-keys -t "$SESSION_NAME:13" "./run-aggregator7.sh" C-m

tmux new-window -t "$SESSION_NAME:14" -n "agg-s3-r2" -k
tmux send-keys -t "$SESSION_NAME:14" "./run-aggregator8.sh" C-m

tmux new-window -t "$SESSION_NAME:15" -n "agg-s3-r3" -k
tmux send-keys -t "$SESSION_NAME:15" "./run-aggregator9.sh" C-m

echo "Third shard added!"
