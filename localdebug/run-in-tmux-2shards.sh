#!/bin/bash

SESSION_NAME="sh-local-2s"

wait_for_clickhouse() {
  echo "Waiting for ClickHouse to be healthy..."
  while ! docker ps | grep -q kh.*healthy; do
    sleep 2
  done
  echo "ClickHouse is healthy!"
}

echo 'Building binaries...'
./build.sh
echo 'Build complete! Starting services...'

./clickhouse-cluster/start.sh
wait_for_clickhouse

if [[ -n "$TMUX" ]]; then
  tmux new-session -d -s "$SESSION_NAME"
else
  tmux new-session -d -s "$SESSION_NAME"
fi

# 1: metadata
tmux new-window -t "$SESSION_NAME:1" -n "metadata" -k
tmux send-keys -t "$SESSION_NAME:1" "./run-metadata.sh" C-m

# 2-4: aggregator shard1 replicas 1..3 (config shard)
tmux new-window -t "$SESSION_NAME:2" -n "agg-s1-r1" -k
tmux send-keys -t "$SESSION_NAME:2" "./run-aggregator1.sh" C-m

tmux new-window -t "$SESSION_NAME:3" -n "agg-s1-r2" -k
tmux send-keys -t "$SESSION_NAME:3" "./run-aggregator2.sh" C-m

tmux new-window -t "$SESSION_NAME:4" -n "agg-s1-r3" -k
tmux send-keys -t "$SESSION_NAME:4" "./run-aggregator3.sh" C-m

# 5-7: aggregator shard2 replicas 1..3 (non-config shard)
tmux new-window -t "$SESSION_NAME:5" -n "agg-s2-r1" -k
tmux send-keys -t "$SESSION_NAME:5" "./run-aggregator4.sh" C-m

tmux new-window -t "$SESSION_NAME:6" -n "agg-s2-r2" -k
tmux send-keys -t "$SESSION_NAME:6" "./run-aggregator5.sh" C-m

tmux new-window -t "$SESSION_NAME:7" -n "agg-s2-r3" -k
tmux send-keys -t "$SESSION_NAME:7" "./run-aggregator6.sh" C-m

# 8: API
tmux new-window -t "$SESSION_NAME:8" -n "api" -k
tmux send-keys -t "$SESSION_NAME:8" "./run-api.sh" C-m

# 9: Agent (direct to first shard addresses)
tmux new-window -t "$SESSION_NAME:9" -n "agent" -k
tmux send-keys -t "$SESSION_NAME:9" "./run-agent.sh" C-m

# 10-11: Ingress and second ingress
#tmux new-window -t "$SESSION_NAME:10" -n "ingress1" -k
#tmux send-keys -t "$SESSION_NAME:10" "./run-ingress.sh" C-m
#
#tmux new-window -t "$SESSION_NAME:11" -n "ingress2" -k
#tmux send-keys -t "$SESSION_NAME:11" "./run-ingress2.sh" C-m

# Window 12: Load generator (only started if API is healthy)
tmux new-window -t "$SESSION_NAME:12" -n "loadgen" -k
tmux send-keys -t "$SESSION_NAME:12" "cd .. && go run ./cmd/loadgen client" C-m

echo -n "Waiting for API" && until curl --output /dev/null --silent --head --fail http://localhost:10888/; do echo -n .; sleep 1; done

URL="http://localhost:10888/view?live=1&f=-300&t=0&s=__contributors_log_rev"
case "$OSTYPE" in
  darwin*)  command -v open >/dev/null && open "$URL" ;;
  linux*)   command -v xdg-open >/dev/null && xdg-open "$URL" ;;
esac

tmux select-window -t "$SESSION_NAME:2"
if [[ -n "$TMUX" ]]; then
  tmux switch-client -t "$SESSION_NAME"
else
  tmux attach-session -t "$SESSION_NAME"
fi


