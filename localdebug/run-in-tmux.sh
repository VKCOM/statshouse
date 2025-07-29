#!/bin/bash

SESSION_NAME="sh-local"

# Function to wait for ClickHouse to be healthy
wait_for_clickhouse() {
  echo "Waiting for ClickHouse to be healthy..."
  while ! docker ps | grep -q kh.*healthy; do
    sleep 2
  done
  echo "ClickHouse is healthy!"
}

# First, build the binaries in the root directory
echo 'Building binaries...'
./build.sh
echo 'Build complete! Starting services...'

./clickhouse-cluster/start.sh
wait_for_clickhouse

# Check if we're inside tmux already
if [[ -n "$TMUX" ]]; then
  echo "Note: Already inside tmux, creating nested session..."
  # Create session without attaching (we'll attach later)
  tmux new-session -d -s "$SESSION_NAME"
else
  # Create new session and attach
  tmux new-session -d -s "$SESSION_NAME"
fi

# Window 1: Metadata service
tmux new-window -t "$SESSION_NAME:1" -n "metadata" -k
tmux send-keys -t "$SESSION_NAME:1" "./run-metadata.sh" C-m

# Window 2: Aggregator service
tmux new-window -t "$SESSION_NAME:2" -n "aggregator" -k
tmux send-keys -t "$SESSION_NAME:2" "./run-aggregator.sh" C-m

# Window 3: Agent service
tmux new-window -t "$SESSION_NAME:3" -n "agent" -k
tmux send-keys -t "$SESSION_NAME:3" "./run-agent.sh" C-m

# Window 4: API service
tmux new-window -t "$SESSION_NAME:4" -n "api" -k
tmux send-keys -t "$SESSION_NAME:4" "./run-api.sh" C-m

# Window 5: Load generator (only started if API is healthy)
tmux new-window -t "$SESSION_NAME:5" -n "loadgen" -k
tmux send-keys -t "$SESSION_NAME:5" "cd .. && go run ./cmd/loadgen client" C-m


echo -n "Waiting for API" && until curl --output /dev/null --silent --head --fail http://localhost:10888/; do echo -n .; sleep 1; done

# copypasted https://stackoverflow.com/questions/54995983/how-to-detect-availability-of-gui-in-bash-shell
check_macos_gui() {
  command -v swift >/dev/null && swift <(cat <<"EOF"
import Security
var attrs = SessionAttributeBits(rawValue:0)
let result = SessionGetInfo(callerSecuritySession, nil, &attrs)
exit((result == 0 && attrs.contains(.sessionHasGraphicAccess)) ? 0 : 1)
EOF
)
}
URL="http://localhost:10888/view?live=1&f=-300&t=0&s=__contributors_log_rev"
case "$OSTYPE" in
  darwin*)  if check_macos_gui ; then open "$URL" ; fi ;;
  linux*) if [[ -n "$XDG_CURRENT_DESKTOP" ]] ; then xdg-open "$URL" ; fi ;;
esac

# Select the agent window and attach to the session
tmux select-window -t "$SESSION_NAME:3"
if [[ -n "$TMUX" ]]; then
  tmux switch-client -t "$SESSION_NAME"
else
  tmux attach-session -t "$SESSION_NAME"
fi
