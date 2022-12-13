#!/bin/bash
set -e

if [[ "$1" == "down" ]]; then
  docker compose --profile sh down
  exit 0
fi

docker compose --profile sh up -d
echo -n Waiting for services to be ready...
until docker exec sh-aggregator-kh clickhouse-client --query='SELECT 1' >/dev/null 2>&1; do echo -n .; sleep 0.2; done
until curl --output /dev/null --silent --head --fail http://localhost:10888/; do echo -n .; sleep 0.2; done
echo READY
URL="http://localhost:10888/view?f=-300&t=0&s=__contributors_log_rev"
$(open "$URL" 2> /dev/null) || $(xdg-open "$URL")
