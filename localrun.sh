#!/bin/bash
set -e

docker compose --profile sh up -d --build
trap "{ docker compose --profile sh down; exit; }" exit
echo -n Waiting for services to be ready...
until docker exec sh-aggregator-kh clickhouse-client --query='SELECT 1' >/dev/null 2>&1; do echo -n .; sleep 0.2; done
until curl --output /dev/null --silent --head --fail http://localhost:10888/; do echo -n .; sleep 0.2; done
echo READY
URL="http://localhost:10888/view?f=-300&t=0&s=__contributors_log_rev"
case "$OSTYPE" in
  darwin*)  open "$URL" ;;
  linux*)   xdg-open "$URL" ;;
esac
read -r -p "Press ENTER key or CTRL+C to exit."
