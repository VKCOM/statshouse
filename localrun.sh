#!/bin/bash
set -e

PROFILE=sh
if [[ $1 ]]; then
  PROFILE=$1
fi

docker compose --profile "$PROFILE" up -d --build
trap "{ docker compose --profile $PROFILE down; exit; }" exit
echo -n Waiting for services to be ready...
until docker exec kh clickhouse-client --query='SELECT 1' >/dev/null 2>&1; do echo -n .; sleep 0.2; done
if [ "$(docker container inspect -f '{{.State.Status}}' sh-api 2>/dev/null)" = "running" ]; then
  until curl --output /dev/null --silent --head --fail http://localhost:10888/; do echo -n .; sleep 0.2; done
  URL="http://localhost:10888/view?live&f=-300&t=0&s=__contributors_log_rev"
  case "$OSTYPE" in
    darwin*)  open "$URL" ;;
    linux*)   xdg-open "$URL" ;;
  esac
fi
echo READY
read -r -p "Press ENTER key or CTRL+C to exit."
