#!/bin/bash
set -e

PROFILE=sh
if [[ $1 ]]; then
  PROFILE=$1
fi

docker compose --profile "$PROFILE" up -d --build --remove-orphans --force-recreate
trap "{ docker compose --profile $PROFILE down; exit; }" exit
echo -n Waiting for services to be ready...
for c in kh sh; do
  if [ "$(docker container inspect -f '{{.State.Status}}' $c 2>/dev/null)" = "running" ]; then
    until docker exec $c clickhouse-client --query='SELECT 1' >/dev/null 2>&1; do echo -n .; sleep 0.2; done
    until curl --output /dev/null --silent --head --fail http://localhost:8123/; do echo -n .; sleep 0.2; done
    break
  fi
done
for c in sh sh-api; do
  if [ "$(docker container inspect -f '{{.State.Status}}' $c 2>/dev/null)" = "running" ]; then
    until curl --output /dev/null --silent --head --fail http://localhost:10888/; do echo -n .; sleep 0.2; done
    URL="http://localhost:10888/view?live&f=-300&t=0&s=__contributors_log_rev"
    case "$OSTYPE" in
      darwin*)  open "$URL" ;;
      linux*)   xdg-open "$URL" ;;
    esac
    break
  fi
done
echo READY
read -r -p "Press ENTER key or CTRL+C to exit."
