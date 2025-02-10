#!/bin/bash
set -e

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

export BUILD_COMMIT="$(git log --format="%H" -n 1)"
export BUILD_COMMIT_TS="$(git log --format="%ct" -n 1)"
export BUILD_MACHINE="$(uname -n -m -r -s)"
export BUILD_TIME="$(date +%FT%T%z)"
docker compose -f localrun.yml up -d --remove-orphans $@ # --build --force-recreate
trap "{ docker compose -f localrun.yml down; exit; }" exit
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
    URL="http://localhost:10888/view?live=1&f=-300&t=0&s=__contributors_log_rev"
    case "$OSTYPE" in
      darwin*)  if check_macos_gui ; then open "$URL" ; fi ;;
      linux*) if [[ -n "$XDG_CURRENT_DESKTOP" ]] ; then xdg-open "$URL" ; fi ;;
    esac
    break
  fi
done
echo READY
read -r -p "Press ENTER key or CTRL+C to exit."
