#!/bin/sh
set -e

SCRIPTPATH=$(pwd)
DBDIR=$SCRIPTPATH/dbmeta
DBPATH=$DBDIR/db
BINLOGPATH=$DBDIR/binlog
BINLOGPREFIX=$BINLOGPATH/bl

mkdir -p $DBDIR |:
mkdir -p $BINLOGPATH |:
if [ -z "$(ls -A $BINLOGPATH)" ]; then
  ../target/statshouse-metadata -p 2442 --db-path "$DBPATH" --binlog-prefix "$BINLOGPREFIX" --create-binlog "0,1" |:
fi
../target/statshouse-metadata -p 2442 --db-path "$DBPATH" --binlog-prefix "$BINLOGPREFIX" "$@"
