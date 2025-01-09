#!/bin/sh

BASEDIR=$(dirname "$0")

exec docker compose --project-directory $BASEDIR down
