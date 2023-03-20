#!/bin/bash
trap "trap - SIGTERM && kill -- -$$ && wait -$$" SIGINT SIGTERM
for i in $(seq "$1"); do
   echo -n "$i"
   go run . &
done
echo " (running $1 workers)"
echo Press Ctrl+C to exit.
for job in $(jobs -p); do
    wait "$job"
done
