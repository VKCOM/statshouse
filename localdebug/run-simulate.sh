#!/bin/sh

../target/statshouse simulator -agg-addr=127.0.0.1:13336,127.0.0.1:13336,127.0.0.1:13336 "$@"
