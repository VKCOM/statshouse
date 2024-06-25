#!/bin/bash

curl -v -H "Content-Type: application/json" --data-binary "@$(dirname "$0")/scrape.json" localhost:10888/api/prometheus
echo
