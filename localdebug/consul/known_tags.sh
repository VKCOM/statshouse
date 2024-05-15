#!/bin/bash

curl -v -H "Content-Type: application/json" --data-binary "@$(dirname "$0")/known_tags.json" localhost:10888/api/known-tags
echo
