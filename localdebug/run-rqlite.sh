#!/bin/sh
set -e

# > git clone https://github.com/rqlite/rqlite.git
# rqlite> cd rqlite
# rqlite> go install ./cmd/...

# if you need to examine DB, use
# rqlite> go run ./cmd/rqlite
# or
# > rqlite

# to create initial tables, run
# func TestCreateSchema(t *testing.T) {
# commenting out t.Skip()

mkdir -p dbrqlite
rqlited dbrqlite/1
