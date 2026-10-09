#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
unformatted=$(gofmt -l .)
if [ -n "$unformatted" ]; then
  printf 'Run gofmt on:\n%s\n' "$unformatted" >&2
  exit 1
fi
go vet ./...
go test -race -timeout=60s -covermode=atomic -coverpkg=./... -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
# Check counts, not the rounded 100.0% display. No excluded packages or files.
awk 'NR > 1 && $3 == 0 { print "Uncovered block:", $0; missing = 1 } END { exit missing }' coverage.out
