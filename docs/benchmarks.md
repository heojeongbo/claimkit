# Initial performance baseline

Measured on 2026-10-09 with Go 1.26.4, macOS arm64, Apple M4 Pro.
These are in-process microbenchmarks, not service latency or deployment SLOs.

```sh
go test -run '^$' -bench . -benchmem -benchtime=200ms -count=3 -cpu=1,8 ./...
```

Median of three runs with `GOMAXPROCS=8`:

| Operation | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Acquire + Release | 480.9 | 720 | 7 |
| Acquire + Release with TTL | 593.6 | 848 | 9 |
| Reject an occupied acquisition | 33.79 | 0 | 0 |
| Check an untimed claim | 3.891 | 0 | 0 |
| Observe an occupied resource | 65.39 | 160 | 1 |
| Transfer a free resource, consume reservation, Release | 890.4 | 1120 | 13 |
| Acquire + Release with JSON logging to io.Discard | 1918 | 1089 | 11 |
| Parallel contended acquisition attempt | 360.0 | 397 | 3 |

Only the contended benchmark uses parallel callers. It includes rejected attempts
and therefore must not be read as successful acquisition latency. Its allocation
counts depend on the fraction of attempts that succeed. Logging to a real file
or exporter introduces additional costs. Observe copies the public snapshot;
custom metadata adds map-copy cost. Check here has no deadline, so it excludes
the clock read required for a timed claim.

The acquire/release path allocates per-acquisition state, lifecycle channels,
notification channels, context retention, and a random public identifier. This
cost is paid when ownership changes, not for each protected command. Timed
claims add a timer; there is no per-resource idle loop or per-observer goroutine.

The suite passed `go vet`, 100% package statement coverage with the race detector,
and 25 repeated race-test runs. This does not prove all concurrent schedules or
external side effects are safe. Re-run on the same host/toolchain when comparing
changes; CI benchmark output on shared runners is informational.

## Release validation in the devcontainer

The v0.1.0 source, including two-phase transfer, was also validated on
2026-10-09 in the shipped devcontainer (Linux arm64 on the same Apple M4 Pro host).
`sh scripts/check.sh` and 25 repeated race runs passed, with 100% statements.
The command below ran three samples at GOMAXPROCS 1 and 4:

```sh
go test -run '^$' -bench . -benchmem -benchtime=200ms -count=3 -cpu=1,4 ./...
```

Median at GOMAXPROCS=4:

| Benchmark | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| AcquireRelease | 467.1 | 720 | 7 |
| AcquireReleaseTTL | 653.6 | 848 | 9 |
| Occupied | 40.65 | 0 | 0 |
| Check | 3.929 | 0 | 0 |
| Observe | 88.87 | 160 | 1 |
| Contended | 403.7 | 545 | 5 |
| Transfer | 861 | 1184 | 14 |
| Logging | 2044 | 1088 | 11 |
