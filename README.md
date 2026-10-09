# claimkit

Exclusive ownership, visible holders, and cooperative handoff for Go.

`claimkit` answers **who owns this resource, which acquisition is still valid,
and when it is safe to hand it to the next owner**. Use it for editing sessions,
device configuration, maintenance operations, or background jobs.

Go 1.25+. Standard library only. MIT licensed.

## Quick start

```go
r := claimkit.New("devices/42/config", claimkit.Options{})
owner := claimkit.Owner{ID: "user-7", Session: "session-2"}

c, err := r.Acquire(ctx, owner, claimkit.AcquireOptions{
    Metadata: map[string]string{"operation": "configure"},
})
if err != nil {
    return err // errors.Is(err, claimkit.ErrOccupied), for example
}
defer c.Release(ctx) // only after all work for this acquisition has stopped

if err := c.Check(); err != nil {
    return err
}
// Perform the protected work. Long-lived work must also observe c.Done().
```

Share one `Resource` instance among **all** callers of an ownership domain.
Names do not register resources globally. Two calls to `New` with the same name
create independent domains. Applications own the registry and its lifetime.

## Lifetime and handoff

An acquisition has two distinct endings:

1. `Done()` closes: stop accepting work and cancel/join outstanding work.
2. `Release(ctx)` confirms cleanup: only now may the next acquisition succeed.

`Release` is idempotent and scoped to its acquisition. A late release from an
old session cannot affect a new session, even for the same user. `Released()`
lets other goroutines wait for cleanup completion. `Info()` includes timestamps,
owner, session, custom metadata, and the first applied revocation reason.

`Acquire`'s context controls the attempt and carries logging values; it does not
automatically release or revoke the resulting claim. This allows a job to outlive
an HTTP request. A session-oriented application should stop its work when either
the session context or `Claim.Done()` ends, then release after joining that work.

```go
// The current holder must already be observing Done and releasing after cleanup.
next := claimkit.Owner{ID: "next-user", Session: "next-session"}
reservation, err := r.Transfer(ctx, next, 30*time.Second, "requested handoff")
if err != nil {
    return err
}
defer reservation.Cancel(ctx) // harmless after consumption

// This may be a later request, after securely transporting the token.
c, err := r.Acquire(ctx, next, claimkit.AcquireOptions{
    Token: reservation.Token(),
})
if err != nil {
    return err
}
defer c.Release(ctx)
```

The reservation is installed **before** revocation. Its TTL starts after cleanup
completes, so a slow shutdown cannot consume the next owner's claim window.
Overlapping transfers return `ErrReserved`. A timeout withdraws its reservation
and leaves revocation in effect; it never pretends that the old work stopped.
`errors.Is` recognizes both `ErrNotReleased` and the context error.

Tokens are random, single-use, resource-bound capabilities, also bound to the
specified owner and session. Stale tokens fail even when a resource is free.
Only `Token()` deliberately exposes the secret. Snapshots, JSON encoding, normal
formatting, and structured logs do not expose it. Authenticate owner/session
values at your transport boundary; do not accept them on trust from a client.

`Revoke(ctx, reason)` stops the current acquisition without reserving its
successor. `Close(ctx)` permanently closes the resource and waits for cleanup;
after a timeout it can be called again. Neither forces live work to release.

For a transport that does not yet know the next session, use
`BeginTransfer(ctx, TransferOptions{TTL: ttl, Reason: reason})`: a nil `Next`
explicitly selects a bearer-only capability. Pass `Next: &owner` for an exact
owner/session binding. `Replace: true` explicitly supersedes a pending
reservation. `Previous()` returns the holder captured atomically at handoff;
`Wait(ctx)` waits for cleanup and withdraws this reservation on timeout. An old
waiter cannot withdraw a replacement. A reservation whose caller never calls
Wait or Cancel can remain pending until the previous holder releases; callers
must own this lifecycle. The ordinary `Transfer` helper waits automatically and
continues to reject overlapping transfers.

## Leases

Set `AcquireOptions.TTL` to enable expiry; zero means no deadline. Renew with
`c.Renew(ctx, ttl)`. Applications control heartbeats and retry policy.

Expiry invalidates the claim and closes `Done`; it does **not** make the resource
available until cleanup calls `Release`. An expired lease cannot be renewed,
even if the timer goroutine has not run. This avoids granting a successor while
the old holder may still be performing external side effects.

## Observe without missed wakeups

```go
for {
    snapshot, changed := r.Observe()
    render(snapshot)
    if snapshot.Closed {
        break
    }
    select {
    case <-changed:
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

The snapshot and invalidation channel are obtained atomically. Multiple updates
may coalesce; this is a latest-state subscription, not an audit log. Observers
create no library goroutines or subscriptions needing cleanup. Snapshots and
metadata maps are detached copies. Revisions are local to this resource instance.

## Customization and boundaries

| Concern | Application extension point |
| --- | --- |
| Resource granularity | Choose instances per device, operation, file, or tenant; own their registry |
| Identity | Assign arbitrary `Owner.ID` and `Owner.Session`; use an empty owner for unattributed local work |
| Job attributes | Add copied string metadata, such as operation, job ID, or correlation ID |
| Authentication and takeover policy | Authorize before calling Acquire, Revoke, or Transfer; wrap the concrete resource in your service |
| Stop behavior | Observe `Claim.Done`; cancel transports, subprocesses, or jobs and join them before Release |
| Session vs. background lifetime | Choose the work context independently of the acquisition request |
| Expiration and renewal | Choose per-acquisition TTL and renewal schedule |
| HTTP, gRPC, WebRTC, or CLI | Map errors and expose selected snapshot fields in your own transport |
| Logs and tracing | Resolve any `slog.Logger` from the operation context |
| Existing distributed lease | Keep its authority in the existing backend; adapt its observed state at your application boundary |

This initial package is a **single-process coordinator**. It does not implement
distributed consensus, persistent restart recovery, FIFO scheduling, multiple
readers, hierarchical ownership, or atomic acquisition of multiple resources.
There is no pluggable distributed backend contract yet. A durable store needs its
own fencing and failure semantics; putting a local claim around it is insufficient.

`Check` is a point-in-time check, not an atomic transaction with your subsequent
command. Stop/join work before release and validate authority at the executor
when needed. Public claim IDs and revisions are not credentials or durable
fencing tokens. Out-of-band actors must participate in the same enforcement path.

## Context logging

The context-carried logging approach follows
[lesomnus/go-app](https://github.com/lesomnus/go-app), without importing an
application runtime or configuring process-wide telemetry in this library:

```go
// import "github.com/lesomnus/otx/log" in your go-app application
r := claimkit.New("resource", claimkit.Options{Logger: log.From})
```

For plain slog, supply `func(context.Context) *slog.Logger { return logger }`.
Nil disables logging. Records include component, resource, event, public claim
ID, revision, owner/session, reason, and error. Renewal is debug-level; normal
lifecycle events are info-level; revocation, expiry, and failed handoffs warn.
Metadata and reservation capabilities are never logged by the library.

Resolvers/handlers run outside the resource mutex and must be concurrency-safe
and prompt. Timer callbacks retain acquisition/transfer context values while
dropping request cancellation. Concurrent records can arrive out of order; use
revision to correlate them. Logs are diagnostic, not a durable audit stream;
lazy deadline cleanup may coalesce reservation-expiry diagnostics.

## Validation and performance

The devcontainer follows go-app's Compose layout and Go development image,
with module/build caches in named volumes. It needs no privileged mode or
host credential mounts. Open this repository in VS Code Dev Containers, or:

```sh
devcontainer up --workspace-folder . --config .devcontainer/devcontainer.json
devcontainer exec --workspace-folder . --config .devcontainer/devcontainer.json \
  sh scripts/check.sh
```

```sh
sh scripts/check.sh
go test -race -count=25 -timeout=120s ./...
go test -run '^$' -bench . -benchmem -benchtime=200ms -count=3 -cpu=1,8 ./...
```

The check script requires **100% statement coverage across every package**, using
uncovered block counts rather than rounded percentages, and runs `go vet` and
the race detector. Tests cover cleanup ordering, stale releases, anonymous
ownership, same-user sessions, token replay, cancellation, shutdown, concurrent
handoff, delayed timers, renewals, context logging, and snapshot isolation.
Executable examples show application integration. Statement coverage does not
measure every possible interleaving or prove external-system correctness.

Benchmarks measure ordinary/timed acquire-release, occupied rejection, Check,
Observe, contention, reservation-based transfer, and JSON logging overhead.
The contended benchmark measures **attempts**, including rejected acquisitions;
it does not report successful-operation latency. Benchmarks exclude network,
storage, and device execution. Compare results on the same host and Go version.

See the [initial measured baseline](docs/benchmarks.md) for environment, results,
and interpretation limits.
