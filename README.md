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
owner, session, custom metadata, and the first terminal `Cause`/`EndedAt`.
`ReleasedAt` separately records cleanup completion.

`Acquire`'s context controls the attempt and carries logging values; it does not
automatically release or revoke the resulting claim. This allows a job to outlive
an HTTP request. A session-oriented application should stop its work when either
the session context or `Claim.Done()` ends, then release after joining that work.
`work, cancel := c.Context(sessionCtx)` wires both cancellation sources and
preserves parent values; always defer `cancel()` to unregister the hook.
`context.Cause(work)` identifies the winning cause. This helper never releases
ownership, including when the parent ends.

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
`Reservation.Token()` and `AcquireOptions.Token` explicitly expose the secret.
Snapshots and default JSON/formatting/slog of either wrapper omit the capability.
Formatting/slog also omit acquisition metadata; JSON preserves metadata.
Explicitly logging the token field, or copying it into metadata, bypasses this protection. Authenticate owner/session
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

Reservations have a public `ID` distinct from their token. `Info()` tracks
`waiting` → `ready` → `consumed`, or termination by cancellation, replacement,
expiry, or shutdown, with creation/readiness/end timestamps. `Done()` closes only
on termination; `Err()` matches `ErrReservationLost` and the specific cause.
`Wait` returns promptly on termination, even if the previous holder is still
cleaning up. A ready, live reservation wins simultaneous context cancellation;
use `Cancel` explicitly to abandon it. Neither cancellation nor replacement
releases the previous holder.

For a takeover based on a displayed snapshot, set `ExpectedClaimID` on
`RevokeWithOptions` or `BeginTransfer`. The comparison and transition are atomic.
A mismatch, including a now-free resource, returns `ErrConflict` without touching
the current holder or reservation. An empty expected ID is unconditional.

Arbitration failures wrap `*ConflictError`: use `errors.As` to inspect
`Operation`, `ExpectedClaimID`, and `Snapshot()`. The detached snapshot was
captured at rejection, rather than through a later potentially stale read.
`errors.Is` continues to match `ErrOccupied`, `ErrReserved`, `ErrInvalidToken`,
and `ErrConflict`. Filter identities/metadata before exposing it to other users.

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

## Lifecycle events

```go
r := claimkit.New("resource", claimkit.Options{
    OnEvent: func(ctx context.Context, e claimkit.Event) {
        // Emit metrics or enqueue into an application-owned audit pipeline.
        record(e.Kind, e.Sequence, e.Revision, e.Claim, e.Reservation)
    },
})
```

Typed events cover acquisition/release, renewal, revocation, lease expiry,
claim/resource shutdown, every reservation transition, and rejected
acquire/renew/revoke/transfer attempts. Expiry discovered through a read emits
the same one-time event as timer expiry. Repeated Wait/Release/Close calls do
not repeat lifecycle events. Denials do not increment state revision.

Payloads are captured under the resource mutex and delivered synchronously
outside it. `Sequence` orders events within the instance; concurrent or reentrant
callbacks may overlap or arrive out of order. Callbacks must be concurrency-safe,
prompt, and must not panic: a panic propagates after state commit without rollback.
A slow callback delays its operation/timer goroutine, but never holds the mutex.
There is no internal queue, overflow policy, replay, or durable delivery; callers
choose these in their own event sink. Events copy metadata and omit tokens.
Keep `Observe` for inexpensive latest-state UI subscriptions.

## Customization and boundaries

| Concern | Application extension point |
| --- | --- |
| Resource granularity | Choose instances per device, operation, file, or tenant; own their registry |
| Identity | Assign arbitrary `Owner.ID` and `Owner.Session`; use an empty owner for unattributed local work |
| Job attributes | Add copied string metadata, such as operation, job ID, or correlation ID |
| Authentication and takeover policy | Authorize before calling Acquire, Revoke, or Transfer; wrap the concrete resource in your service |
| Stop behavior | Use `Claim.Context` or `Done`; join transports, subprocesses, or jobs before Release |
| Session vs. background lifetime | Choose the work context independently of the acquisition request |
| Expiration and renewal | Choose per-acquisition TTL and renewal schedule |
| HTTP, gRPC, WebRTC, or CLI | Map errors and expose selected snapshot fields in your own transport |
| Logs and tracing | Resolve any `slog.Logger` from the operation context |
| Metrics and audit sinks | Handle typed `OnEvent` payloads and choose queue/storage policy externally |
| Stale UI actions | Supply `ExpectedClaimID` for conditional revoke/transfer |
| Existing distributed lease | Keep its authority in the existing backend; adapt its observed state at your application boundary |

This package is a **single-process coordinator**. It does not implement
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
Nil disables logging. Records include component, resource, event, sequence,
revision, occurrence time, public claim/reservation IDs, holder and requester
identity/session, expected claim ID, reason, and error. Renewals and request
denials are debug-level; ordinary lifecycle events are info-level; claim
invalidation (revocation, expiry, shutdown) warns. Metadata and reservation
capabilities are never logged by the library.

Resolvers/handlers run outside the mutex, before the corresponding `OnEvent`,
and must be concurrency-safe and prompt. Timer/lazy-expiry events retain the
acquisition/transfer context values while dropping request cancellation. Use
sequence to order concurrent records. Logs remain diagnostic, not durable audit
storage; both lazy and timer expiry produce the same lifecycle event.

## Migrating from v0.1.x

- Use `errors.Is`, not equality with sentinels: arbitration errors now include
  `ConflictError` snapshots.
- Reservation cancellation/replacement/shutdown promptly unblocks `Wait`, even
  before old-holder cleanup. Only a successful `Wait` establishes readiness.
- A ready reservation wins concurrent caller cancellation. Explicitly `Cancel`
  when abandoning it; waiting-time cancellation still withdraws it.
- Claim reads that discover expiry now persist that cause and signal `Done`.
- Diagnostics use typed reservation lifecycle names; former
  `transfer_requested`/`transfer_ready`/`transfer_failed` log names are replaced.
- AcquireOptions JSON omits `Token`; transport it explicitly over an authorized
  channel. Do not use the options struct as a wire payload for credentials.

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
Observe, contention, reservation-based transfer, JSON logging, event callbacks,
and the optional work-context helper. Structured rejection snapshots now allocate;
they trade fast sentinel-only rejection for a consistent diagnostic state.
The contended benchmark measures **attempts**, including rejected acquisitions;
it does not report successful-operation latency. Benchmarks exclude network,
storage, and device execution. Compare results on the same host and Go version.

See the [measured baselines and v0.2 comparison](docs/benchmarks.md) for environment, results,
and interpretation limits.
