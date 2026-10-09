package claimkit

import (
	"context"
	"time"
)

// EventKind is a stable machine-readable lifecycle or request-denial name.
type EventKind string

const (
	EventAcquired             EventKind = "acquired"
	EventAcquireRejected      EventKind = "acquire_rejected"
	EventRenewed              EventKind = "renewed"
	EventRenewRejected        EventKind = "renew_rejected"
	EventRevoked              EventKind = "revoked"
	EventRevokeRejected       EventKind = "revoke_rejected"
	EventExpired              EventKind = "expired"
	EventReleased             EventKind = "released"
	EventClaimClosed          EventKind = "claim_closed"
	EventClosed               EventKind = "closed"
	EventTransferRejected     EventKind = "transfer_rejected"
	EventReservationCreated   EventKind = "reservation_created"
	EventReservationReady     EventKind = "reservation_ready"
	EventReservationConsumed  EventKind = "reservation_consumed"
	EventReservationCancelled EventKind = "reservation_cancelled"
	EventReservationReplaced  EventKind = "reservation_replaced"
	EventReservationExpired   EventKind = "reservation_expired"
	EventReservationClosed    EventKind = "reservation_closed"
)

// Event is captured under the resource lock and delivered synchronously outside
// it, once per transition or denied attempt. Repeated Wait/Release calls do not
// repeat lifecycle events. Snapshots are detached and never contain capabilities.
// RequestedOwner identifies an Acquire caller, including a denied caller.
//
// Hooks may overlap and arrive out of order across concurrent or reentrant calls;
// Sequence gives their order within this Resource. Revision orders state changes
// only, so denials do not advance it. There is no internal queue, dropping, replay,
// or persistence. Slow hooks delay their operation/timer goroutine, not the lock.
// Hooks and loggers must be concurrency-safe, prompt, and must not panic; a panic
// propagates after commit and does not roll back state. Enqueue externally for
// async delivery and choose your own capacity, overflow, and persistence policy.
// Observe remains the inexpensive, coalesced latest-state subscription.
type Event struct {
	Kind            EventKind
	Resource        string
	Sequence        uint64
	Revision        uint64
	At              time.Time
	Claim           *ClaimInfo
	Reservation     *ReservationInfo
	RequestedOwner  Owner
	ExpectedClaimID string
	Err             error
}

type emission struct {
	ctx   context.Context
	event Event
}

// recordLocked copies payloads only when an observer or logger is configured.
func (r *Resource) recordLocked(ctx context.Context, kind EventKind, c *Claim, p *Reservation, owner Owner, expected string, err error, events *[]emission) {
	if r.opts.OnEvent == nil && r.opts.Logger == nil {
		return
	}
	r.sequence++
	e := Event{Kind: kind, Resource: r.name, Sequence: r.sequence, Revision: r.revision, At: time.Now(), RequestedOwner: owner, ExpectedClaimID: expected, Err: err}
	if c != nil {
		info := c.infoLocked()
		e.Claim = &info
	}
	if p != nil {
		info := p.info
		e.Reservation = &info
	}
	*events = append(*events, emission{ctx, e})
}

// finish is deferred immediately after locking, including read methods that can
// discover an overdue deadline. Nothing supplied by the caller runs under mu.
func (r *Resource) finish(events *[]emission) {
	r.mu.Unlock()
	for _, item := range *events {
		r.log(item.ctx, item.event)
		if r.opts.OnEvent != nil {
			r.opts.OnEvent(item.ctx, item.event)
		}
	}
}
