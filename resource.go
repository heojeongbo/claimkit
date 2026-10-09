package claimkit

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"errors"
	"maps"
	"sync"
	"time"
)

// Resource is one exclusive ownership domain. All callers protecting the same
// underlying work must share the same instance. Names are labels, not a global
// registry: two instances with the same name are independent.
type Resource struct {
	mu       sync.Mutex
	name     string
	opts     Options
	current  *Claim
	pending  *Reservation
	closed   bool
	revision uint64
	sequence uint64
	changed  chan struct{}
}

// New constructs a resource without starting background work.
func New(name string, opts Options) *Resource {
	return &Resource{name: name, opts: opts, changed: make(chan struct{})}
}

// Acquire immediately attempts ownership; it never queues. ctx controls this
// attempt and carries logging values, NOT the lifetime of the resulting claim.
// The holder must observe Claim.Done and Release after its work has stopped.
func (r *Resource) Acquire(ctx context.Context, owner Owner, opts AcquireOptions) (claim *Claim, err error) {
	var events []emission
	r.mu.Lock()
	defer r.finish(&events)
	defer func() {
		if err != nil {
			r.recordLocked(ctx, EventAcquireRejected, r.current, r.pending, owner, "", err, &events)
		}
	}()
	if opts.TTL < 0 {
		return nil, ErrInvalidTTL
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.closed {
		return nil, ErrClosed
	}
	r.expireLocked(&events)
	if r.current != nil {
		return nil, r.conflictLocked("acquire", "", ErrOccupied)
	}
	if r.pending != nil {
		if opts.Token == "" {
			return nil, r.conflictLocked("acquire", "", ErrReserved)
		}
		if subtle.ConstantTimeCompare([]byte(r.pending.token), []byte(opts.Token)) != 1 || (r.pending.info.OwnerBound && r.pending.info.Owner != owner) {
			return nil, r.conflictLocked("acquire", "", ErrInvalidToken)
		}
		r.pending.endLocked(ctx, ReservationConsumed, EventReservationConsumed, ErrReservationConsumed, &events)
	} else if opts.Token != "" {
		return nil, r.conflictLocked("acquire", "", ErrInvalidToken)
	}
	c := &Claim{
		r:    r,
		info: ClaimInfo{ID: rand.Text(), Owner: owner, AcquiredAt: time.Now(), Metadata: maps.Clone(opts.Metadata)},
		done: make(chan struct{}), released: make(chan struct{}),
		logctx: context.WithoutCancel(ctx),
	}
	r.current = c
	if opts.TTL > 0 {
		c.armLocked(opts.TTL)
	}
	r.notifyLocked()
	r.recordLocked(ctx, EventAcquired, c, nil, owner, "", nil, &events)
	return c, nil
}

// Observe atomically returns a detached snapshot and an invalidation channel.
// On closure, call Observe again. Changes may coalesce; this is a latest-state
// subscription, not an event stream. No per-subscriber goroutine is created.
// Stop when Snapshot.Closed is true or your own context ends.
func (r *Resource) Observe() (Snapshot, <-chan struct{}) {
	var events []emission
	r.mu.Lock()
	defer r.finish(&events)
	r.expireLocked(&events)
	return r.snapshotLocked(), r.changed
}

func (r *Resource) snapshotLocked() Snapshot {
	s := Snapshot{Resource: r.name, Revision: r.revision, Closed: r.closed}
	if r.current != nil {
		info := r.current.infoLocked()
		s.Claim = &info
	}
	if r.pending != nil {
		info := r.pending.info
		s.Reservation = &info
	}
	return s
}

// Revoke requests cleanup of the current holder and waits for its Release.
// It reserves no successor. Authorization belongs to the application. A timeout
// leaves revocation in effect and wraps ErrNotReleased and the context error.
func (r *Resource) Revoke(ctx context.Context, reason string) error {
	return r.RevokeWithOptions(ctx, RevokeOptions{Reason: reason})
}

// RevokeWithOptions compares ExpectedClaimID and requests cleanup atomically.
// Observing one claim and revoking with that ID cannot revoke a newer one.
func (r *Resource) RevokeWithOptions(ctx context.Context, opts RevokeOptions) error {
	c, err := r.requestRevoke(ctx, opts)
	if err != nil {
		return err
	}
	if c == nil {
		return nil
	}
	return waitReleased(ctx, c.released)
}

func (r *Resource) requestRevoke(ctx context.Context, opts RevokeOptions) (claim *Claim, err error) {
	var events []emission
	r.mu.Lock()
	defer r.finish(&events)
	defer func() {
		if err != nil {
			r.recordLocked(ctx, EventRevokeRejected, r.current, r.pending, Owner{}, opts.ExpectedClaimID, err, &events)
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.closed {
		return nil, ErrClosed
	}
	r.expireLocked(&events)
	if !r.matchesLocked(opts.ExpectedClaimID) {
		return nil, r.conflictLocked("revoke", opts.ExpectedClaimID, ErrConflict)
	}
	if r.pending != nil {
		return nil, r.conflictLocked("revoke", opts.ExpectedClaimID, ErrReserved)
	}
	c := r.current
	if c != nil {
		c.invalidateLocked(ctx, ErrRevoked, CauseRevoked, opts.Reason, EventRevoked, time.Now(), &events)
	}
	return c, nil
}

func (r *Resource) matchesLocked(id string) bool {
	return id == "" || (r.current != nil && r.current.info.ID == id)
}

// Close permanently rejects acquisitions, ends reservations and requests holder
// cleanup. It is retryable after a timeout and never releases live work itself.
func (r *Resource) Close(ctx context.Context) error {
	c, err := r.beginClose(ctx)
	if err != nil {
		return err
	}
	if c != nil {
		return waitReleased(ctx, c.released)
	}
	return nil
}

func (r *Resource) beginClose(ctx context.Context) (*Claim, error) {
	var events []emission
	r.mu.Lock()
	defer r.finish(&events)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.expireLocked(&events)
	first := !r.closed
	r.closed = true
	if r.pending != nil {
		r.pending.endLocked(ctx, ReservationClosed, EventReservationClosed, ErrClosed, &events)
	}
	c := r.current
	if c != nil {
		c.invalidateLocked(ctx, ErrClosed, CauseClosed, "resource closed", EventClaimClosed, time.Now(), &events)
	}
	if first {
		r.notifyLocked()
		r.recordLocked(ctx, EventClosed, c, nil, Owner{}, "", nil, &events)
	}
	return c, nil
}

func (r *Resource) expireLocked(events *[]emission) {
	now := time.Now()
	if r.current != nil {
		r.current.expireLocked(events)
	}
	r.expireReservationLocked(now, events)
}

func (r *Resource) notifyLocked() {
	r.revision++
	close(r.changed)
	r.changed = make(chan struct{})
}

func waitReleased(ctx context.Context, released <-chan struct{}) error {
	select {
	case <-released:
	case <-ctx.Done():
	}
	select {
	case <-released:
		return nil
	default:
		return errors.Join(ErrNotReleased, ctx.Err())
	}
}
