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
	changed  chan struct{}
}

// New constructs a resource. No goroutine starts until a timed claim or
// reservation is created. The caller chooses the scope and name of the resource.
func New(name string, opts Options) *Resource {
	return &Resource{name: name, opts: opts, changed: make(chan struct{})}
}

// Acquire immediately attempts ownership; it never queues. ctx controls this
// attempt and carries logging values, NOT the lifetime of the resulting claim.
// The holder must observe Claim.Done and Release after its work has stopped.
func (r *Resource) Acquire(ctx context.Context, owner Owner, opts AcquireOptions) (*Claim, error) {
	if opts.TTL < 0 {
		return nil, ErrInvalidTTL
	}
	r.mu.Lock()
	if err := ctx.Err(); err != nil {
		r.mu.Unlock()
		return nil, err
	}
	if r.closed {
		r.mu.Unlock()
		return nil, ErrClosed
	}
	r.expireReservationLocked(time.Now())
	if r.current != nil {
		r.mu.Unlock()
		return nil, ErrOccupied
	}
	if r.pending != nil {
		if opts.Token == "" {
			r.mu.Unlock()
			return nil, ErrReserved
		}
		if subtle.ConstantTimeCompare([]byte(r.pending.token), []byte(opts.Token)) != 1 || (r.pending.bound && r.pending.info.Owner != owner) {
			r.mu.Unlock()
			return nil, ErrInvalidToken
		}
		r.clearReservationLocked()
	} else if opts.Token != "" {
		r.mu.Unlock()
		return nil, ErrInvalidToken
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
	revision := r.revision
	r.mu.Unlock()
	r.log(ctx, "acquired", c.info.ID, revision, nil, owner, "")
	return c, nil
}

// Observe atomically returns a snapshot and an invalidation channel. On channel
// closure, call Observe again to get the latest state. Updates may coalesce;
// this is a state subscription, not an audit stream. There are no per-subscriber
// goroutines and no unsubscribe call. Stop observing when Snapshot.Closed is
// true, or when your own context ends.
func (r *Resource) Observe() (Snapshot, <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expireReservationLocked(time.Now())
	s := Snapshot{Resource: r.name, Revision: r.revision, Closed: r.closed}
	if r.current != nil {
		info := r.current.infoLocked()
		s.Claim = &info
	}
	if r.pending != nil {
		info := r.pending.info
		s.Reservation = &info
	}
	return s, r.changed
}

// Revoke signals the current claim and waits for that specific holder's Release.
// It does not reserve the next acquisition. Use Transfer for a protected handoff.
// Authorization is the application's responsibility. A timeout leaves revocation
// in effect and wraps both ErrNotReleased and the context error.
func (r *Resource) Revoke(ctx context.Context, reason string) error {
	r.mu.Lock()
	if err := ctx.Err(); err != nil {
		r.mu.Unlock()
		return err
	}
	if r.closed {
		r.mu.Unlock()
		return ErrClosed
	}
	r.expireReservationLocked(time.Now())
	if r.pending != nil {
		r.mu.Unlock()
		return ErrReserved
	}
	c := r.current
	if c == nil {
		r.mu.Unlock()
		return nil
	}
	changed := c.invalidateLocked(ErrRevoked, reason)
	revision := r.revision
	r.mu.Unlock()
	if changed {
		r.log(ctx, "revoked", c.info.ID, revision, ErrRevoked, c.info.Owner, reason)
	}
	return waitReleased(ctx, c.released)
}

// Close permanently rejects new acquisitions, cancels reservations, signals any
// holder, and waits for cleanup. It is idempotent and may be called again after
// a timeout. It never forces release of live work.
func (r *Resource) Close(ctx context.Context) error {
	r.mu.Lock()
	if err := ctx.Err(); err != nil {
		r.mu.Unlock()
		return err
	}
	first := !r.closed
	r.closed = true
	r.clearReservationLocked()
	c := r.current
	if c != nil {
		c.invalidateLocked(ErrClosed, "resource closed")
	}
	if first {
		r.notifyLocked()
	}
	revision := r.revision
	r.mu.Unlock()
	if first {
		r.log(ctx, "closed", "", revision, nil, Owner{}, "resource closed")
	}
	if c != nil {
		return waitReleased(ctx, c.released)
	}
	return nil
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
	// Completion wins a simultaneous cancellation.
	select {
	case <-released:
		return nil
	default:
		return errors.Join(ErrNotReleased, ctx.Err())
	}
}
