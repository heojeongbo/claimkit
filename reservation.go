package claimkit

import (
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"maps"
	"time"
)

// Reservation is a single-use handoff with an optional Owner binding. Token
// exposes its bearer capability for transport; keep it confidential. Formatting,
// slog and JSON omit the token. Retain the returned pointer for lifecycle calls.
type Reservation struct {
	// Immutable handle permits concurrent value-receiver redaction.
	*reservationState
}

type reservationState struct {
	r            *Resource
	token        string
	info         ReservationInfo
	ttl          time.Duration
	timer        *time.Timer
	logctx       context.Context
	previousInfo *ClaimInfo // immutable, detached at creation
	ready        chan struct{}
	done         chan struct{}
	cause        error
}

func (p Reservation) String() string       { return "claimkit.Reservation([REDACTED])" }
func (p Reservation) GoString() string     { return p.String() }
func (p Reservation) LogValue() slog.Value { return slog.StringValue(p.String()) }

// Token returns the secret capability. Never include it in logs or snapshots.
func (p *Reservation) Token() string { return p.token }

// Done closes on cancellation, replacement, consumption, expiration or shutdown.
// It does not mean the previous holder finished cleanup; use Wait for readiness.
func (p *Reservation) Done() <-chan struct{} { return p.done }

// Err returns nil while waiting or ready. Otherwise it matches ErrReservationLost
// and the specific terminal cause via errors.Is, including successful consumption.
func (p *Reservation) Err() error {
	var events []emission
	p.r.mu.Lock()
	defer p.r.finish(&events)
	p.r.expireReservationLocked(time.Now(), &events)
	return p.cause
}

// Info returns a detached view, including terminal status after removal.
func (p *Reservation) Info() ReservationInfo {
	var events []emission
	p.r.mu.Lock()
	defer p.r.finish(&events)
	p.r.expireReservationLocked(time.Now(), &events)
	return p.info
}

// Cancel withdraws only this reservation; it never releases the previous holder
// or affects a replacement. It returns true exactly once for a live reservation.
func (p *Reservation) Cancel(ctx context.Context) bool {
	r := p.r
	var events []emission
	r.mu.Lock()
	defer r.finish(&events)
	r.expireReservationLocked(time.Now(), &events)
	if r.pending != p {
		return false
	}
	p.endLocked(ctx, ReservationCancelled, EventReservationCancelled, ErrReservationCancelled, &events)
	return true
}

// Transfer reserves acquisition for next before revoking the holder and waits
// for cleanup. TTL starts at release. Cancellation withdraws only this call's
// reservation; revocation remains in effect. Authorization is application-owned.
func (r *Resource) Transfer(ctx context.Context, next Owner, ttl time.Duration, reason string) (*Reservation, error) {
	p, err := r.BeginTransfer(ctx, TransferOptions{Next: &next, TTL: ttl, Reason: reason})
	if err != nil {
		return nil, err
	}
	if err := p.Wait(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

// BeginTransfer installs a reservation and signals the holder without waiting.
// ExpectedClaimID is compared atomically; Replace explicitly supersedes a pending
// reservation. Neither option releases the holder. Call Wait before advertising
// readiness. Authorization belongs to the application.
func (r *Resource) BeginTransfer(ctx context.Context, opts TransferOptions) (reservation *Reservation, err error) {
	var events []emission
	r.mu.Lock()
	defer r.finish(&events)
	defer func() {
		if err != nil {
			r.recordLocked(ctx, EventTransferRejected, r.current, r.pending, Owner{}, opts.ExpectedClaimID, err, &events)
		}
	}()
	if opts.TTL <= 0 {
		return nil, ErrInvalidTTL
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.closed {
		return nil, ErrClosed
	}
	r.expireLocked(&events)
	if !r.matchesLocked(opts.ExpectedClaimID) {
		return nil, r.conflictLocked("transfer", opts.ExpectedClaimID, ErrConflict)
	}
	if r.pending != nil {
		if !opts.Replace {
			return nil, r.conflictLocked("transfer", opts.ExpectedClaimID, ErrReserved)
		}
		r.pending.endLocked(ctx, ReservationReplaced, EventReservationReplaced, ErrReservationReplaced, &events)
	}
	p := &Reservation{reservationState: &reservationState{
		r: r, token: rand.Text(), ttl: opts.TTL, logctx: context.WithoutCancel(ctx),
		ready: make(chan struct{}), done: make(chan struct{}),
		info: ReservationInfo{
			ID: rand.Text(), State: ReservationWaiting, CreatedAt: time.Now(), Reason: opts.Reason,
		},
	}}
	if opts.Next != nil {
		p.info.OwnerBound = true
		p.info.Owner = *opts.Next
	}
	c := r.current
	if c != nil {
		info := c.infoLocked()
		p.previousInfo = &info
	}
	r.pending = p
	r.notifyLocked()
	r.recordLocked(ctx, EventReservationCreated, c, p, Owner{}, opts.ExpectedClaimID, nil, &events)
	if c == nil {
		p.armLocked(&events)
	} else {
		c.invalidateLocked(ctx, ErrRevoked, CauseRevoked, opts.Reason, EventRevoked, time.Now(), &events)
	}
	return p, nil
}

// Previous returns the holder captured at installation, or nil if free.
func (p *Reservation) Previous() *ClaimInfo {
	if p.previousInfo == nil {
		return nil
	}
	info := *p.previousInfo
	info.Metadata = maps.Clone(info.Metadata)
	return &info
}

// Wait succeeds when the prior holder releases and this reservation is live.
// Terminal reservations return promptly, even while the prior holder is cleaning
// up. A waiting caller's context cancellation withdraws this shared reservation
// and returns ErrNotReleased plus the context error. Readiness wins concurrent
// cancellation. Prefer one lifecycle owner; other observers can use Done/Info.
func (p *Reservation) Wait(ctx context.Context) error {
	select {
	case <-p.ready:
	case <-p.done:
	case <-ctx.Done():
	}
	r := p.r
	var events []emission
	r.mu.Lock()
	defer r.finish(&events)
	r.expireReservationLocked(time.Now(), &events)
	if p.cause != nil {
		return p.cause
	}
	if p.info.State == ReservationReady {
		return nil
	}
	p.endLocked(ctx, ReservationCancelled, EventReservationCancelled, ErrReservationCancelled, &events)
	return errors.Join(ErrNotReleased, ctx.Err())
}

func (p *Reservation) armLocked(events *[]emission) {
	p.info.State, p.info.ReadyAt = ReservationReady, time.Now()
	p.info.ExpiresAt = p.info.ReadyAt.Add(p.ttl)
	close(p.ready)
	p.timer = time.AfterFunc(p.ttl, func() {
		r := p.r
		var events []emission
		r.mu.Lock()
		defer r.finish(&events)
		r.expireReservationLocked(time.Now(), &events)
	})
	p.r.notifyLocked()
	p.r.recordLocked(p.logctx, EventReservationReady, nil, p, Owner{}, "", nil, events)
}

func (p *Reservation) endLocked(ctx context.Context, status ReservationStatus, kind EventKind, cause error, events *[]emission) {
	if p.timer != nil {
		p.timer.Stop()
	}
	p.info.State, p.info.EndedAt = status, time.Now()
	if status == ReservationExpired {
		p.info.EndedAt = p.info.ExpiresAt
	}
	p.cause = errors.Join(ErrReservationLost, cause)
	close(p.done)
	p.r.pending = nil
	p.r.notifyLocked()
	p.r.recordLocked(ctx, kind, nil, p, Owner{}, "", nil, events)
}

func (r *Resource) expireReservationLocked(now time.Time, events *[]emission) {
	p := r.pending
	if p == nil || p.info.ExpiresAt.IsZero() || now.Before(p.info.ExpiresAt) {
		return
	}
	p.endLocked(p.logctx, ReservationExpired, EventReservationExpired, ErrExpired, events)
}
