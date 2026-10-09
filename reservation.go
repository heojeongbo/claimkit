package claimkit

import (
	"context"
	"crypto/rand"
	"log/slog"
	"maps"
	"time"
)

// Reservation is a single-use handoff to a specific Owner. Token explicitly
// exposes its bearer capability for transport; keep it confidential. Default
// formatting, slog, and JSON encoding do not include the token. Cancelling an
// abandoned reservation is optional: it expires after its TTL once available.
type Reservation struct {
	r            *Resource
	token        string
	info         ReservationInfo
	ttl          time.Duration
	timer        *time.Timer
	logctx       context.Context
	previous     *Claim
	previousInfo *ClaimInfo
	bound        bool
}

func (p Reservation) String() string       { return "claimkit.Reservation([REDACTED])" }
func (p Reservation) GoString() string     { return p.String() }
func (p Reservation) LogValue() slog.Value { return slog.StringValue(p.String()) }

// Token returns the secret capability. Never include it in logs or snapshots.
func (p *Reservation) Token() string { return p.token }

// Info returns a detached view of the reservation.
func (p *Reservation) Info() ReservationInfo {
	p.r.mu.Lock()
	defer p.r.mu.Unlock()
	return p.info
}

// Cancel withdraws this reservation only. It never revives a revoked holder or
// affects a replacement reservation. It returns true if it removed a live one.
func (p *Reservation) Cancel(ctx context.Context) bool {
	r := p.r
	r.mu.Lock()
	r.expireReservationLocked(time.Now())
	if r.pending != p {
		r.mu.Unlock()
		return false
	}
	r.clearReservationLocked()
	r.notifyLocked()
	revision := r.revision
	r.mu.Unlock()
	r.log(ctx, "reservation_cancelled", "", revision, nil, p.info.Owner, "")
	return true
}

// Transfer reserves the next acquisition for next BEFORE revoking the current
// holder, and returns only after that holder releases. The caller passes Token
// to Acquire with the same Owner. ttl starts at release, not at the revoke
// request. A resource that is already free is reserved immediately.
//
// Concurrent transfers fail with ErrReserved instead of silently replacing the
// winner's reservation. Cancellation withdraws only this call's reservation;
// revocation remains in effect. Authorization belongs to the application.
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

// BeginTransfer installs a reservation and signals the current holder without
// waiting for cleanup. Call Wait before advertising the resource as available.
// This split lets transport adapters install or invoke their teardown hooks
// after the reservation is in place. Authorization belongs to the application.
func (r *Resource) BeginTransfer(ctx context.Context, opts TransferOptions) (*Reservation, error) {
	if opts.TTL <= 0 {
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
	if r.pending != nil && !opts.Replace {
		r.mu.Unlock()
		return nil, ErrReserved
	}
	r.clearReservationLocked()
	p := &Reservation{r: r, token: rand.Text(), ttl: opts.TTL, logctx: context.WithoutCancel(ctx), previous: r.current}
	if opts.Next != nil {
		p.bound = true
		p.info.Owner = *opts.Next
	}
	p.info.OwnerBound = p.bound
	r.pending = p
	c := r.current
	if c == nil {
		p.armLocked()
	} else {
		info := c.infoLocked()
		p.previousInfo = &info
		c.invalidateLocked(ErrRevoked, opts.Reason)
	}
	r.notifyLocked()
	revision := r.revision
	r.mu.Unlock()
	r.log(ctx, "transfer_requested", "", revision, nil, p.info.Owner, opts.Reason)
	return p, nil
}

// Previous returns the holder captured atomically when the reservation was
// installed, or nil if the resource was free. The snapshot is a detached copy.
func (p *Reservation) Previous() *ClaimInfo {
	if p.previousInfo == nil {
		return nil
	}
	info := *p.previousInfo
	info.Metadata = maps.Clone(info.Metadata)
	return &info
}

// Wait waits for the previous holder to Release and checks that this reservation
// is still live. On timeout it withdraws only this reservation; revocation remains
// in effect. A replaced, consumed, cancelled, or expired reservation returns
// ErrReservationLost after cleanup. Concurrent calls are safe, but any caller's
// timeout can cancel the shared reservation. Prefer one lifecycle owner.
func (p *Reservation) Wait(ctx context.Context) error {
	var err error
	if p.previous != nil {
		err = waitReleased(ctx, p.previous.released)
	} else {
		err = ctx.Err()
	}
	r := p.r
	r.mu.Lock()
	r.expireReservationLocked(time.Now())
	if err != nil {
		if r.pending == p {
			r.clearReservationLocked()
			r.notifyLocked()
		}
	} else if r.pending != p {
		err = ErrReservationLost
	}
	revision := r.revision
	r.mu.Unlock()
	if err != nil {
		r.log(ctx, "transfer_failed", "", revision, err, p.info.Owner, "")
		return err
	}
	r.log(ctx, "transfer_ready", "", revision, nil, p.info.Owner, "")
	return nil
}

func (p *Reservation) armLocked() {
	p.info.ExpiresAt = time.Now().Add(p.ttl)
	p.timer = time.AfterFunc(p.ttl, func() {
		r := p.r
		r.mu.Lock()
		changed := r.pending == p && r.expireReservationLocked(time.Now())
		revision := r.revision
		r.mu.Unlock()
		if changed {
			r.log(p.logctx, "reservation_expired", "", revision, nil, p.info.Owner, "")
		}
	})
}

func (r *Resource) expireReservationLocked(now time.Time) bool {
	p := r.pending
	if p == nil || p.info.ExpiresAt.IsZero() || now.Before(p.info.ExpiresAt) {
		return false
	}
	r.clearReservationLocked()
	r.notifyLocked()
	return true
}

func (r *Resource) clearReservationLocked() {
	if r.pending != nil {
		if r.pending.timer != nil {
			r.pending.timer.Stop()
		}
		r.pending = nil
	}
}
