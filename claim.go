package claimkit

import (
	"context"
	"maps"
	"time"
)

// Claim is one acquisition, with grant-scoped, idempotent release. A stale
// handle can never release a newer holder, including another session of the
// same Owner. Do not copy a Claim; retain the pointer returned by Acquire.
type Claim struct {
	r        *Resource
	info     ClaimInfo // guarded by r.mu; ID is immutable
	done     chan struct{}
	released chan struct{}
	cause    error
	timer    *time.Timer
	logctx   context.Context
}

// Done closes on revocation, expiry, resource shutdown, or voluntary Release.
// The holder should stop accepting work, cancel and join outstanding work, and
// then Release. Listening after revocation is safe; the signal cannot be missed.
func (c *Claim) Done() <-chan struct{} { return c.done }

// Released closes only after Release, unlike Done which requests cleanup.
func (c *Claim) Released() <-chan struct{} { return c.released }

// Info returns a detached snapshot of this acquisition, even after release.
func (c *Claim) Info() ClaimInfo {
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	return c.infoLocked()
}

func (c *Claim) infoLocked() ClaimInfo {
	info := c.info
	info.Metadata = maps.Clone(info.Metadata)
	return info
}

// Check reports whether this acquisition can still accept work. Expiration is
// checked against the deadline even if the timer has not run yet. The first
// terminal cause is preserved. Checking does not serialize subsequent work with
// revocation; the holder must cancel/join in-flight work before Release.
func (c *Claim) Check() error {
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	if c.cause == nil && !c.info.ExpiresAt.IsZero() && !time.Now().Before(c.info.ExpiresAt) {
		return ErrExpired
	}
	return c.cause
}

// Release confirms all work belonging to this acquisition has stopped. It
// returns true exactly once. ctx carries logging values; cancellation does not
// prevent cleanup. A stale release is harmless and returns false.
func (c *Claim) Release(ctx context.Context) bool {
	r := c.r
	r.mu.Lock()
	if r.current != c {
		r.mu.Unlock()
		return false
	}
	if c.cause == nil && !c.info.ExpiresAt.IsZero() && !time.Now().Before(c.info.ExpiresAt) {
		c.invalidateLocked(ErrExpired, "lease expired")
	}
	if c.cause == nil {
		c.cause = ErrReleased
		close(c.done)
	}
	if c.timer != nil {
		c.timer.Stop()
	}
	c.info.ReleasedAt = time.Now()
	close(c.released)
	r.current = nil
	if r.pending != nil {
		r.pending.armLocked()
	}
	r.notifyLocked()
	revision := r.revision
	r.mu.Unlock()
	r.log(ctx, "released", c.info.ID, revision, nil, c.info.Owner, c.info.Reason)
	return true
}

// Renew replaces the active claim's deadline with now+ttl. It cannot revive a
// revoked, expired, or released claim. ttl must be positive. Renewal is explicit;
// applications choose heartbeat frequency, retry policy, and session lifetime.
func (c *Claim) Renew(ctx context.Context, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	r := c.r
	r.mu.Lock()
	if err := ctx.Err(); err != nil {
		r.mu.Unlock()
		return err
	}
	if c.cause != nil {
		err := c.cause
		r.mu.Unlock()
		return err
	}
	if !c.info.ExpiresAt.IsZero() && !time.Now().Before(c.info.ExpiresAt) {
		r.mu.Unlock()
		return ErrExpired
	}
	c.armLocked(ttl)
	r.notifyLocked()
	revision := r.revision
	r.mu.Unlock()
	r.log(ctx, "renewed", c.info.ID, revision, nil, c.info.Owner, "")
	return nil
}

func (c *Claim) armLocked(ttl time.Duration) {
	if c.timer != nil {
		c.timer.Stop()
	}
	c.info.ExpiresAt = time.Now().Add(ttl)
	c.timer = time.AfterFunc(ttl, c.expire)
}

func (c *Claim) expire() {
	r := c.r
	r.mu.Lock()
	// A stopped timer may already be running. It must not invalidate a renewal
	// or a later acquisition, nor overwrite the first terminal reason.
	changed := c.cause == nil && !time.Now().Before(c.info.ExpiresAt) && c.invalidateLocked(ErrExpired, "lease expired")
	revision := r.revision
	r.mu.Unlock()
	if changed {
		r.log(c.logctx, "expired", c.info.ID, revision, ErrExpired, c.info.Owner, "lease expired")
	}
}

func (c *Claim) invalidateLocked(cause error, reason string) bool {
	if c.cause != nil {
		return false
	}
	c.cause = cause
	c.info.Revoked = true
	c.info.Reason = reason
	if c.timer != nil {
		c.timer.Stop()
	}
	close(c.done)
	c.r.notifyLocked()
	return true
}
