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
	lifetime context.Context // allocated only when Context is used
	end      context.CancelCauseFunc
}

// Done closes on revocation, expiry, shutdown, or voluntary Release. The holder
// must stop accepting work, cancel and join outstanding work, then Release.
func (c *Claim) Done() <-chan struct{} { return c.done }

// Released closes only after Release, unlike Done which requests cleanup.
func (c *Claim) Released() <-chan struct{} { return c.released }

// Info returns a detached snapshot of this acquisition, even after release.
func (c *Claim) Info() ClaimInfo {
	var events []emission
	c.r.mu.Lock()
	defer c.r.finish(&events)
	c.expireLocked(&events)
	return c.infoLocked()
}

func (c *Claim) infoLocked() ClaimInfo {
	info := c.info
	info.Metadata = maps.Clone(info.Metadata)
	return info
}

// Check reports whether this acquisition can accept work, reconciling overdue
// timers. The first terminal cause is preserved. This does not serialize later
// work with revocation; cancel/join in-flight work before Release.
func (c *Claim) Check() error {
	var events []emission
	c.r.mu.Lock()
	defer c.r.finish(&events)
	c.expireLocked(&events)
	return c.cause
}

// Context derives work from parent and cancels it when this claim ends. Values
// and deadlines come from parent; context.Cause reports whichever cancellation
// wins. Always call the returned cancel function to unregister the hook. Neither
// parent cancellation nor this function releases ownership: join work and Release.
func (c *Claim) Context(parent context.Context) (context.Context, context.CancelFunc) {
	var events []emission
	c.r.mu.Lock()
	defer c.r.finish(&events)
	c.expireLocked(&events)
	if c.lifetime == nil {
		c.lifetime, c.end = context.WithCancelCause(context.Background())
		if c.cause != nil {
			c.end(c.cause)
		}
	}
	child, cancel := context.WithCancelCause(parent)
	stop := context.AfterFunc(c.lifetime, func() { cancel(context.Cause(c.lifetime)) })
	if c.cause != nil {
		cancel(c.cause)
	}
	return child, func() { stop(); cancel(context.Canceled) }
}

// Release confirms all work belonging to this acquisition has stopped. It
// returns true exactly once. ctx carries logging values; cancellation does not
// prevent cleanup. A stale release is harmless and returns false.
func (c *Claim) Release(ctx context.Context) bool {
	r := c.r
	var events []emission
	r.mu.Lock()
	defer r.finish(&events)
	if r.current != c {
		return false
	}
	c.expireLocked(&events)
	now := time.Now()
	if c.cause == nil {
		c.cause, c.info.Cause, c.info.EndedAt = ErrReleased, CauseReleased, now
		c.signalLocked()
	}
	if c.timer != nil {
		c.timer.Stop()
	}
	c.info.ReleasedAt = now
	close(c.released)
	r.current = nil
	r.notifyLocked()
	r.recordLocked(ctx, EventReleased, c, nil, Owner{}, "", nil, &events)
	if r.pending != nil {
		r.pending.armLocked(&events)
	}
	return true
}

// Renew replaces the active claim's deadline with now+ttl. It cannot revive a
// terminal claim. ttl must be positive; applications choose heartbeat policy.
func (c *Claim) Renew(ctx context.Context, ttl time.Duration) (err error) {
	r := c.r
	var events []emission
	r.mu.Lock()
	defer r.finish(&events)
	defer func() {
		if err != nil {
			r.recordLocked(ctx, EventRenewRejected, c, nil, Owner{}, "", err, &events)
		}
	}()
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.expireLocked(&events)
	if c.cause != nil {
		return c.cause
	}
	c.armLocked(ttl)
	r.notifyLocked()
	r.recordLocked(ctx, EventRenewed, c, nil, Owner{}, "", nil, &events)
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
	var events []emission
	c.r.mu.Lock()
	defer c.r.finish(&events)
	// A stopped timer may already be running; the current deadline and cause win.
	c.expireLocked(&events)
}

func (c *Claim) expireLocked(events *[]emission) {
	if c.cause == nil && !c.info.ExpiresAt.IsZero() && !time.Now().Before(c.info.ExpiresAt) {
		c.invalidateLocked(c.logctx, ErrExpired, CauseExpired, "lease expired", EventExpired, c.info.ExpiresAt, events)
	}
}

func (c *Claim) invalidateLocked(ctx context.Context, cause error, code Cause, reason string, kind EventKind, at time.Time, events *[]emission) {
	if c.cause != nil {
		return
	}
	c.cause, c.info.Cause, c.info.EndedAt = cause, code, at
	c.info.Revoked, c.info.Reason = true, reason
	if c.timer != nil {
		c.timer.Stop()
	}
	c.signalLocked()
	c.r.notifyLocked()
	c.r.recordLocked(ctx, kind, c, nil, Owner{}, "", cause, events)
}

func (c *Claim) signalLocked() {
	close(c.done)
	if c.end != nil {
		c.end(c.cause)
	}
}
