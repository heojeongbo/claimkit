package claimkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

var alice = Owner{ID: "alice", Session: "a"}
var bob = Owner{ID: "bob", Session: "b"}

func take(t testing.TB, r *Resource, owner Owner, opts AcquireOptions) *Claim {
	t.Helper()
	c, err := r.Acquire(context.Background(), owner, opts)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func wantErr(t testing.TB, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("error = %v, want %v", got, want)
	}
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestAcquisitionAndSnapshots(t *testing.T) {
	r := New("editor/document", Options{})
	initial, changed := r.Observe()
	if initial.Resource != "editor/document" || initial.Claim != nil || initial.Revision != 0 || initial.Closed {
		t.Fatal(initial)
	}
	metadata := map[string]string{"operation": "editing"}
	c := take(t, r, alice, AcquireOptions{Metadata: metadata})
	if !isClosed(changed) || isClosed(c.Done()) || isClosed(c.Released()) {
		t.Fatal("incorrect lifecycle signal")
	}
	wantErr(t, c.Check(), nil)
	metadata["operation"] = "changed outside"
	s, nextChange := r.Observe()
	if s.Revision != 1 || s.Claim.Owner != alice || s.Claim.ID == "" || s.Claim.AcquiredAt.IsZero() || s.Claim.Metadata["operation"] != "editing" {
		t.Fatal(s)
	}
	s.Claim.Metadata["operation"] = "changed snapshot"
	info := c.Info()
	if info.Metadata["operation"] != "editing" {
		t.Fatal("snapshot mutated library state")
	}
	info.Metadata["operation"] = "changed info"
	if c.Info().Metadata["operation"] != "editing" {
		t.Fatal("Info aliased metadata")
	}
	if !c.Release(t.Context()) || c.Release(t.Context()) {
		t.Fatal("release must succeed exactly once")
	}
	if !isClosed(c.Done()) || !isClosed(c.Released()) || !isClosed(nextChange) {
		t.Fatal("release did not notify")
	}
	wantErr(t, c.Check(), ErrReleased)
	s, _ = r.Observe()
	if s.Claim != nil {
		t.Fatal("released claim remains occupied")
	}
	next := take(t, r, alice, AcquireOptions{})
	if next.Info().ID == c.Info().ID || c.Release(t.Context()) {
		t.Fatal("old acquisition affected new one")
	}
	wantErr(t, next.Check(), nil)
	next.Release(t.Context())
}

func TestAcquireValidationAndIdentity(t *testing.T) {
	r := New("", Options{})
	_, err := r.Acquire(t.Context(), alice, AcquireOptions{TTL: -1})
	wantErr(t, err, ErrInvalidTTL)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = r.Acquire(ctx, alice, AcquireOptions{})
	wantErr(t, err, context.Canceled)
	_, err = r.Acquire(t.Context(), alice, AcquireOptions{Token: "stale"})
	wantErr(t, err, ErrInvalidToken)
	c := take(t, r, Owner{}, AcquireOptions{})
	_, err = r.Acquire(t.Context(), Owner{}, AcquireOptions{})
	wantErr(t, err, ErrOccupied)
	s, _ := r.Observe()
	if s.Claim == nil || s.Claim.Owner != (Owner{}) {
		t.Fatal("anonymous occupancy lost")
	}
	c.Release(t.Context())
	ctx, cancel = context.WithCancel(t.Context())
	c, err = r.Acquire(ctx, alice, AcquireOptions{})
	wantErr(t, err, nil)
	cancel()
	wantErr(t, c.Check(), nil) // request context is not ownership lifetime
	if !c.Release(ctx) {
		t.Fatal("cancelled request prevented cleanup")
	}
	wantErr(t, r.Close(t.Context()), nil)
	_, err = r.Acquire(t.Context(), alice, AcquireOptions{})
	wantErr(t, err, ErrClosed)
}

func TestRevokeWaitsForCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New("resource", Options{})
		c := take(t, r, alice, AcquireOptions{})
		result := make(chan error, 1)
		go func() { result <- r.Revoke(t.Context(), "operator handoff") }()
		<-c.Done()
		synctest.Wait()
		wantErr(t, c.Check(), ErrRevoked)
		if c.Info().Reason != "operator handoff" || !c.Info().Revoked {
			t.Fatal(c.Info())
		}
		select {
		case err := <-result:
			t.Fatalf("returned before cleanup: %v", err)
		default:
		}
		_, err := r.Acquire(t.Context(), bob, AcquireOptions{})
		wantErr(t, err, ErrOccupied)
		c.Release(t.Context())
		wantErr(t, <-result, nil)
		next := take(t, r, bob, AcquireOptions{})
		if c.Release(t.Context()) {
			t.Fatal("stale release succeeded")
		}
		wantErr(t, c.Check(), ErrRevoked)
		next.Release(t.Context())
		wantErr(t, r.Revoke(t.Context(), "nothing held"), nil)
	})
}

func TestRevokeTimeoutAndRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New("resource", Options{})
		c := take(t, r, alice, AcquireOptions{})
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		err := r.Revoke(ctx, "first reason")
		wantErr(t, err, ErrNotReleased)
		wantErr(t, err, context.DeadlineExceeded)
		ctx, cancel2 := context.WithTimeout(t.Context(), time.Second)
		defer cancel2()
		wantErr(t, r.Revoke(ctx, "second reason"), ErrNotReleased)
		if c.Info().Reason != "first reason" {
			t.Fatal("first revocation overwritten")
		}
		c.Release(t.Context())
		cancelled, stop := context.WithCancel(t.Context())
		stop()
		wantErr(t, r.Revoke(cancelled, "no effect"), context.Canceled)
		wantErr(t, r.Close(t.Context()), nil)
		wantErr(t, r.Revoke(t.Context(), "closed"), ErrClosed)
	})
}

func TestTransferProtectsNextOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New("resource", Options{})
		c := take(t, r, alice, AcquireOptions{})
		result := make(chan *Reservation, 1)
		go func() {
			p, err := r.Transfer(t.Context(), bob, 5*time.Second, "handoff")
			if err != nil {
				t.Error(err)
			}
			result <- p
		}()
		<-c.Done()
		s, _ := r.Observe()
		if s.Reservation.Owner != bob || !s.Reservation.ExpiresAt.IsZero() {
			t.Fatal(s)
		}
		time.Sleep(10 * time.Second) // cleanup cannot consume the reservation TTL
		_, err := r.Transfer(t.Context(), alice, time.Second, "competing transfer")
		wantErr(t, err, ErrReserved)
		wantErr(t, r.Revoke(t.Context(), "cannot steal reservation"), ErrReserved)
		c.Release(t.Context())
		p := <-result
		if p == nil {
			t.Fatal("missing reservation")
		}
		if p.Info().ExpiresAt != time.Now().Add(5*time.Second) {
			t.Fatal(p.Info())
		}
		info := p.Info()
		info.Owner = alice
		if p.Info().Owner != bob {
			t.Fatal("reservation snapshot aliased")
		}
		_, err = r.Acquire(t.Context(), bob, AcquireOptions{})
		wantErr(t, err, ErrReserved)
		for _, owner := range []Owner{alice, {ID: bob.ID, Session: "another-tab"}} {
			_, err = r.Acquire(t.Context(), owner, AcquireOptions{Token: p.Token()})
			wantErr(t, err, ErrInvalidToken)
		}
		_, err = r.Acquire(t.Context(), bob, AcquireOptions{Token: "wrong"})
		wantErr(t, err, ErrInvalidToken)
		next := take(t, r, bob, AcquireOptions{Token: p.Token()})
		if p.Cancel(t.Context()) {
			t.Fatal("consumed reservation cancelled new owner")
		}
		if c.Release(t.Context()) {
			t.Fatal("stale holder released successor")
		}
		next.Release(t.Context())
		_, err = r.Acquire(t.Context(), bob, AcquireOptions{Token: p.Token()})
		wantErr(t, err, ErrInvalidToken) // replay while free must fail
	})
}

func TestReservationExpiryCancellationAndRedaction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New("resource", Options{})
		p, err := r.Transfer(t.Context(), bob, time.Second, "")
		wantErr(t, err, nil)
		s, changed := r.Observe()
		encoded, err := json.Marshal([]any{s, p})
		wantErr(t, err, nil)
		for _, text := range []string{string(encoded), fmt.Sprint(p), fmt.Sprintf("%+v", p), fmt.Sprintf("%#v", p), fmt.Sprintf("%#v", *p), p.LogValue().String()} {
			if strings.Contains(text, p.Token()) {
				t.Fatal("secret reservation token was exposed")
			}
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if !isClosed(changed) {
			t.Fatal("reservation expiry not observed")
		}
		s, _ = r.Observe()
		if s.Reservation != nil || p.Cancel(t.Context()) {
			t.Fatal("expired reservation remains active")
		}
		_, err = r.Acquire(t.Context(), bob, AcquireOptions{Token: p.Token()})
		wantErr(t, err, ErrInvalidToken)
		next, err := r.Transfer(t.Context(), alice, time.Second, "")
		wantErr(t, err, nil)
		if next.Token() == p.Token() || p.Cancel(t.Context()) {
			t.Fatal("old reservation affected replacement")
		}
		if !next.Cancel(t.Context()) || next.Cancel(t.Context()) {
			t.Fatal("cancel must be idempotent")
		}
		c := take(t, r, alice, AcquireOptions{})
		c.Release(t.Context())
	})
}

func TestTransferErrorsAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New("resource", Options{})
		_, err := r.Transfer(t.Context(), bob, 0, "")
		wantErr(t, err, ErrInvalidTTL)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = r.Transfer(ctx, bob, time.Second, "")
		wantErr(t, err, context.Canceled)
		c := take(t, r, alice, AcquireOptions{})
		ctx, cancel = context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		p, err := r.Transfer(ctx, bob, time.Second, "handoff timed out")
		if p != nil {
			t.Fatal("failed transfer returned a credential")
		}
		wantErr(t, err, ErrNotReleased)
		wantErr(t, err, context.DeadlineExceeded)
		s, _ := r.Observe()
		if s.Reservation != nil || s.Claim == nil || !s.Claim.Revoked {
			t.Fatal(s)
		}
		c.Release(t.Context())
		next := take(t, r, alice, AcquireOptions{})
		next.Release(t.Context())
		wantErr(t, r.Close(t.Context()), nil)
		_, err = r.Transfer(t.Context(), bob, time.Second, "")
		wantErr(t, err, ErrClosed)
	})
}

func TestCloseDuringTransfer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New("resource", Options{})
		c := take(t, r, alice, AcquireOptions{})
		result := make(chan error, 1)
		go func() { _, err := r.Transfer(t.Context(), bob, time.Minute, "handoff"); result <- err }()
		<-c.Done()
		closed := make(chan error, 1)
		go func() { closed <- r.Close(t.Context()) }()
		synctest.Wait()
		c.Release(t.Context())
		wantErr(t, <-result, ErrReservationLost)
		wantErr(t, <-closed, nil)
		wantErr(t, r.Close(t.Context()), nil)
	})
}

func TestCloseLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New("resource", Options{})
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		wantErr(t, r.Close(ctx), context.Canceled)
		c := take(t, r, alice, AcquireOptions{TTL: time.Minute})
		ctx, cancel = context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		wantErr(t, r.Close(ctx), ErrNotReleased)
		wantErr(t, c.Check(), ErrClosed)
		s, _ := r.Observe()
		if !s.Closed || s.Claim == nil || s.Claim.Reason != "resource closed" {
			t.Fatal(s)
		}
		c.Release(t.Context())
		wantErr(t, r.Close(t.Context()), nil)
	})
}

func TestLeaseRenewalAndExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New("resource", Options{})
		c := take(t, r, alice, AcquireOptions{TTL: 2 * time.Second})
		time.Sleep(time.Second)
		wantErr(t, c.Renew(t.Context(), 3*time.Second), nil)
		c.expire() // callback from a stopped timer must respect the new deadline
		time.Sleep(time.Second)
		wantErr(t, c.Check(), nil)
		if isClosed(c.Done()) {
			t.Fatal("old deadline invalidated renewed claim")
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if !isClosed(c.Done()) || isClosed(c.Released()) {
			t.Fatal("expiry must signal without releasing")
		}
		wantErr(t, c.Check(), ErrExpired)
		wantErr(t, c.Renew(t.Context(), time.Second), ErrExpired)
		_, err := r.Acquire(t.Context(), bob, AcquireOptions{})
		wantErr(t, err, ErrOccupied)
		if c.Info().Reason != "lease expired" {
			t.Fatal(c.Info())
		}
		c.Release(t.Context())
		next := take(t, r, bob, AcquireOptions{})
		c.expire() // callback already running when Release stopped its timer
		wantErr(t, next.Check(), nil)
		wantErr(t, next.Renew(t.Context(), time.Second), nil) // untimed -> timed
		next.Release(t.Context())
	})
}

func TestRenewValidationAndDelayedTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New("resource", Options{})
		c := take(t, r, alice, AcquireOptions{TTL: time.Second})
		wantErr(t, c.Renew(t.Context(), 0), ErrInvalidTTL)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		wantErr(t, c.Renew(ctx, time.Second), context.Canceled)
		// Model a timer goroutine that has not been scheduled at its deadline.
		r.mu.Lock()
		c.timer.Stop()
		r.mu.Unlock()
		time.Sleep(time.Second)
		wantErr(t, c.Check(), ErrExpired)
		wantErr(t, c.Renew(t.Context(), time.Second), ErrExpired)
		c.expire()
		c.Release(t.Context())
	})
}

func TestDelayedReservationTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New("resource", Options{})
		p, err := r.Transfer(t.Context(), bob, time.Second, "")
		wantErr(t, err, nil)
		r.mu.Lock()
		p.timer.Stop()
		r.mu.Unlock()
		time.Sleep(time.Second)
		c := take(t, r, alice, AcquireOptions{}) // deadline checked without timer
		c.Release(t.Context())
	})
}

func TestConcurrentAcquireAndStaleRelease(t *testing.T) {
	r := New("resource", Options{})
	var workers sync.WaitGroup
	var inside atomic.Int32
	var successes atomic.Int32
	for range 12 {
		workers.Go(func() {
			for range 300 {
				c, err := r.Acquire(t.Context(), alice, AcquireOptions{})
				if errors.Is(err, ErrOccupied) {
					continue
				}
				if err != nil {
					t.Error(err)
					return
				}
				if inside.Add(1) != 1 {
					t.Error("two live owners")
				}
				s, _ := r.Observe()
				if s.Claim == nil || s.Claim.ID != c.Info().ID {
					t.Error("wrong current holder")
				}
				inside.Add(-1)
				c.Release(t.Context())
				c.Release(t.Context())
				successes.Add(1)
			}
		})
	}
	workers.Wait()
	if successes.Load() == 0 {
		t.Fatal("no acquisition succeeded")
	}
}

func TestContextLoggerAndReentrancy(t *testing.T) {
	var out strings.Builder
	l := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	type loggerKey struct{}
	ctx := context.WithValue(t.Context(), loggerKey{}, l)
	var r *Resource
	r = New("resource", Options{Logger: func(ctx context.Context) *slog.Logger {
		r.Observe() // a resolver must not run under the resource mutex
		value, _ := ctx.Value(loggerKey{}).(*slog.Logger)
		return value
	}})
	c := take(t, r, alice, AcquireOptions{}) // resolver returns nil
	c.Release(ctx)
	c, err := r.Acquire(ctx, bob, AcquireOptions{})
	wantErr(t, err, nil)
	wantErr(t, c.Renew(ctx, time.Minute), nil)
	c.Release(ctx)
	wantErr(t, r.Close(ctx), nil)
	for _, event := range []string{"acquired", "released", "renewed", "closed"} {
		if !strings.Contains(out.String(), `"event":"`+event+`"`) {
			t.Fatalf("missing %s in %s", event, out.String())
		}
	}
	if !strings.Contains(out.String(), `"component":"claimkit"`) || !strings.Contains(out.String(), `"level":"DEBUG"`) {
		t.Fatal(out.String())
	}
}

func TestTimerLoggingRetainsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type key struct{}
		var count atomic.Int32
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		r := New("resource", Options{Logger: func(ctx context.Context) *slog.Logger {
			if ctx.Value(key{}) != "request" {
				t.Error("lost context values")
			}
			count.Add(1)
			return logger
		}})
		ctx := context.WithValue(t.Context(), key{}, "request")
		c, err := r.Acquire(ctx, alice, AcquireOptions{TTL: time.Second})
		wantErr(t, err, nil)
		time.Sleep(time.Second)
		synctest.Wait()
		c.Release(ctx)
		_, err = r.Transfer(ctx, bob, time.Second, "")
		wantErr(t, err, nil)
		time.Sleep(time.Second)
		synctest.Wait()
		if count.Load() != 6 {
			t.Fatalf("logged %d events, want 6", count.Load())
		}
	})
}

func TestWaitReleasedCancellationRace(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	released := make(chan struct{})
	close(released)
	for range 100 {
		wantErr(t, waitReleased(ctx, released), nil)
	}
}

func TestReleaseAfterDeadlineBeforeTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New("resource", Options{})
		c := take(t, r, alice, AcquireOptions{TTL: time.Second})
		r.mu.Lock()
		c.timer.Stop()
		r.mu.Unlock()
		time.Sleep(time.Second)
		wantErr(t, c.Check(), ErrExpired)
		c.Release(t.Context())
		wantErr(t, c.Check(), ErrExpired)
		if c.Info().ReleasedAt.IsZero() {
			t.Fatal("release timestamp missing")
		}
	})
}

func TestHandoffAgainstRacingAcquisitions(t *testing.T) {
	r := New("resource", Options{})
	c := take(t, r, alice, AcquireOptions{})
	result := make(chan *Reservation, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	go func() {
		p, err := r.Transfer(ctx, bob, time.Minute, "take over")
		if err != nil {
			t.Error(err)
		}
		result <- p
	}()
	<-c.Done() // reservation already installed
	var racers sync.WaitGroup
	start := make(chan struct{})
	for range 8 {
		racers.Go(func() {
			<-start
			for range 100 {
				unexpected, err := r.Acquire(ctx, alice, AcquireOptions{})
				if unexpected != nil {
					t.Error("unreserved caller stole handoff")
					unexpected.Release(ctx)
				}
				if !errors.Is(err, ErrOccupied) && !errors.Is(err, ErrReserved) {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
	close(start)
	c.Release(ctx)
	p := <-result
	racers.Wait()
	if p == nil {
		t.Fatal("missing reservation")
	}
	next := take(t, r, bob, AcquireOptions{Token: p.Token()})
	next.Release(ctx)
}

func TestConcurrentRenewAndRelease(t *testing.T) {
	r := New("resource", Options{})
	for range 50 {
		c := take(t, r, alice, AcquireOptions{TTL: time.Minute})
		var wg sync.WaitGroup
		wg.Go(func() {
			for range 50 {
				err := c.Renew(t.Context(), time.Minute)
				if err != nil && !errors.Is(err, ErrReleased) {
					t.Error(err)
				}
			}
		})
		wg.Go(func() { c.Release(t.Context()) })
		wg.Wait()
		wantErr(t, c.Check(), ErrReleased)
	}
}

func TestTwoPhaseTransferAndReplacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New("resource", Options{})
		c := take(t, r, alice, AcquireOptions{Metadata: map[string]string{"job": "first"}})
		first, err := r.BeginTransfer(t.Context(), TransferOptions{TTL: time.Minute, Reason: "replace owner"})
		wantErr(t, err, nil)
		if !isClosed(c.Done()) || first.Info().OwnerBound {
			t.Fatal("unbound handoff not started")
		}
		before := first.Previous()
		if before.Owner != alice || before.Revoked {
			t.Fatal(before)
		}
		before.Metadata["job"] = "changed"
		if first.Previous().Metadata["job"] != "first" {
			t.Fatal("previous snapshot aliases metadata")
		}
		second, err := r.BeginTransfer(t.Context(), TransferOptions{Next: &bob, TTL: time.Minute, Replace: true})
		wantErr(t, err, nil)
		if !second.Info().OwnerBound {
			t.Fatal("missing owner binding")
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		wantErr(t, first.Wait(ctx), ErrReservationReplaced)
		if first.Cancel(t.Context()) {
			t.Fatal("superseded reservation removed successor")
		}
		c.Release(t.Context())
		wantErr(t, second.Wait(t.Context()), nil)
		wantErr(t, first.Wait(t.Context()), ErrReservationLost)
		next := take(t, r, bob, AcquireOptions{Token: second.Token()})
		next.Release(t.Context())
	})
}

func TestUnboundReservationAndAbandonedFreeTransfer(t *testing.T) {
	r := New("resource", Options{})
	p, err := r.BeginTransfer(t.Context(), TransferOptions{TTL: time.Minute})
	wantErr(t, err, nil)
	if p.Previous() != nil {
		t.Fatal("reported a previous owner of a free resource")
	}
	wantErr(t, p.Wait(t.Context()), nil)
	c := take(t, r, bob, AcquireOptions{Token: p.Token()})
	c.Release(t.Context())
	p, err = r.BeginTransfer(t.Context(), TransferOptions{TTL: time.Minute})
	wantErr(t, err, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	wantErr(t, p.Wait(ctx), nil) // readiness wins cancellation; explicitly abandon it
	if !p.Cancel(ctx) {
		t.Fatal("ready reservation not cancelled")
	}
	s, _ := r.Observe()
	if s.Reservation != nil {
		t.Fatal("abandoned free transfer left a reservation")
	}
}
