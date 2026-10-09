package claimkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type eventRecorder struct {
	mu     sync.Mutex
	events []Event
}

func (r *eventRecorder) record(_ context.Context, e Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}
func (r *eventRecorder) read() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events)
}
func (r *eventRecorder) kinds() []EventKind {
	var kinds []EventKind
	for _, e := range r.read() {
		kinds = append(kinds, e.Kind)
	}
	return kinds
}

func TestDelayedExpiryFirstCause(t *testing.T) {
	for _, op := range []string{"check-revoke", "revoke", "transfer", "close", "info", "observe", "renew", "context", "release"} {
		t.Run(op, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var rec eventRecorder
				r := New("resource", Options{OnEvent: rec.record})
				c := take(t, r, alice, AcquireOptions{TTL: time.Second})
				deadline := c.Info().ExpiresAt
				r.mu.Lock()
				c.timer.Stop()
				r.mu.Unlock()
				time.Sleep(time.Second)
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				switch op {
				case "check-revoke":
					wantErr(t, c.Check(), ErrExpired)
					wantErr(t, r.Revoke(ctx, "late revoke"), ErrNotReleased)
				case "revoke":
					wantErr(t, r.Revoke(ctx, "late revoke"), ErrNotReleased)
				case "transfer":
					p, err := r.BeginTransfer(ctx, TransferOptions{TTL: time.Minute})
					wantErr(t, err, nil)
					defer p.Cancel(t.Context())
				case "close":
					wantErr(t, r.Close(ctx), ErrNotReleased)
				case "info":
					if c.Info().Cause != CauseExpired {
						t.Fatal("Info missed deadline")
					}
				case "observe":
					s, _ := r.Observe()
					if s.Claim.Cause != CauseExpired {
						t.Fatal("Observe missed deadline")
					}
				case "renew":
					wantErr(t, c.Renew(ctx, time.Minute), ErrExpired)
				case "context":
					work, stop := c.Context(ctx)
					defer stop()
					wantErr(t, context.Cause(work), ErrExpired)
				case "release":
					c.Release(ctx)
				}
				info := c.Info()
				if info.Cause != CauseExpired || !info.EndedAt.Equal(deadline) || info.Reason != "lease expired" {
					t.Fatal(info)
				}
				wantErr(t, c.Check(), ErrExpired)
				if !isClosed(c.Done()) {
					t.Fatal("deadline not signalled")
				}
				c.Release(ctx)
				c.expire()
				if count := slices.Index(rec.kinds(), EventExpired); count < 0 {
					t.Fatal(rec.kinds())
				}
				n := 0
				for _, e := range rec.read() {
					if e.Kind == EventExpired {
						n++
					}
				}
				if n != 1 {
					t.Fatalf("expiry emitted %d times", n)
				}
			})
		})
	}
}

func TestAcquireOptionsRedaction(t *testing.T) {
	o := AcquireOptions{TTL: time.Minute, Token: "synthetic-secret-marker", Metadata: map[string]string{"private": "metadata-marker"}}
	var out strings.Builder
	logger := slog.New(slog.NewJSONHandler(&out, nil))
	logger.Info("request", "options", o, "pointer", &o)
	for _, v := range []any{o, &o, []AcquireOptions{o}, struct{ Options AcquireOptions }{o}} {
		for _, format := range []string{"%v", "%+v", "%#v"} {
			text := fmt.Sprintf(format, v)
			if strings.Contains(text, o.Token) || strings.Contains(text, "metadata-marker") {
				t.Fatal(text)
			}
		}
		data, err := json.Marshal(v)
		wantErr(t, err, nil)
		if strings.Contains(string(data), o.Token) {
			t.Fatal(string(data))
		}
	}
	if strings.Contains(out.String(), o.Token) || strings.Contains(out.String(), "metadata-marker") {
		t.Fatal(out.String())
	}
	data, err := json.Marshal(o)
	wantErr(t, err, nil)
	if !strings.Contains(string(data), "metadata-marker") {
		t.Fatal("JSON should preserve explicit metadata")
	}
	if (AcquireOptions{}).LogValue().Kind() != slog.KindGroup {
		t.Fatal("not a structured slog value")
	}
}

func TestConflictSnapshotAndConditionalOperations(t *testing.T) {
	var rec eventRecorder
	r := New("resource", Options{OnEvent: rec.record})
	first := take(t, r, alice, AcquireOptions{Metadata: map[string]string{"job": "one"}})
	_, err := r.Acquire(t.Context(), bob, AcquireOptions{})
	var conflict *ConflictError
	if !errors.As(err, &conflict) || conflict.Operation != "acquire" || conflict.Error() != ErrOccupied.Error() {
		t.Fatal(err)
	}
	wantErr(t, err, ErrOccupied)
	oldID := first.Info().ID
	first.Release(t.Context())
	next := take(t, r, bob, AcquireOptions{})
	snap := conflict.Snapshot()
	if snap.Claim.ID != oldID || snap.Claim.Owner != alice || snap.Claim.Metadata["job"] != "one" {
		t.Fatal(snap)
	}
	snap.Claim.Metadata["job"] = "changed"
	snap.Claim.ID = "changed"
	if conflict.Snapshot().Claim.ID != oldID || conflict.Snapshot().Claim.Metadata["job"] != "one" {
		t.Fatal("conflict aliases caller snapshots")
	}
	for _, op := range []string{"revoke", "transfer"} {
		if op == "revoke" {
			err = r.RevokeWithOptions(t.Context(), RevokeOptions{ExpectedClaimID: oldID})
		} else {
			_, err = r.BeginTransfer(t.Context(), TransferOptions{ExpectedClaimID: oldID, TTL: time.Minute})
		}
		wantErr(t, err, ErrConflict)
		if !errors.As(err, &conflict) || conflict.Operation != op || conflict.ExpectedClaimID != oldID || conflict.Snapshot().Claim.ID != next.Info().ID {
			t.Fatal(err)
		}
		wantErr(t, next.Check(), nil)
	}
	p, err := r.BeginTransfer(t.Context(), TransferOptions{ExpectedClaimID: next.Info().ID, TTL: time.Minute})
	wantErr(t, err, nil)
	wantErr(t, next.Check(), ErrRevoked)
	next.Release(t.Context())
	_, err = r.Acquire(t.Context(), alice, AcquireOptions{})
	wantErr(t, err, ErrReserved)
	if !errors.As(err, &conflict) {
		t.Fatal(err)
	}
	snap = conflict.Snapshot()
	snap.Reservation.ID = "changed"
	if conflict.Snapshot().Reservation.ID != p.Info().ID {
		t.Fatal("reservation snapshot aliases")
	}
	p.Cancel(t.Context())
	wantErr(t, r.RevokeWithOptions(t.Context(), RevokeOptions{ExpectedClaimID: oldID}), ErrConflict)
	// A matching conditional revoke targets this acquisition and waits for cleanup.
	current := take(t, r, alice, AcquireOptions{})
	go func() { <-current.Done(); current.Release(t.Context()) }()
	wantErr(t, r.RevokeWithOptions(t.Context(), RevokeOptions{ExpectedClaimID: current.Info().ID, Reason: "matched"}), nil)
	if current.Info().Reason != "matched" {
		t.Fatal(current.Info())
	}
}

func TestReservationTerminalWakeWithoutRelease(t *testing.T) {
	for _, end := range []string{"cancel", "replace", "close"} {
		t.Run(end, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var rec eventRecorder
				r := New("resource", Options{OnEvent: rec.record})
				c := take(t, r, alice, AcquireOptions{})
				p, err := r.BeginTransfer(t.Context(), TransferOptions{TTL: time.Minute, Reason: "handoff"})
				wantErr(t, err, nil)
				info := p.Info()
				if info.ID == "" || info.ID == p.Token() || info.State != ReservationWaiting || info.CreatedAt.IsZero() || !info.ReadyAt.IsZero() || info.Reason != "handoff" || p.Err() != nil || isClosed(p.Done()) {
					t.Fatal(info)
				}
				result := make(chan error, 1)
				go func() { result <- p.Wait(t.Context()) }()
				synctest.Wait()
				var status ReservationStatus
				var cause error
				switch end {
				case "cancel":
					p.Cancel(t.Context())
					status, cause = ReservationCancelled, ErrReservationCancelled
				case "replace":
					q, err := r.BeginTransfer(t.Context(), TransferOptions{TTL: time.Minute, Replace: true})
					wantErr(t, err, nil)
					defer q.Cancel(t.Context())
					status, cause = ReservationReplaced, ErrReservationReplaced
				case "close":
					ctx, cancel := context.WithTimeout(t.Context(), time.Second)
					defer cancel()
					wantErr(t, r.Close(ctx), ErrNotReleased)
					status, cause = ReservationClosed, ErrClosed
				}
				synctest.Wait()
				select {
				case err := <-result:
					wantErr(t, err, cause)
					wantErr(t, err, ErrReservationLost)
				default:
					t.Fatal("terminal waiter still blocked on old holder")
				}
				if isClosed(c.Released()) || !isClosed(p.Done()) || p.Info().State != status || p.Info().EndedAt.IsZero() {
					t.Fatal(p.Info())
				}
				wantErr(t, p.Err(), cause)
				if p.Cancel(t.Context()) {
					t.Fatal("terminal cancel succeeded")
				}
				c.Release(t.Context())
			})
		})
	}
}

func TestReservationReadyConsumedAndLazyExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var rec eventRecorder
		r := New("resource", Options{OnEvent: rec.record})
		p, err := r.BeginTransfer(t.Context(), TransferOptions{TTL: time.Second})
		wantErr(t, err, nil)
		if p.Info().State != ReservationReady || p.Info().ReadyAt.IsZero() || p.Err() != nil || isClosed(p.Done()) {
			t.Fatal(p.Info())
		}
		for range 3 {
			wantErr(t, p.Wait(t.Context()), nil)
		}
		c := take(t, r, alice, AcquireOptions{Token: p.Token()})
		if p.Info().State != ReservationConsumed || !isClosed(p.Done()) {
			t.Fatal(p.Info())
		}
		wantErr(t, p.Err(), ErrReservationConsumed)
		wantErr(t, p.Wait(t.Context()), ErrReservationConsumed)
		c.Release(t.Context())
		p, err = r.BeginTransfer(t.Context(), TransferOptions{TTL: time.Second})
		wantErr(t, err, nil)
		r.mu.Lock()
		p.timer.Stop()
		r.mu.Unlock()
		time.Sleep(time.Second)
		wantErr(t, p.Err(), ErrExpired)
		if p.Info().State != ReservationExpired || p.Info().EndedAt != p.Info().ExpiresAt {
			t.Fatal(p.Info())
		}
		wantErr(t, p.Wait(t.Context()), ErrExpired)
		expected := []EventKind{EventReservationCreated, EventReservationReady, EventReservationConsumed, EventAcquired, EventReleased, EventReservationCreated, EventReservationReady, EventReservationExpired}
		if !slices.Equal(rec.kinds(), expected) {
			t.Fatal(rec.kinds())
		}
	})
}

func TestClaimContextLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type key struct{}
		r := New("resource", Options{})
		c := take(t, r, alice, AcquireOptions{})
		parent := context.WithValue(t.Context(), key{}, "trace")
		work, stop := c.Context(parent)
		defer stop()
		second, stopSecond := c.Context(parent)
		defer stopSecond()
		if work.Value(key{}) != "trace" || work.Err() != nil {
			t.Fatal("lost parent values")
		}
		stopSecond()
		wantErr(t, context.Cause(second), context.Canceled)
		wantErr(t, c.Check(), nil)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		wantErr(t, r.Revoke(ctx, "stop work"), ErrNotReleased)
		synctest.Wait()
		wantErr(t, context.Cause(work), ErrRevoked)
		wantErr(t, context.Cause(second), context.Canceled)
		if isClosed(c.Released()) {
			t.Fatal("context auto-released work")
		}
		c.Release(t.Context())
		late, stopLate := c.Context(parent)
		defer stopLate()
		wantErr(t, context.Cause(late), ErrRevoked)
		next := take(t, r, bob, AcquireOptions{})
		cancelled, cancelParent := context.WithCancelCause(parent)
		child, stopChild := next.Context(cancelled)
		defer stopChild()
		parentCause := errors.New("caller ended")
		cancelParent(parentCause)
		wantErr(t, context.Cause(child), parentCause)
		wantErr(t, next.Check(), nil)
		next.Release(t.Context())
		synctest.Wait()
		wantErr(t, context.Cause(child), parentCause)
		ended := take(t, r, alice, AcquireOptions{})
		ended.Release(t.Context())
		after, stopAfter := ended.Context(parent)
		defer stopAfter()
		wantErr(t, context.Cause(after), ErrReleased)
		voluntary := take(t, r, alice, AcquireOptions{})
		work, stop = voluntary.Context(parent)
		defer stop()
		voluntary.Release(t.Context())
		synctest.Wait()
		wantErr(t, context.Cause(work), ErrReleased)
	})
}

func TestEventSnapshotsAndReentrancy(t *testing.T) {
	var rec eventRecorder
	var r *Resource
	r = New("resource", Options{OnEvent: func(ctx context.Context, e Event) {
		rec.record(ctx, e)
		r.Observe() // callbacks run after unlock, including denied attempts
		if e.Kind == EventAcquired {
			e.Claim.Metadata["job"] = "callback mutation"
			e.Claim.ID = "callback mutation"
		}
	}})
	c := take(t, r, alice, AcquireOptions{Metadata: map[string]string{"job": "original"}})
	if c.Info().Metadata["job"] != "original" || c.Info().ID == "callback mutation" {
		t.Fatal("callback aliased live claim")
	}
	_, err := r.Acquire(t.Context(), bob, AcquireOptions{})
	wantErr(t, err, ErrOccupied)
	if got := rec.read()[1]; got.RequestedOwner != bob || got.Claim.Owner != alice || got.Revision != 1 || !errors.Is(got.Err, ErrOccupied) {
		t.Fatal(got)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = r.Acquire(ctx, bob, AcquireOptions{})
	wantErr(t, err, context.Canceled)
	wantErr(t, c.Renew(t.Context(), 0), ErrInvalidTTL)
	wantErr(t, c.Renew(t.Context(), time.Minute), nil)
	wantErr(t, r.Revoke(ctx, "cancelled"), context.Canceled)
	_, err = r.BeginTransfer(t.Context(), TransferOptions{})
	wantErr(t, err, ErrInvalidTTL)
	c.Release(t.Context())
	c.Release(t.Context())
	wantErr(t, r.Close(t.Context()), nil)
	wantErr(t, r.Close(t.Context()), nil)
	expected := []EventKind{EventAcquired, EventAcquireRejected, EventAcquireRejected, EventRenewRejected, EventRenewed, EventRevokeRejected, EventTransferRejected, EventReleased, EventClosed}
	if !slices.Equal(rec.kinds(), expected) {
		t.Fatal(rec.kinds())
	}
	for i, e := range rec.read() {
		if e.Sequence != uint64(i+1) || e.At.IsZero() || e.Resource != "resource" {
			t.Fatal(e)
		}
	}
}

func TestConcurrentEventSequence(t *testing.T) {
	var rec eventRecorder
	r := New("resource", Options{OnEvent: rec.record})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				c, err := r.Acquire(t.Context(), alice, AcquireOptions{})
				if err == nil {
					c.Release(t.Context())
				} else {
					wantErr(t, err, ErrOccupied)
				}
			}
		})
	}
	wg.Wait()
	events := rec.read()
	slices.SortFunc(events, func(a, b Event) int {
		if a.Sequence < b.Sequence {
			return -1
		}
		if a.Sequence > b.Sequence {
			return 1
		}
		return 0
	})
	var rev uint64
	for i, e := range events {
		if e.Sequence != uint64(i+1) || e.Revision < rev {
			t.Fatal(e)
		}
		rev = e.Revision
	}
}

func TestTransitionLogsContainIdentityNotSecrets(t *testing.T) {
	var out strings.Builder
	logger := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	r := New("resource", Options{Logger: func(context.Context) *slog.Logger { return logger }})
	p, err := r.BeginTransfer(t.Context(), TransferOptions{Next: &alice, TTL: time.Minute, Reason: "handoff"})
	wantErr(t, err, nil)
	_, err = r.Acquire(t.Context(), bob, AcquireOptions{Token: p.Token(), Metadata: map[string]string{"private": "synthetic-private-marker"}})
	wantErr(t, err, ErrInvalidToken)
	c := take(t, r, alice, AcquireOptions{Token: p.Token()})
	c.Release(t.Context())
	text := out.String()
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		var record map[string]any
		wantErr(t, json.Unmarshal([]byte(line), &record), nil)
		if record["event"] == string(EventReservationReady) && record["owner_id"] != alice.ID {
			t.Fatal("reservation log lost bound owner", record)
		}
	}
	if strings.Contains(text, p.Token()) || strings.Contains(text, "synthetic-private-marker") {
		t.Fatal("log exposed secret")
	}
	for _, field := range []string{`"reservation_id":"` + p.Info().ID + `"`, `"reason":"handoff"`, `"requester_id":"bob"`, `"sequence":`, `"revision":`, `"level":"DEBUG"`} {
		if !strings.Contains(text, field) {
			t.Fatalf("missing %s", field)
		}
	}
}
