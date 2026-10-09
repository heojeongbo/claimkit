package claimkit_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/heojeongbo/claimkit"
)

func ExampleResource_Acquire() {
	ctx := context.Background()
	r := claimkit.New("documents/42/edit", claimkit.Options{})
	c, err := r.Acquire(ctx,
		claimkit.Owner{ID: "user-7", Session: "tab-2"},
		claimkit.AcquireOptions{Metadata: map[string]string{"operation": "edit"}})
	if err != nil {
		panic(err)
	}
	defer c.Release(ctx)
	s, _ := r.Observe()
	fmt.Println(s.Claim.Owner.ID, s.Claim.Owner.Session)
	fmt.Println(c.Check() == nil)
	// Output:
	// user-7 tab-2
	// true
}

func ExampleResource_Transfer() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r := claimkit.New("device/configuration", claimkit.Options{})
	first, err := r.Acquire(ctx, claimkit.Owner{ID: "alice"}, claimkit.AcquireOptions{})
	if err != nil {
		panic(err)
	}
	var cleaned atomic.Bool
	go func() {
		<-first.Done()
		// Stop accepting commands, cancel work, and join all work here.
		cleaned.Store(true)
		first.Release(ctx)
	}()
	nextOwner := claimkit.Owner{ID: "bob", Session: "session-2"}
	reservation, err := r.Transfer(ctx, nextOwner, time.Minute, "requested handoff")
	if err != nil {
		panic(err)
	}
	defer reservation.Cancel(ctx)
	second, err := r.Acquire(ctx, nextOwner, claimkit.AcquireOptions{Token: reservation.Token()})
	if err != nil {
		panic(err)
	}
	defer second.Release(ctx)
	fmt.Println("previous work stopped:", cleaned.Load())
	fmt.Println("old release succeeded:", first.Release(ctx))
	fmt.Println("new owner:", second.Info().Owner.ID)
	// Output:
	// previous work stopped: true
	// old release succeeded: false
	// new owner: bob
}

func ExampleResource_Observe() {
	ctx := context.Background()
	r := claimkit.New("shared-resource", claimkit.Options{})
	s, changed := r.Observe()
	fmt.Println("initially occupied:", s.Claim != nil)
	c, err := r.Acquire(ctx, claimkit.Owner{ID: "worker"}, claimkit.AcquireOptions{})
	if err != nil {
		panic(err)
	}
	defer c.Release(ctx)
	<-changed // the acquisition cannot be missed between snapshot and subscribe
	s, _ = r.Observe()
	fmt.Println("current owner:", s.Claim.Owner.ID)
	// Output:
	// initially occupied: false
	// current owner: worker
}

func ExampleClaim_Context() {
	ctx := context.Background()
	r := claimkit.New("jobs/export", claimkit.Options{})
	c, err := r.Acquire(ctx, claimkit.Owner{ID: "worker"}, claimkit.AcquireOptions{})
	if err != nil {
		panic(err)
	}
	work, cancel := c.Context(ctx)
	defer cancel()
	joined := make(chan struct{})
	go func() {
		<-work.Done()
		// Finish work cleanup before acknowledging release.
		c.Release(ctx)
		close(joined)
	}()
	if err := r.Revoke(ctx, "operator stopped export"); err != nil {
		panic(err)
	}
	<-joined
	fmt.Println("cause:", context.Cause(work))
	fmt.Println("cleanup acknowledged:", !c.Info().ReleasedAt.IsZero())
	// Output:
	// cause: claimkit: claim was revoked
	// cleanup acknowledged: true
}

func ExampleConflictError() {
	ctx := context.Background()
	r := claimkit.New("documents/42", claimkit.Options{})
	c, err := r.Acquire(ctx, claimkit.Owner{ID: "alice"}, claimkit.AcquireOptions{})
	if err != nil {
		panic(err)
	}
	defer c.Release(ctx)
	_, err = r.Acquire(ctx, claimkit.Owner{ID: "bob"}, claimkit.AcquireOptions{})
	var conflict *claimkit.ConflictError
	if errors.As(err, &conflict) {
		fmt.Println("occupied:", errors.Is(err, claimkit.ErrOccupied))
		fmt.Println("holder at rejection:", conflict.Snapshot().Claim.Owner.ID)
	}
	// Output:
	// occupied: true
	// holder at rejection: alice
}

func ExampleOptions_OnEvent() {
	ctx := context.Background()
	r := claimkit.New("resource", claimkit.Options{OnEvent: func(_ context.Context, e claimkit.Event) {
		fmt.Println(e.Sequence, e.Kind)
	}})
	c, err := r.Acquire(ctx, claimkit.Owner{}, claimkit.AcquireOptions{})
	if err != nil {
		panic(err)
	}
	c.Release(ctx)
	c.Release(ctx) // idempotent release does not repeat the event
	// Output:
	// 1 acquired
	// 2 released
}
