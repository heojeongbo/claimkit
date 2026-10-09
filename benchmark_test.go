package claimkit

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func BenchmarkAcquireRelease(b *testing.B) {
	r := New("resource", Options{})
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		c, _ := r.Acquire(ctx, alice, AcquireOptions{})
		c.Release(ctx)
	}
}

func BenchmarkAcquireReleaseTTL(b *testing.B) {
	r := New("resource", Options{})
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		c, _ := r.Acquire(ctx, alice, AcquireOptions{TTL: time.Minute})
		c.Release(ctx)
	}
}

func BenchmarkOccupied(b *testing.B) {
	r := New("resource", Options{})
	c := take(b, r, alice, AcquireOptions{})
	defer c.Release(context.Background())
	b.ReportAllocs()
	for b.Loop() {
		_, _ = r.Acquire(context.Background(), bob, AcquireOptions{})
	}
}

func BenchmarkCheck(b *testing.B) {
	c := take(b, New("resource", Options{}), alice, AcquireOptions{})
	defer c.Release(context.Background())
	b.ReportAllocs()
	for b.Loop() {
		_ = c.Check()
	}
}

func BenchmarkObserve(b *testing.B) {
	r := New("resource", Options{})
	c := take(b, r, alice, AcquireOptions{})
	defer c.Release(context.Background())
	b.ReportAllocs()
	for b.Loop() {
		_, _ = r.Observe()
	}
}

func BenchmarkContended(b *testing.B) {
	r := New("resource", Options{})
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if c, err := r.Acquire(ctx, alice, AcquireOptions{}); err == nil {
				c.Release(ctx)
			}
		}
	})
}

func BenchmarkTransfer(b *testing.B) {
	r := New("resource", Options{})
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		p, _ := r.Transfer(ctx, alice, time.Minute, "handoff")
		c, _ := r.Acquire(ctx, alice, AcquireOptions{Token: p.Token()})
		c.Release(ctx)
	}
}

func BenchmarkLogging(b *testing.B) {
	l := slog.New(slog.NewJSONHandler(io.Discard, nil))
	r := New("resource", Options{Logger: func(context.Context) *slog.Logger { return l }})
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		c, _ := r.Acquire(ctx, alice, AcquireOptions{})
		c.Release(ctx)
	}
}

func BenchmarkEvents(b *testing.B) {
	r := New("resource", Options{OnEvent: func(context.Context, Event) {}})
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		c, _ := r.Acquire(ctx, alice, AcquireOptions{})
		c.Release(ctx)
	}
}

func BenchmarkClaimContext(b *testing.B) {
	r := New("resource", Options{})
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		c, _ := r.Acquire(ctx, alice, AcquireOptions{})
		_, cancel := c.Context(ctx)
		cancel()
		c.Release(ctx)
	}
}
