package claimkit_test

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/heojeongbo/claimkit"
)

// Formatting a pending handoff can overlap the previous holder's release.
// Value-receiver redaction must not copy the mutable expiry/timer state.
func TestReservationFormattingDuringRelease(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	for range 100 {
		r := claimkit.New("worker", claimkit.Options{})
		c, err := r.Acquire(ctx, claimkit.Owner{}, claimkit.AcquireOptions{})
		if err != nil {
			t.Fatal(err)
		}
		p, err := r.BeginTransfer(ctx, claimkit.TransferOptions{TTL: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				for _, value := range []any{p, *p} {
					output := fmt.Sprintf("%v %+v %#v", value, value, value)
					if strings.Contains(output, p.Token()) || !strings.Contains(output, "REDACTED") {
						t.Error("reservation formatting exposed its state")
					}
					logger.InfoContext(ctx, "handoff", "reservation", value)
				}
			}
		}()
		c.Release(ctx)
		wg.Wait()
		p.Cancel(ctx)
	}
}
