package claimkit

import (
	"fmt"
	"log/slog"
)

// Formatting intentionally omits metadata too, since applications may put
// private attributes there. JSON encoding preserves metadata but omits Token.
// Explicit access to Token is still secret-bearing and must not be logged.
func (o AcquireOptions) String() string {
	return fmt.Sprintf("claimkit.AcquireOptions{TTL:%s, Token:[REDACTED], Metadata:[REDACTED]}", o.TTL)
}
func (o AcquireOptions) GoString() string { return o.String() }
func (o AcquireOptions) LogValue() slog.Value {
	return slog.GroupValue(slog.Duration("ttl", o.TTL), slog.Bool("has_token", o.Token != ""))
}
