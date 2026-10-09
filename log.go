package claimkit

import (
	"context"
	"log/slog"
	"strings"
)

// Logging and callbacks share the same captured event. Metadata and secret
// capabilities are never logged. Do this before giving the event to user code.
func (r *Resource) log(ctx context.Context, e Event) {
	if r.opts.Logger == nil {
		return
	}
	l := r.opts.Logger(ctx)
	if l == nil {
		return
	}
	level := slog.LevelInfo
	if e.Err != nil {
		level = slog.LevelWarn
	}
	if e.Kind == EventRenewed || strings.HasSuffix(string(e.Kind), "_rejected") {
		level = slog.LevelDebug
	}
	owner, claimID, reservationID, reason := e.RequestedOwner, "", "", ""
	if e.Claim != nil {
		owner, claimID, reason = e.Claim.Owner, e.Claim.ID, e.Claim.Reason
	}
	if e.Reservation != nil {
		reservationID = e.Reservation.ID
		if e.Claim == nil && e.Reservation.OwnerBound {
			owner = e.Reservation.Owner
		}
		if reason == "" {
			reason = e.Reservation.Reason
		}
	}
	l.LogAttrs(ctx, level, "claimkit "+string(e.Kind),
		slog.String("component", "claimkit"), slog.String("resource", e.Resource), slog.String("event", string(e.Kind)),
		slog.Uint64("sequence", e.Sequence), slog.Uint64("revision", e.Revision), slog.Time("occurred_at", e.At),
		slog.String("claim_id", claimID), slog.String("reservation_id", reservationID),
		slog.String("owner_id", owner.ID), slog.String("session", owner.Session),
		slog.String("requester_id", e.RequestedOwner.ID), slog.String("requester_session", e.RequestedOwner.Session),
		slog.String("expected_claim_id", e.ExpectedClaimID), slog.String("reason", reason), slog.Any("error", e.Err))
}
