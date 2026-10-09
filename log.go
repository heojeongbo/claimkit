package claimkit

import (
	"context"
	"log/slog"
)

func (r *Resource) log(ctx context.Context, event, claimID string, revision uint64, err error, owner Owner, reason string) {
	if r.opts.Logger == nil {
		return
	}
	l := r.opts.Logger(ctx)
	if l == nil {
		return
	}
	level := slog.LevelInfo
	if event == "renewed" {
		level = slog.LevelDebug
	}
	if err != nil {
		level = slog.LevelWarn
	}
	l.LogAttrs(ctx, level, "claimkit "+event,
		slog.String("component", "claimkit"), slog.String("resource", r.name),
		slog.String("event", event), slog.String("claim_id", claimID),
		slog.Uint64("revision", revision), slog.Any("error", err),
		slog.String("owner_id", owner.ID), slog.String("session", owner.Session),
		slog.String("reason", reason))
}
