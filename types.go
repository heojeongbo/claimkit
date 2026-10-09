package claimkit

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

var (
	ErrOccupied             = errors.New("claimkit: resource is occupied")
	ErrReserved             = errors.New("claimkit: resource is reserved")
	ErrInvalidToken         = errors.New("claimkit: reservation token is invalid")
	ErrInvalidTTL           = errors.New("claimkit: TTL must be positive")
	ErrRevoked              = errors.New("claimkit: claim was revoked")
	ErrExpired              = errors.New("claimkit: claim expired")
	ErrReleased             = errors.New("claimkit: claim was released")
	ErrClosed               = errors.New("claimkit: resource is closed")
	ErrNotReleased          = errors.New("claimkit: holder has not released")
	ErrConflict             = errors.New("claimkit: claim changed")
	ErrReservationCancelled = errors.New("claimkit: reservation cancelled")
	ErrReservationReplaced  = errors.New("claimkit: reservation replaced")
	ErrReservationConsumed  = errors.New("claimkit: reservation consumed")
	ErrReservationLost      = errors.New("claimkit: reservation ended before handoff completed")
)

// Owner is application-defined identity. Session distinguishes concurrent uses
// by the same principal. Empty fields are allowed for anonymous or local work.
// These are descriptive attributes; the application must authenticate them.
type Owner struct {
	ID      string `json:"id,omitempty"`
	Session string `json:"session,omitempty"`
}

// Options configures a resource. Logger resolves a logger from each operation's
// context, for example github.com/lesomnus/otx/log.From in go-app applications.
// Nil disables logging. The resolver and its handler must be concurrency-safe;
// neither is called with the resource mutex held. They should return promptly.
type Options struct {
	Logger func(context.Context) *slog.Logger
	// OnEvent receives detached transition/denial events; see Event for delivery semantics.
	OnEvent func(context.Context, Event)
}

// AcquireOptions describes one acquisition. TTL zero disables expiration;
// a positive TTL requires Renew to keep the claim active. Expiration signals
// Done but NEVER releases the resource. Metadata is copied on input and output;
// callers must not mutate the input map concurrently with Acquire.
//
// Token is a secret, single-use reservation capability. It must match both the
// resource's live reservation and any configured Owner binding. A supplied
// stale token is rejected even when the resource is free. Omit Token for ordinary acquisition.
type AcquireOptions struct {
	TTL      time.Duration
	Metadata map[string]string
	Token    string `json:"-"`
}

// ClaimInfo is a detached snapshot, safe for the caller to modify. ID is a public
// identifier, NOT a credential or a durable fencing token. An expired or revoked
// claim remains occupied until its holder calls Release after cleanup.
type ClaimInfo struct {
	ID         string            `json:"id"`
	Owner      Owner             `json:"owner"`
	AcquiredAt time.Time         `json:"acquired_at"`
	ReleasedAt time.Time         `json:"released_at,omitempty"`
	ExpiresAt  time.Time         `json:"expires_at,omitempty"`
	EndedAt    time.Time         `json:"ended_at,omitempty"`
	Cause      Cause             `json:"cause,omitempty"`
	Revoked    bool              `json:"revoked"`
	Reason     string            `json:"reason,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// ReservationInfo describes the next owner without exposing the secret token.
// ExpiresAt is zero while the current holder is still cleaning up. Its TTL
// starts only after that holder releases.
type ReservationInfo struct {
	ID         string            `json:"id"`
	State      ReservationStatus `json:"state"`
	CreatedAt  time.Time         `json:"created_at"`
	ReadyAt    time.Time         `json:"ready_at,omitempty"`
	EndedAt    time.Time         `json:"ended_at,omitempty"`
	Reason     string            `json:"reason,omitempty"`
	OwnerBound bool              `json:"owner_bound"`
	Owner      Owner             `json:"owner"`
	ExpiresAt  time.Time         `json:"expires_at,omitempty"`
}

// Snapshot is a consistent view of one resource. Revision increases on changes
// within this Resource instance only, and is not a distributed fencing token.
type Snapshot struct {
	Resource    string           `json:"resource"`
	Revision    uint64           `json:"revision"`
	Closed      bool             `json:"closed"`
	Claim       *ClaimInfo       `json:"claim,omitempty"`
	Reservation *ReservationInfo `json:"reservation,omitempty"`
}

// TransferOptions controls an explicit two-phase handoff. Next nil grants the
// next acquisition to any authenticated owner presenting the token; non-nil binds
// the capability to that exact owner/session. Replace explicitly supersedes a
// pending reservation. Superseded waiters cannot withdraw the replacement.
// Neither option changes the rule that the previous holder must Release.
type TransferOptions struct {
	// ExpectedClaimID, when non-empty, must match the current claim under the lock.
	ExpectedClaimID string
	Next            *Owner
	TTL             time.Duration
	Reason          string
	Replace         bool
}

// RevokeOptions optionally compares the public claim ID before requesting cleanup.
// Empty ExpectedClaimID preserves unconditional Revoke behavior. A mismatch,
// including a now-free resource, returns ErrConflict without revoking anyone.
type RevokeOptions struct {
	Reason          string
	ExpectedClaimID string
}

// Cause identifies the first terminal condition. Empty means still active.
// ReleasedAt separately identifies when cleanup finished and ownership was freed.
type Cause string

const (
	CauseReleased Cause = "released"
	CauseRevoked  Cause = "revoked"
	CauseExpired  Cause = "expired"
	CauseClosed   Cause = "closed"
)

// ReservationStatus distinguishes readiness from terminal states. Termination
// never releases the previous holder; cleanup remains that holder's obligation.
type ReservationStatus string

const (
	ReservationWaiting   ReservationStatus = "waiting"
	ReservationReady     ReservationStatus = "ready"
	ReservationConsumed  ReservationStatus = "consumed"
	ReservationCancelled ReservationStatus = "cancelled"
	ReservationReplaced  ReservationStatus = "replaced"
	ReservationExpired   ReservationStatus = "expired"
	ReservationClosed    ReservationStatus = "closed"
)
