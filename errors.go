package claimkit

import "maps"

// ConflictError captures the state at an unsuccessful arbitration decision.
// errors.Is still matches ErrOccupied, ErrReserved, ErrInvalidToken or ErrConflict.
// Snapshot is NOT a current read or an authorization decision; filter identities
// and metadata at your application boundary before returning it to other users.
type ConflictError struct {
	Operation       string
	ExpectedClaimID string
	cause           error
	state           Snapshot
}

func (e *ConflictError) Error() string { return e.cause.Error() }
func (e *ConflictError) Unwrap() error { return e.cause }

// Snapshot returns a fresh copy on every call, with no secret capability.
func (e *ConflictError) Snapshot() Snapshot {
	s := e.state
	if s.Claim != nil {
		c := *s.Claim
		c.Metadata = maps.Clone(c.Metadata)
		s.Claim = &c
	}
	if s.Reservation != nil {
		p := *s.Reservation
		s.Reservation = &p
	}
	return s
}

func (r *Resource) conflictLocked(operation, expected string, cause error) error {
	return &ConflictError{Operation: operation, ExpectedClaimID: expected, cause: cause, state: r.snapshotLocked()}
}
