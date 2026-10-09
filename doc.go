// Package claimkit coordinates exclusive ownership of a resource within one
// process. Revocation and lease expiry request that a holder stop; only Release,
// after the holder's work has stopped, makes the resource available again.
//
// Resource, Claim, and Reservation methods are safe for concurrent use. These
// objects must not be copied. Construct resources with New; claims and
// reservations can only be obtained from their resource.
//
// Authentication, authorization, scheduling, durable storage, and enforcement at
// an external device belong to the application. A successful Check is a point in
// time observation, not a transaction with subsequent external side effects.
package claimkit
