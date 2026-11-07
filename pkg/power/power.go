// Package power defines the contract between POSSUM and the systems that switch node power.
package power

import (
	"context"
	"errors"
	"net"
)

// Target identifies a single power outlet in a driver-specific format.
type Target string

// State is the power state of a Target.
type State string

const (
	StateOn      State = "on"
	StateOff     State = "off"
	StateUnknown State = "unknown"
)

var (
	ErrNotFound    = errors.New("power target not found")
	ErrRateLimited = errors.New("power backend rate limited")
	ErrUnsupported = errors.New("operation not supported by driver")
)

// Driver switches power for targets. Implementations must be safe for concurrent use
// and PowerOn/PowerOff must be idempotent.
type Driver interface {
	Name() string
	State(ctx context.Context, t Target) (State, error)
	PowerOn(ctx context.Context, t Target) error
	PowerOff(ctx context.Context, t Target) error
}

// Discoverer is implemented by drivers that can find the target a NIC is connected to.
type Discoverer interface {
	Discover(ctx context.Context, macs []net.HardwareAddr) (map[string]Target, error)
}
