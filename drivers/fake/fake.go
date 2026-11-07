// Package fake provides an in-memory power driver for tests and demos.
package fake

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"

	"github.com/tpural/possum/pkg/power"
)

const Name = "fake"

func init() {
	power.Register(Name, func(raw json.RawMessage) (power.Driver, error) {
		var cfg Config
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &cfg); err != nil {
				return nil, err
			}
		}
		return New(cfg), nil
	})
}

// Config seeds the driver. Outlets maps targets to their initial state; NICs maps
// lowercase MAC addresses to the target they are plugged into.
type Config struct {
	Outlets map[power.Target]power.State `json:"outlets"`
	NICs    map[string]power.Target      `json:"nics"`
}

// Driver is an in-memory power.Driver that records every call.
type Driver struct {
	mu      sync.Mutex
	outlets map[power.Target]power.State
	nics    map[string]power.Target
	calls   []string
	fail    error
}

var (
	_ power.Driver     = (*Driver)(nil)
	_ power.Discoverer = (*Driver)(nil)
)

// New returns a Driver initialised from cfg.
func New(cfg Config) *Driver {
	d := &Driver{outlets: map[power.Target]power.State{}, nics: map[string]power.Target{}}
	for t, s := range cfg.Outlets {
		d.outlets[t] = s
	}
	for mac, t := range cfg.NICs {
		d.nics[strings.ToLower(mac)] = t
	}
	return d
}

func (d *Driver) Name() string { return Name }

func (d *Driver) State(_ context.Context, t power.Target) (power.State, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fail != nil {
		return power.StateUnknown, d.fail
	}
	s, ok := d.outlets[t]
	if !ok {
		return power.StateUnknown, power.ErrNotFound
	}
	return s, nil
}

func (d *Driver) PowerOn(ctx context.Context, t power.Target) error {
	return d.set(ctx, t, power.StateOn)
}

func (d *Driver) PowerOff(ctx context.Context, t power.Target) error {
	return d.set(ctx, t, power.StateOff)
}

func (d *Driver) Discover(_ context.Context, macs []net.HardwareAddr) (map[string]power.Target, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := map[string]power.Target{}
	for _, m := range macs {
		if t, ok := d.nics[m.String()]; ok {
			out[m.String()] = t
		}
	}
	return out, nil
}

// Calls returns the power operations performed so far, as "on:<target>" or "off:<target>".
func (d *Driver) Calls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.calls...)
}

// FailWith makes every subsequent operation return err; nil restores normal behaviour.
func (d *Driver) FailWith(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fail = err
}

func (d *Driver) set(ctx context.Context, t power.Target, s power.State) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fail != nil {
		return d.fail
	}
	if _, ok := d.outlets[t]; !ok {
		return power.ErrNotFound
	}
	d.outlets[t] = s
	d.calls = append(d.calls, string(s)+":"+string(t))
	return nil
}
