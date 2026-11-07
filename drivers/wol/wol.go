// Package wol wakes machines with Wake-on-LAN magic packets. Power off is left
// to the POSSUM agent's graceful shutdown, and state is inferred by probing the host.
package wol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tpural/possum/pkg/power"
)

const Name = "wol"

func init() {
	power.Register(Name, func(raw json.RawMessage) (power.Driver, error) {
		var cfg Config
		if len(raw) > 0 {
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&cfg); err != nil {
				return nil, err
			}
		}
		return New(cfg)
	})
}

// Config configures the driver. Broadcast is where magic packets are sent; the
// sender must be on the target's network segment (pods usually need hostNetwork).
type Config struct {
	Broadcast    string          `json:"broadcast,omitempty"`
	ProbePort    int             `json:"probePort,omitempty"`
	ProbeTimeout metav1.Duration `json:"probeTimeout,omitempty"`
}

// Driver sends magic packets and probes hosts. Targets have the form
// "<mac>@<host>", for example "d8:43:ae:24:7c:8a@10.1.51.10".
type Driver struct {
	broadcast string
	port      string
	timeout   time.Duration
}

var _ power.Driver = (*Driver)(nil)

// New validates cfg and returns a Driver.
func New(cfg Config) (*Driver, error) {
	if cfg.Broadcast == "" {
		cfg.Broadcast = "255.255.255.255:9"
	}
	if _, _, err := net.SplitHostPort(cfg.Broadcast); err != nil {
		return nil, fmt.Errorf("wol: broadcast %q: %w", cfg.Broadcast, err)
	}
	if cfg.ProbePort == 0 {
		cfg.ProbePort = 22
	}
	if cfg.ProbeTimeout.Duration == 0 {
		cfg.ProbeTimeout.Duration = 2 * time.Second
	}
	return &Driver{broadcast: cfg.Broadcast, port: strconv.Itoa(cfg.ProbePort), timeout: cfg.ProbeTimeout.Duration}, nil
}

func (d *Driver) Name() string { return Name }

func (d *Driver) State(ctx context.Context, t power.Target) (power.State, error) {
	_, host, err := ParseTarget(t)
	if err != nil {
		return power.StateUnknown, err
	}
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	var dialer net.Dialer
	c, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, d.port))
	switch {
	case err == nil:
		_ = c.Close()
		return power.StateOn, nil
	case errors.Is(err, syscall.ECONNREFUSED):
		return power.StateOn, nil
	default:
		return power.StateOff, nil
	}
}

func (d *Driver) PowerOn(ctx context.Context, t power.Target) error {
	mac, _, err := ParseTarget(t)
	if err != nil {
		return err
	}
	var lc net.ListenConfig
	lc.Control = func(_, _ string, rc syscall.RawConn) error {
		var serr error
		if err := rc.Control(func(fd uintptr) {
			serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_BROADCAST, 1)
		}); err != nil {
			return err
		}
		return serr
	}
	conn, err := lc.ListenPacket(ctx, "udp4", ":0")
	if err != nil {
		return err
	}
	defer conn.Close()
	addr, err := net.ResolveUDPAddr("udp4", d.broadcast)
	if err != nil {
		return err
	}
	packet := MagicPacket(mac)
	for range 3 {
		if _, err := conn.WriteTo(packet, addr); err != nil {
			return fmt.Errorf("wol: send to %s: %w", d.broadcast, err)
		}
	}
	return nil
}

func (d *Driver) PowerOff(context.Context, power.Target) error { return nil }

// ParseTarget splits a target into the MAC address to wake and the host to probe.
func ParseTarget(t power.Target) (net.HardwareAddr, string, error) {
	mac, host, ok := strings.Cut(string(t), "@")
	if !ok || host == "" {
		return nil, "", fmt.Errorf("wol: target %q is not <mac>@<host>", t)
	}
	hw, err := net.ParseMAC(mac)
	if err != nil || len(hw) != 6 {
		return nil, "", fmt.Errorf("wol: target %q has an invalid MAC", t)
	}
	return hw, host, nil
}

// MagicPacket returns the Wake-on-LAN payload for mac: six 0xFF bytes followed by the MAC sixteen times.
func MagicPacket(mac net.HardwareAddr) []byte {
	p := bytes.Repeat([]byte{0xff}, 6)
	for range 16 {
		p = append(p, mac...)
	}
	return p
}
