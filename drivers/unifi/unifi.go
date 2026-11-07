// Package unifi switches PoE ports on UniFi switches through a UniFi OS console.
package unifi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tpural/possum/pkg/power"
)

const Name = "unifi"

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

// Config configures the UniFi driver. Username and Password fall back to the
// POSSUM_UNIFI_USERNAME and POSSUM_UNIFI_PASSWORD environment variables.
type Config struct {
	URL                string          `json:"url"`
	Site               string          `json:"site,omitempty"`
	Username           string          `json:"username,omitempty"`
	Password           string          `json:"password,omitempty"`
	InsecureSkipVerify bool            `json:"insecureSkipVerify,omitempty"`
	CacheTTL           metav1.Duration `json:"cacheTTL,omitempty"`
}

// Driver controls PoE through the UniFi Network application. Targets have the
// form "<switch-mac>/<port>", for example "74:ac:b9:55:27:a0/4".
type Driver struct {
	api *client
	ttl time.Duration

	writeMu sync.Mutex

	cacheMu  sync.Mutex
	devices  []device
	cachedAt time.Time
}

var (
	_ power.Driver     = (*Driver)(nil)
	_ power.Discoverer = (*Driver)(nil)
)

// New validates cfg and returns a Driver. It does not contact the console.
func New(cfg Config) (*Driver, error) {
	if cfg.URL == "" {
		return nil, errors.New("unifi: url is required")
	}
	if cfg.Site == "" {
		cfg.Site = "default"
	}
	if cfg.Username == "" {
		cfg.Username = os.Getenv("POSSUM_UNIFI_USERNAME")
	}
	if cfg.Password == "" {
		cfg.Password = os.Getenv("POSSUM_UNIFI_PASSWORD")
	}
	if cfg.Username == "" || cfg.Password == "" {
		return nil, errors.New("unifi: username and password are required")
	}
	if cfg.CacheTTL.Duration == 0 {
		cfg.CacheTTL.Duration = 10 * time.Second
	}
	api, err := newClient(cfg.URL, cfg.Site, cfg.Username, cfg.Password, cfg.InsecureSkipVerify)
	if err != nil {
		return nil, err
	}
	return &Driver{api: api, ttl: cfg.CacheTTL.Duration}, nil
}

func (d *Driver) Name() string { return Name }

func (d *Driver) State(ctx context.Context, t power.Target) (power.State, error) {
	mac, port, err := ParseTarget(t)
	if err != nil {
		return power.StateUnknown, err
	}
	devs, err := d.cachedDevices(ctx)
	if err != nil {
		return power.StateUnknown, err
	}
	dev, ok := findDevice(devs, mac)
	if !ok {
		return power.StateUnknown, fmt.Errorf("switch %s: %w", mac, power.ErrNotFound)
	}
	p, ok := dev.port(port)
	if !ok {
		return power.StateUnknown, fmt.Errorf("switch %s port %d: %w", mac, port, power.ErrNotFound)
	}
	return p.state(), nil
}

func (d *Driver) PowerOn(ctx context.Context, t power.Target) error {
	return d.setPoE(ctx, t, "auto")
}

func (d *Driver) PowerOff(ctx context.Context, t power.Target) error {
	return d.setPoE(ctx, t, "off")
}

func (d *Driver) Discover(ctx context.Context, macs []net.HardwareAddr) (map[string]power.Target, error) {
	var clients []station
	if err := d.api.network(ctx, http.MethodGet, "/stat/sta", nil, &clients); err != nil {
		return nil, err
	}
	want := make(map[string]bool, len(macs))
	for _, m := range macs {
		want[m.String()] = true
	}
	out := map[string]power.Target{}
	for _, c := range clients {
		mac := strings.ToLower(c.MAC)
		if !want[mac] || c.SwitchMAC == "" || c.SwitchPort == 0 {
			continue
		}
		out[mac] = FormatTarget(strings.ToLower(c.SwitchMAC), c.SwitchPort)
	}
	return out, nil
}

// ParseTarget splits a target into the switch MAC and port number.
func ParseTarget(t power.Target) (string, int, error) {
	mac, portStr, ok := strings.Cut(string(t), "/")
	if !ok {
		return "", 0, fmt.Errorf("unifi: target %q is not <switch-mac>/<port>", t)
	}
	hw, err := net.ParseMAC(mac)
	if err != nil {
		return "", 0, fmt.Errorf("unifi: target %q: %w", t, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 {
		return "", 0, fmt.Errorf("unifi: target %q has an invalid port", t)
	}
	return hw.String(), port, nil
}

// FormatTarget builds a target from a switch MAC and port number.
func FormatTarget(switchMAC string, port int) power.Target {
	return power.Target(fmt.Sprintf("%s/%d", switchMAC, port))
}

func (d *Driver) setPoE(ctx context.Context, t power.Target, mode string) error {
	mac, port, err := ParseTarget(t)
	if err != nil {
		return err
	}
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	devs, err := d.fetchDevices(ctx)
	if err != nil {
		return err
	}
	dev, ok := findDevice(devs, mac)
	if !ok {
		return fmt.Errorf("switch %s: %w", mac, power.ErrNotFound)
	}
	p, ok := dev.port(port)
	if !ok {
		return fmt.Errorf("switch %s port %d: %w", mac, port, power.ErrNotFound)
	}
	if !p.PoE {
		return fmt.Errorf("switch %s port %d has no PoE: %w", mac, port, power.ErrUnsupported)
	}

	overrides, changed := withPoEMode(dev.Overrides, port, mode)
	if !changed && p.PoEMode == mode {
		return nil
	}
	body := map[string]any{"port_overrides": overrides}
	if err := d.api.network(ctx, http.MethodPut, "/rest/device/"+dev.ID, body, nil); err != nil {
		return err
	}
	d.invalidate()
	return d.verify(ctx, mac, port, dev.Overrides)
}

func (d *Driver) verify(ctx context.Context, mac string, port int, before []override) error {
	devs, err := d.fetchDevices(ctx)
	if err != nil {
		return fmt.Errorf("verify switch %s: %w", mac, err)
	}
	dev, ok := findDevice(devs, mac)
	if !ok {
		return fmt.Errorf("verify switch %s: %w", mac, power.ErrNotFound)
	}
	if !maps.Equal(poeModesExcept(before, port), poeModesExcept(dev.Overrides, port)) {
		return fmt.Errorf("unifi: switch %s PoE modes for ports other than %d changed unexpectedly", mac, port)
	}
	return nil
}

func (d *Driver) cachedDevices(ctx context.Context) ([]device, error) {
	d.cacheMu.Lock()
	if d.devices != nil && time.Since(d.cachedAt) < d.ttl {
		devs := d.devices
		d.cacheMu.Unlock()
		return devs, nil
	}
	d.cacheMu.Unlock()
	return d.fetchDevices(ctx)
}

func (d *Driver) fetchDevices(ctx context.Context) ([]device, error) {
	var devs []device
	if err := d.api.network(ctx, http.MethodGet, "/stat/device", nil, &devs); err != nil {
		return nil, err
	}
	d.cacheMu.Lock()
	d.devices, d.cachedAt = devs, time.Now()
	d.cacheMu.Unlock()
	return devs, nil
}

func (d *Driver) invalidate() {
	d.cacheMu.Lock()
	d.devices = nil
	d.cacheMu.Unlock()
}
