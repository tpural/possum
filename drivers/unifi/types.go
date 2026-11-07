package unifi

import (
	"maps"
	"strings"

	"github.com/tpural/possum/pkg/power"
)

type device struct {
	ID        string     `json:"_id"`
	MAC       string     `json:"mac"`
	Name      string     `json:"name"`
	Ports     []port     `json:"port_table"`
	Overrides []override `json:"port_overrides"`
}

type port struct {
	Index   int    `json:"port_idx"`
	PoEMode string `json:"poe_mode"`
	PoE     bool   `json:"port_poe"`
}

type override map[string]any

type station struct {
	MAC        string `json:"mac"`
	SwitchMAC  string `json:"sw_mac"`
	SwitchPort int    `json:"sw_port"`
}

func (p port) state() power.State {
	switch p.PoEMode {
	case "off":
		return power.StateOff
	case "":
		return power.StateUnknown
	default:
		return power.StateOn
	}
}

func (d device) port(idx int) (port, bool) {
	for _, p := range d.Ports {
		if p.Index == idx {
			return p, true
		}
	}
	return port{}, false
}

func findDevice(devs []device, mac string) (device, bool) {
	for _, d := range devs {
		if strings.EqualFold(d.MAC, mac) {
			return d, true
		}
	}
	return device{}, false
}

func (o override) index() int {
	switch v := o["port_idx"].(type) {
	case float64:
		return int(v)
	case int:
		return v
	}
	return 0
}

func withPoEMode(in []override, idx int, mode string) ([]override, bool) {
	out := make([]override, 0, len(in)+1)
	found, changed := false, false
	for _, o := range in {
		c := maps.Clone(o)
		if c.index() == idx {
			found = true
			if c["poe_mode"] != mode {
				c["poe_mode"] = mode
				changed = true
			}
		}
		out = append(out, c)
	}
	if !found {
		out = append(out, override{"port_idx": idx, "poe_mode": mode})
		changed = true
	}
	return out, changed
}

func poeModesExcept(in []override, idx int) map[int]any {
	out := map[int]any{}
	for _, o := range in {
		if i := o.index(); i != idx {
			out[i] = o["poe_mode"]
		}
	}
	return out
}
