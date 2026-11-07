// Package config loads and validates the POSSUM configuration file.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

const (
	APIVersion = "possum.dev/v1alpha1"
	Kind       = "PossumConfig"
)

// Config is the top-level PossumConfig document.
type Config struct {
	APIVersion string      `json:"apiVersion"`
	Kind       string      `json:"kind"`
	DryRun     *bool       `json:"dryRun,omitempty"`
	Timeouts   Timeouts    `json:"timeouts"`
	NodeGroups []NodeGroup `json:"nodeGroups"`
}

// Timeouts bounds each step of powering a node up or down.
type Timeouts struct {
	Boot         metav1.Duration `json:"boot"`
	Shutdown     metav1.Duration `json:"shutdown"`
	OfflineGrace metav1.Duration `json:"offlineGrace"`
	PowerSettle  metav1.Duration `json:"powerSettle"`
}

// NodeGroup is a set of interchangeable nodes sharing one power driver.
// DryRun and Timeouts fall back to the top-level values when unset. Suspend
// puts members to sleep instead of powering them off; their Nodes stay registered.
type NodeGroup struct {
	Name     string   `json:"name"`
	MinSize  int      `json:"minSize"`
	MaxSize  int      `json:"maxSize"`
	DryRun   *bool    `json:"dryRun,omitempty"`
	Timeouts Timeouts `json:"timeouts"`
	Suspend  bool     `json:"suspend,omitempty"`

	ScaleDownUnneededTime metav1.Duration `json:"scaleDownUnneededTime,omitempty"`

	Driver   Driver            `json:"driver"`
	Template *Template         `json:"template,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
	Taints   []corev1.Taint    `json:"taints,omitempty"`
	Nodes    []Node            `json:"nodes"`
}

// Driver selects a registered power driver and passes it its settings.
type Driver struct {
	Name   string          `json:"name"`
	Config json.RawMessage `json:"config,omitempty"`
}

// Template describes a node of the group for scale-up simulation when no
// member is running to copy from.
type Template struct {
	CPU       resource.Quantity   `json:"cpu"`
	Memory    resource.Quantity   `json:"memory"`
	Pods      int64               `json:"pods,omitempty"`
	Resources corev1.ResourceList `json:"resources,omitempty"`
	Labels    map[string]string   `json:"labels,omitempty"`
}

// Node is a single machine. Target is optional when the driver can discover it
// from the node's MAC address.
type Node struct {
	Name   string `json:"name"`
	Target string `json:"target,omitempty"`
	MAC    string `json:"mac,omitempty"`
}

// DryRunEnabled reports whether POSSUM should only log what it would do by default.
func (c *Config) DryRunEnabled() bool { return c.DryRun == nil || *c.DryRun }

// DryRunEnabled reports whether the group only logs what it would do.
func (g *NodeGroup) DryRunEnabled() bool { return g.DryRun == nil || *g.DryRun }

// Load reads and validates the config file at path.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b)
}

// Parse decodes a YAML or JSON document, applies defaults and validates it.
func Parse(b []byte) (*Config, error) {
	js, err := yaml.YAMLToJSON(b)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(js))
	dec.DisallowUnknownFields()
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	c.setDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) setDefaults() {
	def := func(d *metav1.Duration, v time.Duration) {
		if d.Duration == 0 {
			d.Duration = v
		}
	}
	def(&c.Timeouts.Boot, 10*time.Minute)
	def(&c.Timeouts.Shutdown, 5*time.Minute)
	def(&c.Timeouts.OfflineGrace, 20*time.Second)
	def(&c.Timeouts.PowerSettle, time.Minute)
	for i := range c.NodeGroups {
		g := &c.NodeGroups[i]
		if g.DryRun == nil {
			g.DryRun = c.DryRun
		}
		def(&g.Timeouts.Boot, c.Timeouts.Boot.Duration)
		def(&g.Timeouts.Shutdown, c.Timeouts.Shutdown.Duration)
		def(&g.Timeouts.OfflineGrace, c.Timeouts.OfflineGrace.Duration)
		def(&g.Timeouts.PowerSettle, c.Timeouts.PowerSettle.Duration)
		if g.Template != nil && g.Template.Pods == 0 {
			g.Template.Pods = 110
		}
	}
}

var nameRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`)

// Validate reports every problem with the configuration.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.APIVersion != APIVersion || c.Kind != Kind {
		add("apiVersion/kind must be %s/%s", APIVersion, Kind)
	}
	if len(c.NodeGroups) == 0 {
		add("at least one node group is required")
	}
	groups := map[string]bool{}
	nodes := map[string]string{}
	for _, g := range c.NodeGroups {
		p := fmt.Sprintf("nodeGroups[%s]", g.Name)
		if !nameRE.MatchString(g.Name) {
			add("%s: invalid name", p)
		}
		if groups[g.Name] {
			add("%s: duplicate name", p)
		}
		groups[g.Name] = true
		if g.Driver.Name == "" {
			add("%s: driver.name is required", p)
		}
		if g.MinSize < 0 || g.MaxSize < g.MinSize || g.MaxSize > len(g.Nodes) {
			add("%s: need 0 <= minSize <= maxSize <= len(nodes)", p)
		}
		if t := g.Template; t != nil && (t.CPU.IsZero() || t.Memory.IsZero()) {
			add("%s: template needs cpu and memory", p)
		}
		for _, t := range g.Taints {
			switch {
			case t.Key == "":
				add("%s: taint needs a key", p)
			case t.Effect != corev1.TaintEffectNoSchedule && t.Effect != corev1.TaintEffectPreferNoSchedule && t.Effect != corev1.TaintEffectNoExecute:
				add("%s: taint %s has invalid effect %q", p, t.Key, t.Effect)
			}
		}
		for _, n := range g.Nodes {
			if !nameRE.MatchString(n.Name) {
				add("%s: invalid node name %q", p, n.Name)
			}
			if other, dup := nodes[n.Name]; dup {
				add("%s: node %s already belongs to %s", p, n.Name, other)
			}
			nodes[n.Name] = g.Name
			if n.MAC != "" {
				if _, err := net.ParseMAC(n.MAC); err != nil {
					add("%s: node %s: %v", p, n.Name, err)
				}
			}
		}
	}
	return errors.Join(errs...)
}
