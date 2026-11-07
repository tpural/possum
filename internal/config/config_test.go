package config

import (
	"strings"
	"testing"
	"time"
)

const valid = `
apiVersion: possum.dev/v1alpha1
kind: PossumConfig
dryRun: false
nodeGroups:
- name: pis
  minSize: 1
  maxSize: 2
  driver:
    name: unifi
    config:
      url: https://unifi.example
  template:
    cpu: "4"
    memory: 8Gi
  nodes:
  - name: pi-1
    target: 74:ac:b9:55:27:a0/1
  - name: pi-2
    mac: dc:a6:32:da:74:09
`

func TestParseValid(t *testing.T) {
	c, err := Parse([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	if c.DryRunEnabled() {
		t.Error("dryRun should be false")
	}
	if c.Timeouts.Shutdown.Duration != 5*time.Minute {
		t.Errorf("shutdown timeout default = %v", c.Timeouts.Shutdown.Duration)
	}
	g := c.NodeGroups[0]
	if g.Template.Pods != 110 || g.Template.Memory.String() != "8Gi" {
		t.Errorf("template = %+v", g.Template)
	}
	if !strings.Contains(string(g.Driver.Config), "unifi.example") {
		t.Errorf("driver config = %s", g.Driver.Config)
	}
}

func TestDryRunDefaultsOn(t *testing.T) {
	c, err := Parse([]byte(strings.Replace(valid, "dryRun: false\n", "", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if !c.DryRunEnabled() {
		t.Error("dryRun should default to true")
	}
}

func TestParseInvalid(t *testing.T) {
	tests := map[string]struct {
		from, to string
		want     string
	}{
		"unknown field":   {"dryRun: false", "dryRun: false\nbogus: 1", "unknown field"},
		"wrong kind":      {"kind: PossumConfig", "kind: Other", "apiVersion/kind"},
		"max over nodes":  {"maxSize: 2", "maxSize: 3", "maxSize"},
		"min over max":    {"minSize: 1", "minSize: 3", "maxSize"},
		"no driver":       {"name: unifi", "name: \"\"", "driver.name"},
		"bad mac":         {"mac: dc:a6:32:da:74:09", "mac: nope", "node pi-2"},
		"duplicate node":  {"name: pi-2", "name: pi-1", "already belongs"},
		"template no cpu": {`cpu: "4"`, `cpu: "0"`, "template needs"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(strings.Replace(valid, tt.from, tt.to, 1)))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestGroupOverrides(t *testing.T) {
	c, err := Parse([]byte(strings.Replace(valid, "  minSize: 1\n", "  minSize: 1\n  dryRun: true\n  timeouts: {powerSettle: 3m}\n", 1)))
	if err != nil {
		t.Fatal(err)
	}
	g := c.NodeGroups[0]
	if !g.DryRunEnabled() || c.DryRunEnabled() {
		t.Errorf("dryRun group=%v global=%v", g.DryRunEnabled(), c.DryRunEnabled())
	}
	if g.Timeouts.PowerSettle.Duration != 3*time.Minute || g.Timeouts.Shutdown.Duration != 5*time.Minute {
		t.Errorf("timeouts = %+v", g.Timeouts)
	}

	c, err = Parse([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	if c.NodeGroups[0].DryRunEnabled() {
		t.Error("group should inherit dryRun: false")
	}
}

func TestInvalidTaint(t *testing.T) {
	_, err := Parse([]byte(strings.Replace(valid, "  minSize: 1\n", "  minSize: 1\n  taints: [{key: gpu, effect: Sometimes}]\n", 1)))
	if err == nil || !strings.Contains(err.Error(), "invalid effect") {
		t.Fatalf("err = %v", err)
	}
}
