// Package provider implements the Cluster Autoscaler externalgrpc cloud provider.
package provider

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"

	"github.com/tpural/possum/internal/config"
	"github.com/tpural/possum/pkg/power"
)

const (
	ProviderIDPrefix       = "possum://"
	AnnotationShutdown     = "possum.dev/shutdown"
	AnnotationShutdownMode = "possum.dev/shutdown-mode"
	AnnotationMAC          = "possum.dev/mac"
	ShutdownModeSuspend    = "suspend"
	TaintAsleep            = "possum.dev/asleep"

	annotationScaleDownDisabled = "cluster-autoscaler.kubernetes.io/scale-down-disabled"
	annotationDryRunHold        = "possum.dev/dry-run-hold"

	controlPlaneLabel = "node-role.kubernetes.io/control-plane"
)

// Phase is where a member is in its power lifecycle.
type Phase string

const (
	PhaseUnknown      Phase = "Unknown"
	PhaseOff          Phase = "Off"
	PhaseBooting      Phase = "Booting"
	PhaseRunning      Phase = "Running"
	PhaseShuttingDown Phase = "ShuttingDown"
)

var ErrDryRun = errors.New("dry run: no power changes are made")

// Member is a machine managed by POSSUM.
type Member struct {
	Group   string       `json:"group"`
	Name    string       `json:"name"`
	Target  power.Target `json:"target,omitempty"`
	MAC     string       `json:"mac,omitempty"`
	Phase   Phase        `json:"phase"`
	Since   time.Time    `json:"since"`
	Message string       `json:"message,omitempty"`
	Power   power.State  `json:"power,omitempty"`
}

// ProviderID is the spec.providerID POSSUM assigns to the member's Node.
func (m Member) ProviderID() string { return ProviderIDPrefix + m.Group + "/" + m.Name }

type group struct {
	cfg    config.NodeGroup
	driver power.Driver
}

// Options configures a Fleet.
type Options struct {
	Config       *config.Config
	Drivers      map[string]power.Driver
	Client       kubernetes.Interface
	Namespace    string
	Recorder     record.EventRecorder
	Metrics      *Metrics
	Log          logr.Logger
	Probe        func(ctx context.Context, addr string) bool
	PollInterval time.Duration
}

// Fleet tracks every managed member and drives power transitions.
type Fleet struct {
	opts   Options
	groups map[string]*group
	order  []string
	state  *stateStore

	mu      sync.Mutex
	members map[string]*Member
	busy    map[string]bool
	refused map[string]bool
	ready   bool

	saveMu sync.Mutex

	powerMu sync.Mutex
	ops     sync.WaitGroup
	wake    chan struct{}
}

// NewFleet builds a Fleet. Drivers must contain one driver per node group.
func NewFleet(opts Options) (*Fleet, error) {
	if opts.Probe == nil {
		opts.Probe = probeKubelet
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = 5 * time.Second
	}
	if opts.Metrics == nil {
		opts.Metrics = NewMetrics(nil)
	}
	f := &Fleet{
		opts:    opts,
		groups:  map[string]*group{},
		members: map[string]*Member{},
		busy:    map[string]bool{},
		refused: map[string]bool{},
		state:   &stateStore{client: opts.Client, namespace: opts.Namespace},
		wake:    make(chan struct{}, 1),
	}
	for _, g := range opts.Config.NodeGroups {
		d, ok := opts.Drivers[g.Name]
		if !ok {
			return nil, fmt.Errorf("no driver for node group %s", g.Name)
		}
		f.groups[g.Name] = &group{cfg: g, driver: d}
		f.order = append(f.order, g.Name)
		for _, n := range g.Nodes {
			f.members[n.Name] = &Member{
				Group:  g.Name,
				Name:   n.Name,
				Target: power.Target(n.Target),
				MAC:    strings.ToLower(n.MAC),
				Phase:  PhaseUnknown,
			}
		}
	}
	return f, nil
}

// DryRun reports whether power changes are suppressed for the group.
func (f *Fleet) DryRun(group string) bool {
	g, ok := f.groups[group]
	return !ok || g.cfg.DryRunEnabled()
}

// Ready reports whether the first reconcile has completed.
func (f *Fleet) Ready() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ready
}

// Run reconciles until ctx is cancelled, then waits for in-flight operations.
func (f *Fleet) Run(ctx context.Context, interval time.Duration) error {
	if err := f.restore(ctx); err != nil {
		return err
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		f.Reconcile(ctx)
		select {
		case <-ctx.Done():
			f.ops.Wait()
			return nil
		case <-t.C:
		case <-f.wake:
		}
	}
}

// Trigger requests a reconcile soon.
func (f *Fleet) Trigger() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

// Members returns a snapshot of all members in config order.
func (f *Fleet) Members() []Member {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Member
	for _, name := range f.order {
		for _, n := range f.groups[name].cfg.Nodes {
			out = append(out, *f.members[n.Name])
		}
	}
	return out
}

// Groups returns the configured node groups in order.
func (f *Fleet) Groups() []config.NodeGroup {
	out := make([]config.NodeGroup, 0, len(f.order))
	for _, name := range f.order {
		out = append(out, f.groups[name].cfg)
	}
	return out
}

func (f *Fleet) group(name string) (*group, error) {
	g, ok := f.groups[name]
	if !ok {
		return nil, fmt.Errorf("unknown node group %q", name)
	}
	return g, nil
}

// TargetSize is the number of members that are on or coming up.
func (f *Fleet) TargetSize(groupName string) (int, error) {
	if _, err := f.group(groupName); err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, m := range f.members {
		if m.Group == groupName && (m.Phase == PhaseRunning || m.Phase == PhaseBooting) {
			n++
		}
	}
	return n, nil
}

// GroupFor returns the group a node belongs to, or "" if it is not managed.
func (f *Fleet) GroupFor(name, providerID string) string {
	if rest, ok := strings.CutPrefix(providerID, ProviderIDPrefix); ok {
		if g, _, ok := strings.Cut(rest, "/"); ok {
			if _, known := f.groups[g]; known {
				return g
			}
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if m, ok := f.members[name]; ok && !f.refused[name] {
		return m.Group
	}
	return ""
}

// Resolve maps a node reported by the autoscaler to a member name. Nodes that
// never registered arrive with the instance ID as both name and providerID.
func (f *Fleet) Resolve(name, providerID string) string {
	if rest, ok := strings.CutPrefix(providerID, ProviderIDPrefix); ok {
		if _, member, ok := strings.Cut(rest, "/"); ok {
			return member
		}
	}
	return name
}

// IncreaseSize powers on delta members that are off.
func (f *Fleet) IncreaseSize(ctx context.Context, groupName string, delta int) error {
	g, err := f.group(groupName)
	if err != nil {
		return err
	}
	if delta <= 0 {
		return fmt.Errorf("delta must be positive, got %d", delta)
	}
	f.mu.Lock()
	var picked []*Member
	size := 0
	for _, n := range g.cfg.Nodes {
		m := f.members[n.Name]
		switch {
		case m.Phase == PhaseRunning || m.Phase == PhaseBooting:
			size++
		case m.Phase == PhaseOff && m.Target != "" && !f.busy[m.Name] && len(picked) < delta:
			picked = append(picked, m)
		}
	}
	if size+delta > g.cfg.MaxSize {
		f.mu.Unlock()
		return fmt.Errorf("size %d + %d exceeds max %d", size, delta, g.cfg.MaxSize)
	}
	if len(picked) < delta {
		f.mu.Unlock()
		return fmt.Errorf("only %d of %d requested nodes are off and available", len(picked), delta)
	}
	if g.cfg.DryRunEnabled() {
		f.mu.Unlock()
		for _, m := range picked {
			f.event(m.Name, corev1.EventTypeNormal, "DryRunPowerOn", "would power on %s", m.Target)
		}
		return ErrDryRun
	}
	for _, m := range picked {
		f.setPhase(m, PhaseBooting, "powering on")
		f.busy[m.Name] = true
	}
	f.mu.Unlock()

	for _, m := range picked {
		f.ops.Add(1)
		go f.powerOn(context.WithoutCancel(ctx), g, m.Name)
	}
	f.save(ctx)
	return nil
}

// Wake powers on one member of the group unless one is already on or coming up,
// so a workload that knows it needs the group can skip the autoscaler's delay.
func (f *Fleet) Wake(ctx context.Context, groupName string) (bool, error) {
	size, err := f.TargetSize(groupName)
	if err != nil || size > 0 {
		return false, err
	}
	if err := f.IncreaseSize(ctx, groupName, 1); err != nil {
		return false, err
	}
	return true, nil
}

// DeleteNodes gracefully shuts down and powers off the named members.
func (f *Fleet) DeleteNodes(ctx context.Context, groupName string, names []string) error {
	g, err := f.group(groupName)
	if err != nil {
		return err
	}
	f.mu.Lock()
	size := 0
	for _, n := range g.cfg.Nodes {
		if p := f.members[n.Name].Phase; p == PhaseRunning || p == PhaseBooting {
			size++
		}
	}
	if size-len(names) < g.cfg.MinSize {
		f.mu.Unlock()
		return fmt.Errorf("deleting %d nodes would go below min size %d", len(names), g.cfg.MinSize)
	}
	var picked []*Member
	for _, name := range names {
		m, ok := f.members[name]
		switch {
		case !ok || m.Group != groupName:
			f.mu.Unlock()
			return fmt.Errorf("node %s is not in group %s", name, groupName)
		case m.Phase != PhaseRunning && m.Phase != PhaseBooting:
			f.mu.Unlock()
			return fmt.Errorf("node %s is %s", name, m.Phase)
		case m.Target == "":
			f.mu.Unlock()
			return fmt.Errorf("node %s has no power target", name)
		case f.busy[name]:
			f.mu.Unlock()
			return fmt.Errorf("node %s has an operation in progress", name)
		}
		picked = append(picked, m)
	}
	if g.cfg.DryRunEnabled() {
		f.mu.Unlock()
		for _, m := range picked {
			f.event(m.Name, corev1.EventTypeNormal, "DryRunPowerOff", "would shut down and power off %s", m.Target)
		}
		return ErrDryRun
	}
	for _, m := range picked {
		f.setPhase(m, PhaseShuttingDown, "shutdown requested")
		f.busy[m.Name] = true
	}
	f.mu.Unlock()

	for _, m := range picked {
		f.ops.Add(1)
		go f.shutdown(context.WithoutCancel(ctx), g, m.Name)
	}
	f.save(ctx)
	return nil
}

// DecreaseTargetSize powers off members that were powered on but never registered.
func (f *Fleet) DecreaseTargetSize(ctx context.Context, groupName string, delta int) error {
	if delta >= 0 {
		return fmt.Errorf("delta must be negative, got %d", delta)
	}
	if _, err := f.group(groupName); err != nil {
		return err
	}
	var names []string
	for _, m := range f.Members() {
		if m.Group == groupName && m.Phase == PhaseBooting && len(names) < -delta {
			if _, err := f.getNode(ctx, m.Name); isNotFound(err) {
				names = append(names, m.Name)
			}
		}
	}
	if len(names) < -delta {
		return fmt.Errorf("only %d unregistered nodes can be removed, asked for %d", len(names), -delta)
	}
	return f.DeleteNodes(ctx, groupName, names)
}

func (f *Fleet) setPhase(m *Member, p Phase, msg string) {
	if m.Phase != p {
		m.Since = time.Now()
	}
	m.Phase, m.Message = p, msg
}

func (f *Fleet) update(name string, fn func(m *Member)) {
	f.mu.Lock()
	fn(f.members[name])
	f.mu.Unlock()
}

// updateIf applies fn only when no operation has claimed the member and its
// phase has not moved since snap was taken.
func (f *Fleet) updateIf(snap Member, fn func(m *Member)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := f.members[snap.Name]
	if f.busy[snap.Name] || m.Phase != snap.Phase || !m.Since.Equal(snap.Since) {
		return
	}
	fn(m)
}

func (f *Fleet) claim(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.busy[name] {
		return false
	}
	f.busy[name] = true
	return true
}

func (f *Fleet) isBusy(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.busy[name]
}

func (f *Fleet) done(name string) {
	f.mu.Lock()
	delete(f.busy, name)
	f.mu.Unlock()
	f.ops.Done()
}

func (f *Fleet) event(node, kind, reason, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	f.opts.Log.Info(msg, "node", node, "reason", reason)
	if f.opts.Recorder != nil {
		ref := &corev1.ObjectReference{Kind: "Node", Name: node, UID: types.UID(node)}
		f.opts.Recorder.Event(ref, kind, reason, msg)
	}
}
