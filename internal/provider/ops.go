package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"syscall"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/tpural/possum/pkg/power"
)

const (
	msgBootTimeout = "node did not register in time"
	nodeLeaseNS    = "kube-node-lease"
)

// Reconcile refreshes every idle member from its Node and power state and
// resumes shutdowns interrupted by a restart.
func (f *Fleet) Reconcile(ctx context.Context) {
	for _, m := range f.Members() {
		g := f.groups[m.Group]
		if m.Phase == PhaseShuttingDown {
			if f.claim(m.Name) {
				f.ops.Add(1)
				go f.shutdown(context.WithoutCancel(ctx), g, m.Name)
			}
			continue
		}
		f.reconcileMember(ctx, g, m)
	}
	f.mu.Lock()
	f.ready = true
	f.mu.Unlock()
	f.observe()
	f.save(ctx)
}

func (f *Fleet) reconcileMember(ctx context.Context, g *group, snap Member) {
	if f.isBusy(snap.Name) {
		return
	}
	log := f.opts.Log.WithValues("node", snap.Name)
	m := snap
	node, err := f.getNode(ctx, m.Name)
	switch {
	case err == nil:
		if msg := f.adopt(ctx, g, &m, node); msg != "" {
			f.updateIf(snap, func(mm *Member) {
				f.setPhase(mm, PhaseUnknown, msg)
				f.refused[mm.Name] = true
			})
			return
		}
		f.updateIf(snap, func(mm *Member) {
			mm.MAC, mm.Target = m.MAC, m.Target
			delete(f.refused, mm.Name)
			f.setPhase(mm, PhaseRunning, "")
		})
		return
	case !isNotFound(err):
		log.Error(err, "get node")
		return
	}

	if m.Target == "" {
		f.updateIf(snap, func(mm *Member) { f.setPhase(mm, PhaseUnknown, "no power target; node not seen yet") })
		return
	}
	state, err := g.driver.State(ctx, m.Target)
	if err != nil {
		f.opts.Metrics.driverError(g.driver.Name())
		log.Error(err, "power state")
		return
	}
	f.updateIf(snap, func(mm *Member) {
		mm.Power = state
		switch {
		case state == power.StateOff:
			f.setPhase(mm, PhaseOff, "")
		case state == power.StateOn && mm.Phase != PhaseBooting:
			f.setPhase(mm, PhaseBooting, "powered on, waiting for node")
		case mm.Phase == PhaseBooting && time.Since(mm.Since) > g.cfg.Timeouts.Boot.Duration:
			mm.Message = msgBootTimeout
		}
	})
}

func (f *Fleet) adopt(ctx context.Context, g *group, m *Member, node *corev1.Node) string {
	if _, cp := node.Labels[controlPlaneLabel]; cp {
		return "refusing to manage a control-plane node"
	}
	if id := node.Spec.ProviderID; id != "" && id != m.ProviderID() {
		return fmt.Sprintf("node already has providerID %s", id)
	}
	if m.MAC == "" {
		m.MAC = node.Annotations[AnnotationMAC]
	}
	if m.Target == "" && m.MAC != "" {
		if t, err := f.discover(ctx, g, m.MAC); err == nil {
			m.Target = t
		} else {
			f.opts.Log.Error(err, "discover target", "node", m.Name)
		}
	}
	if err := f.patchNode(ctx, g, m, node); err != nil {
		f.opts.Log.Error(err, "patch node", "node", m.Name)
	}
	return ""
}

func (f *Fleet) patchNode(ctx context.Context, g *group, m *Member, node *corev1.Node) error {
	labels := map[string]string{}
	for k, v := range g.cfg.Labels {
		if node.Labels[k] != v {
			labels[k] = v
		}
	}
	patch := map[string]any{}
	spec := map[string]any{}
	if node.Spec.ProviderID == "" {
		spec["providerID"] = m.ProviderID()
	}
	if taints, changed := withTaints(node.Spec.Taints, g.cfg.Taints); changed {
		spec["taints"] = taints
	}
	if len(spec) > 0 {
		patch["spec"] = spec
	}
	meta := map[string]any{}
	if len(labels) > 0 {
		meta["labels"] = labels
	}
	if ann := dryRunHold(node.Annotations, g.cfg.DryRunEnabled()); len(ann) > 0 {
		meta["annotations"] = ann
	}
	if len(meta) > 0 {
		patch["metadata"] = meta
	}
	if len(patch) == 0 {
		return nil
	}
	b, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = f.opts.Client.CoreV1().Nodes().Patch(ctx, node.Name, types.MergePatchType, b, metav1.PatchOptions{})
	return err
}

// dryRunHold keeps the autoscaler from draining nodes of a dry-run group, and
// removes that hold once the group goes live, unless someone else set it.
func dryRunHold(have map[string]string, dryRun bool) map[string]any {
	held := have[annotationDryRunHold] == "true"
	switch {
	case dryRun && !held:
		return map[string]any{annotationScaleDownDisabled: "true", annotationDryRunHold: "true"}
	case !dryRun && held:
		return map[string]any{annotationScaleDownDisabled: nil, annotationDryRunHold: nil}
	}
	return nil
}

func withTaints(have, want []corev1.Taint) ([]corev1.Taint, bool) {
	out := slices.Clone(have)
	for _, w := range want {
		if !slices.ContainsFunc(have, func(h corev1.Taint) bool { return h.MatchTaint(&w) }) {
			out = append(out, w)
		}
	}
	return out, len(out) != len(have)
}

func (f *Fleet) discover(ctx context.Context, g *group, mac string) (power.Target, error) {
	d, ok := g.driver.(power.Discoverer)
	if !ok {
		return "", power.ErrUnsupported
	}
	hw, err := net.ParseMAC(mac)
	if err != nil {
		return "", err
	}
	found, err := d.Discover(ctx, []net.HardwareAddr{hw})
	if err != nil {
		return "", err
	}
	t, ok := found[hw.String()]
	if !ok {
		return "", fmt.Errorf("%s not seen by %s: %w", mac, g.driver.Name(), power.ErrNotFound)
	}
	return t, nil
}

func (f *Fleet) powerOn(ctx context.Context, g *group, name string) {
	defer f.done(name)
	m := f.member(name)
	if err := f.switchPower(ctx, g, m.Target, power.StateOn); err != nil {
		f.event(name, corev1.EventTypeWarning, "PowerOnFailed", "power on %s: %v", m.Target, err)
		f.update(name, func(mm *Member) { f.setPhase(mm, PhaseOff, err.Error()) })
	} else {
		f.event(name, corev1.EventTypeNormal, "PoweredOn", "powered on %s", m.Target)
		f.update(name, func(mm *Member) { mm.Power, mm.Message = power.StateOn, "waiting for node" })
	}
	f.save(ctx)
}

// errRetry marks a shutdown failure after power was cut; the member stays
// ShuttingDown so the next reconcile finishes the job.
type errRetry struct{ error }

func (e errRetry) Unwrap() error { return e.error }

func (f *Fleet) shutdown(ctx context.Context, g *group, name string) {
	defer f.done(name)
	err := f.runShutdown(ctx, g, name)
	var retry errRetry
	switch {
	case err == nil:
	case errors.As(err, &retry):
		f.event(name, corev1.EventTypeWarning, "ShutdownIncomplete", "%v; will retry", err)
		f.update(name, func(mm *Member) { mm.Message = err.Error() })
	default:
		f.event(name, corev1.EventTypeWarning, "ShutdownAborted", "%v; leaving power on", err)
		if cerr := f.clearShutdown(ctx, name); cerr != nil {
			f.opts.Log.Error(cerr, "clear shutdown annotation", "node", name)
		}
		f.update(name, func(mm *Member) { f.setPhase(mm, PhaseUnknown, "shutdown aborted: "+err.Error()) })
	}
	f.save(ctx)
	f.Trigger()
}

func (f *Fleet) runShutdown(ctx context.Context, g *group, name string) error {
	m := f.member(name)
	node, err := f.getNode(ctx, name)
	switch {
	case isNotFound(err):
		node = nil
	case err != nil:
		return err
	}

	if node != nil {
		if err := f.stopHost(ctx, g, m, node); err != nil {
			return err
		}
	}

	if err := f.switchPower(ctx, g, m.Target, power.StateOff); err != nil {
		return fmt.Errorf("power off %s: %w", m.Target, err)
	}
	f.event(name, corev1.EventTypeNormal, "PoweredOff", "powered off %s", m.Target)
	f.update(name, func(mm *Member) { mm.Power = power.StateOff })

	if node != nil {
		if err := f.clearShutdown(ctx, name); err != nil {
			return errRetry{fmt.Errorf("clear shutdown annotation: %w", err)}
		}
		err := f.opts.Client.CoreV1().Nodes().Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !isNotFound(err) {
			return errRetry{fmt.Errorf("delete node: %w", err)}
		}
	}
	f.update(name, func(mm *Member) { f.setPhase(mm, PhaseOff, "") })
	return nil
}

func (f *Fleet) stopHost(ctx context.Context, g *group, m Member, node *corev1.Node) error {
	if _, cp := node.Labels[controlPlaneLabel]; cp {
		return errors.New("refusing to shut down a control-plane node")
	}
	addr := kubeletAddr(node)
	if addr == "" {
		return errors.New("node has no InternalIP")
	}
	if f.opts.Probe(ctx, addr) {
		if err := f.verifyTarget(ctx, g, m); err != nil {
			return err
		}
	}
	if err := f.requestShutdown(ctx, node.Name); err != nil {
		return err
	}
	f.event(m.Name, corev1.EventTypeNormal, "ShutdownRequested", "asked the agent to power off the host")
	return f.waitOffline(ctx, g, node.Name, addr)
}

func (f *Fleet) verifyTarget(ctx context.Context, g *group, m Member) error {
	if m.MAC == "" {
		return nil
	}
	t, err := f.discover(ctx, g, m.MAC)
	switch {
	case errors.Is(err, power.ErrUnsupported):
		return nil
	case err != nil:
		return fmt.Errorf("verify target: %w", err)
	case t != m.Target:
		return fmt.Errorf("node is plugged into %s, expected %s", t, m.Target)
	}
	return nil
}

func (f *Fleet) requestShutdown(ctx context.Context, name string) error {
	at := time.Now().UTC().Format(time.RFC3339)
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, AnnotationShutdown, at)
	_, err := f.opts.Client.CoreV1().Nodes().Patch(ctx, name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

func (f *Fleet) clearShutdown(ctx context.Context, name string) error {
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:null}}}`, AnnotationShutdown)
	_, err := f.opts.Client.CoreV1().Nodes().Patch(ctx, name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	if isNotFound(err) {
		return nil
	}
	return err
}

// waitOffline returns once the host has both stopped answering on the network
// and stopped renewing its node Lease for the offline grace period.
func (f *Fleet) waitOffline(ctx context.Context, g *group, name, addr string) error {
	t := g.cfg.Timeouts
	ctx, cancel := context.WithTimeout(ctx, t.Shutdown.Duration)
	defer cancel()
	var silentSince time.Time
	for {
		if f.opts.Probe(ctx, addr) {
			silentSince = time.Time{}
		} else if silentSince.IsZero() {
			silentSince = time.Now()
		}
		if !silentSince.IsZero() && time.Since(silentSince) >= t.OfflineGrace.Duration {
			stale, err := f.leaseStale(ctx, name, t.OfflineGrace.Duration)
			if err != nil {
				f.opts.Log.Error(err, "read node lease", "node", name)
			}
			if stale {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return errors.New("host still alive after shutdown timeout")
		case <-time.After(f.opts.PollInterval):
		}
	}
}

func (f *Fleet) leaseStale(ctx context.Context, name string, grace time.Duration) (bool, error) {
	lease, err := f.opts.Client.CoordinationV1().Leases(nodeLeaseNS).Get(ctx, name, metav1.GetOptions{})
	if isNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return time.Since(renewTime(lease)) >= grace, nil
}

func renewTime(l *coordinationv1.Lease) time.Time {
	if l.Spec.RenewTime == nil {
		return time.Time{}
	}
	return l.Spec.RenewTime.Time
}

func (f *Fleet) switchPower(ctx context.Context, g *group, t power.Target, want power.State) error {
	f.powerMu.Lock()
	defer f.powerMu.Unlock()
	op := g.driver.PowerOn
	if want == power.StateOff {
		op = g.driver.PowerOff
	}
	if err := op(ctx, t); err != nil {
		f.opts.Metrics.driverError(g.driver.Name())
		return err
	}
	f.opts.Metrics.powerAction(g.cfg.Name, string(want))

	ctx, cancel := context.WithTimeout(ctx, g.cfg.Timeouts.PowerSettle.Duration)
	defer cancel()
	for {
		s, err := g.driver.State(ctx, t)
		if err == nil && s == want {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("power state did not become %s (last %s, %v)", want, s, err)
		case <-time.After(f.opts.PollInterval):
		}
	}
}

func (f *Fleet) member(name string) Member {
	f.mu.Lock()
	defer f.mu.Unlock()
	return *f.members[name]
}

func (f *Fleet) getNode(ctx context.Context, name string) (*corev1.Node, error) {
	return f.opts.Client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
}

func (f *Fleet) observe() {
	counts := map[[2]string]int{}
	for _, m := range f.Members() {
		counts[[2]string{m.Group, string(m.Phase)}]++
	}
	for _, g := range f.order {
		for _, p := range []Phase{PhaseUnknown, PhaseOff, PhaseBooting, PhaseRunning, PhaseShuttingDown} {
			f.opts.Metrics.nodes.WithLabelValues(g, string(p)).Set(float64(counts[[2]string{g, string(p)}]))
		}
	}
}

func kubeletAddr(node *corev1.Node) string {
	port := int(node.Status.DaemonEndpoints.KubeletEndpoint.Port)
	if port == 0 {
		port = 10250
	}
	for _, a := range node.Status.Addresses {
		if a.Type == corev1.NodeInternalIP {
			return net.JoinHostPort(a.Address, strconv.Itoa(port))
		}
	}
	return ""
}

// probeKubelet reports whether the host behind addr is up. A refused
// connection still means the host answered.
func probeKubelet(ctx context.Context, addr string) bool {
	d := net.Dialer{Timeout: 3 * time.Second}
	c, err := d.DialContext(ctx, "tcp", addr)
	if err == nil {
		_ = c.Close()
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED)
}

func isNotFound(err error) bool { return apierrors.IsNotFound(err) }
