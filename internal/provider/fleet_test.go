package provider

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/tpural/possum/drivers/fake"
	"github.com/tpural/possum/internal/config"
	"github.com/tpural/possum/internal/externalgrpc/protos"
	"github.com/tpural/possum/pkg/power"
)

const testConfig = `
apiVersion: possum.dev/v1alpha1
kind: PossumConfig
dryRun: false
timeouts:
  shutdown: 300ms
  offlineGrace: 10ms
  powerSettle: 1s
nodeGroups:
- name: pis
  minSize: 1
  maxSize: 3
  driver: {name: fake}
  labels: {possum.dev/group: pis}
  template: {cpu: "4", memory: 8Gi}
  nodes:
  - {name: pi-1, target: sw/1}
  - {name: pi-2, target: sw/2}
  - {name: pi-3, target: sw/3}
`

type harness struct {
	t      *testing.T
	fleet  *Fleet
	client *k8sfake.Clientset
	driver *fake.Driver
	online atomic.Bool
	lease  atomic.Bool
}

func newHarness(t *testing.T, cfgText string, outlets map[power.Target]power.State, nodes ...runtime.Object) *harness {
	t.Helper()
	cfg, err := config.Parse([]byte(cfgText))
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, client: k8sfake.NewClientset(nodes...), driver: fake.New(fake.Config{Outlets: outlets})}
	h.online.Store(true)
	h.lease.Store(true)
	h.client.PrependReactor("get", "leases", func(a k8stesting.Action) (bool, runtime.Object, error) {
		renew := time.Now().Add(-time.Hour)
		if h.lease.Load() {
			renew = time.Now()
		}
		name := a.(k8stesting.GetAction).GetName()
		return true, &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: nodeLeaseNS},
			Spec:       coordinationv1.LeaseSpec{RenewTime: &metav1.MicroTime{Time: renew}},
		}, nil
	})
	h.fleet, err = NewFleet(Options{
		Config:       cfg,
		Drivers:      map[string]power.Driver{"pis": h.driver},
		Client:       h.client,
		Namespace:    "possum",
		Log:          logr.Discard(),
		Probe:        func(context.Context, string) bool { return h.online.Load() },
		PollInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.fleet.restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.fleet.Reconcile(context.Background())
	h.fleet.ops.Wait()
	return h
}

func (h *harness) phase(name string) Phase { return h.fleet.member(name).Phase }

func (h *harness) powerDownOnRequest() {
	h.client.PrependReactor("patch", "nodes", func(a k8stesting.Action) (bool, runtime.Object, error) {
		p := string(a.(k8stesting.PatchAction).GetPatch())
		if strings.Contains(p, AnnotationShutdown) && !strings.Contains(p, "null") {
			h.online.Store(false)
			h.lease.Store(false)
		}
		return false, nil, nil
	})
}

func (h *harness) node(name string) (*corev1.Node, error) {
	return h.client.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
}

func node(name string, labels map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.1"}},
			Capacity:  corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
		},
	}
}

func allOn() map[power.Target]power.State {
	return map[power.Target]power.State{"sw/1": power.StateOn, "sw/2": power.StateOn, "sw/3": power.StateOff}
}

func TestReconcileAdoptsNodes(t *testing.T) {
	h := newHarness(t, testConfig, allOn(), node("pi-1", nil), node("pi-2", nil))

	if p := h.phase("pi-1"); p != PhaseRunning {
		t.Fatalf("pi-1 phase = %s", p)
	}
	if p := h.phase("pi-3"); p != PhaseOff {
		t.Fatalf("pi-3 phase = %s", p)
	}
	n, _ := h.node("pi-1")
	if n.Spec.ProviderID != "possum://pis/pi-1" || n.Labels["possum.dev/group"] != "pis" {
		t.Fatalf("node not adopted: providerID=%q labels=%v", n.Spec.ProviderID, n.Labels)
	}
	if size, _ := h.fleet.TargetSize("pis"); size != 2 {
		t.Fatalf("target size = %d", size)
	}
}

func TestReconcileRefusesControlPlane(t *testing.T) {
	h := newHarness(t, testConfig, allOn(), node("pi-1", map[string]string{controlPlaneLabel: ""}))

	if p := h.phase("pi-1"); p != PhaseUnknown {
		t.Fatalf("phase = %s, want Unknown", p)
	}
	if n, _ := h.node("pi-1"); n.Spec.ProviderID != "" {
		t.Fatal("control-plane node was patched")
	}
	if err := h.fleet.DeleteNodes(context.Background(), "pis", []string{"pi-1"}); err == nil {
		t.Fatal("DeleteNodes accepted a control-plane node")
	}
}

func TestIncreaseSize(t *testing.T) {
	h := newHarness(t, testConfig, allOn(), node("pi-1", nil), node("pi-2", nil))

	if err := h.fleet.IncreaseSize(context.Background(), "pis", 1); err != nil {
		t.Fatal(err)
	}
	h.fleet.ops.Wait()
	if p := h.phase("pi-3"); p != PhaseBooting {
		t.Fatalf("pi-3 phase = %s", p)
	}
	if !slices.Equal(h.driver.Calls(), []string{"on:sw/3"}) {
		t.Fatalf("calls = %v", h.driver.Calls())
	}
	if err := h.fleet.IncreaseSize(context.Background(), "pis", 1); err == nil {
		t.Fatal("IncreaseSize beyond max succeeded")
	}
}

func TestDryRunMakesNoChanges(t *testing.T) {
	cfg := strings.Replace(testConfig, "dryRun: false", "dryRun: true", 1)
	h := newHarness(t, cfg, allOn(), node("pi-1", nil), node("pi-2", nil))
	ctx := context.Background()

	if err := h.fleet.IncreaseSize(ctx, "pis", 1); !errors.Is(err, ErrDryRun) {
		t.Fatalf("IncreaseSize err = %v", err)
	}
	if err := h.fleet.DeleteNodes(ctx, "pis", []string{"pi-2"}); !errors.Is(err, ErrDryRun) {
		t.Fatalf("DeleteNodes err = %v", err)
	}
	if calls := h.driver.Calls(); len(calls) != 0 {
		t.Fatalf("calls = %v", calls)
	}
}

func TestDeleteNodesShutsDownGracefully(t *testing.T) {
	h := newHarness(t, testConfig, allOn(), node("pi-1", nil), node("pi-2", nil))
	ctx := context.Background()
	h.powerDownOnRequest()

	if err := h.fleet.DeleteNodes(ctx, "pis", []string{"pi-2"}); err != nil {
		t.Fatal(err)
	}
	h.fleet.ops.Wait()

	if p := h.phase("pi-2"); p != PhaseOff {
		t.Fatalf("phase = %s (%s)", p, h.fleet.member("pi-2").Message)
	}
	if !slices.Equal(h.driver.Calls(), []string{"off:sw/2"}) {
		t.Fatalf("calls = %v", h.driver.Calls())
	}
	if _, err := h.node("pi-2"); !isNotFound(err) {
		t.Fatalf("node still exists: %v", err)
	}
}

func TestShutdownTimeoutLeavesPowerOn(t *testing.T) {
	h := newHarness(t, testConfig, allOn(), node("pi-1", nil), node("pi-2", nil))

	if err := h.fleet.DeleteNodes(context.Background(), "pis", []string{"pi-2"}); err != nil {
		t.Fatal(err)
	}
	h.fleet.ops.Wait()

	if calls := h.driver.Calls(); len(calls) != 0 {
		t.Fatalf("power was cut: %v", calls)
	}
	n, err := h.node("pi-2")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := n.Annotations[AnnotationShutdown]; ok {
		t.Fatal("shutdown annotation not cleared")
	}
}

func TestDeleteNodesRespectsMinSize(t *testing.T) {
	h := newHarness(t, testConfig, allOn(), node("pi-1", nil), node("pi-2", nil))

	err := h.fleet.DeleteNodes(context.Background(), "pis", []string{"pi-1", "pi-2"})
	if err == nil {
		t.Fatal("DeleteNodes went below min size")
	}
}

func TestRestartResumesShutdown(t *testing.T) {
	h := newHarness(t, testConfig, allOn(), node("pi-1", nil), node("pi-2", nil))
	h.fleet.update("pi-2", func(m *Member) { m.Phase = PhaseShuttingDown })
	h.fleet.save(context.Background())

	h.lease.Store(false)
	h2 := &harness{t: t, client: h.client, driver: h.driver}
	h2.online.Store(false)
	cfg, _ := config.Parse([]byte(testConfig))
	var err error
	h2.fleet, err = NewFleet(Options{
		Config: cfg, Drivers: map[string]power.Driver{"pis": h.driver}, Client: h.client,
		Namespace: "possum", Log: logr.Discard(), PollInterval: 5 * time.Millisecond,
		Probe: func(context.Context, string) bool { return h2.online.Load() },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := h2.fleet.restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	h2.fleet.Reconcile(context.Background())
	h2.fleet.ops.Wait()

	if p := h2.phase("pi-2"); p != PhaseOff {
		t.Fatalf("phase = %s", p)
	}
}

func TestServer(t *testing.T) {
	h := newHarness(t, testConfig, allOn(), node("pi-1", nil), node("pi-2", nil))
	s := NewServer(h.fleet)
	ctx := context.Background()

	nodes, err := s.NodeGroupNodes(ctx, &protos.NodeGroupNodesRequest{Id: "pis"})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes.Instances) != 2 || nodes.Instances[0].Id != "possum://pis/pi-1" {
		t.Fatalf("instances = %v", nodes.Instances)
	}

	g, _ := s.NodeGroupForNode(ctx, &protos.NodeGroupForNodeRequest{Node: &protos.ExternalGrpcNode{Name: "mm01"}})
	if g.NodeGroup.Id != "" {
		t.Fatalf("unmanaged node got group %q", g.NodeGroup.Id)
	}

	tmpl, err := s.NodeGroupTemplateNodeInfo(ctx, &protos.NodeGroupTemplateNodeInfoRequest{Id: "pis"})
	if err != nil {
		t.Fatal(err)
	}
	var n corev1.Node
	if err := n.Unmarshal(tmpl.NodeBytes); err != nil {
		t.Fatal(err)
	}
	if n.Labels["possum.dev/group"] != "pis" || n.Status.Capacity.Cpu().String() != "4" {
		t.Fatalf("template = %+v", n)
	}
}

func TestNetworkBlipDoesNotCutPower(t *testing.T) {
	h := newHarness(t, testConfig, allOn(), node("pi-1", nil), node("pi-2", nil))
	h.online.Store(false)

	if err := h.fleet.DeleteNodes(context.Background(), "pis", []string{"pi-2"}); err != nil {
		t.Fatal(err)
	}
	h.fleet.ops.Wait()

	if calls := h.driver.Calls(); len(calls) != 0 {
		t.Fatalf("power was cut while the lease was fresh: %v", calls)
	}
}

func TestShutdownAlwaysAsksTheAgent(t *testing.T) {
	h := newHarness(t, testConfig, allOn(), node("pi-1", nil), node("pi-2", nil))
	h.online.Store(false)
	h.lease.Store(false)

	if err := h.fleet.DeleteNodes(context.Background(), "pis", []string{"pi-2"}); err != nil {
		t.Fatal(err)
	}
	h.fleet.ops.Wait()

	asked := false
	for _, a := range h.client.Actions() {
		if p, ok := a.(k8stesting.PatchAction); ok && strings.Contains(string(p.GetPatch()), AnnotationShutdown) {
			asked = true
		}
	}
	if !asked {
		t.Fatal("power was cut without asking the agent")
	}
}

func TestReconcileDoesNotOverwriteOperation(t *testing.T) {
	h := newHarness(t, testConfig, allOn(), node("pi-1", nil), node("pi-2", nil))
	stale := h.fleet.member("pi-2")
	h.fleet.update("pi-2", func(m *Member) { h.fleet.setPhase(m, PhaseShuttingDown, "") })

	h.fleet.reconcileMember(context.Background(), h.fleet.groups["pis"], stale)

	if p := h.phase("pi-2"); p != PhaseShuttingDown {
		t.Fatalf("phase = %s, reconcile overwrote ShuttingDown", p)
	}
}

func TestDeleteFailureKeepsRetrying(t *testing.T) {
	h := newHarness(t, testConfig, allOn(), node("pi-1", nil), node("pi-2", nil))
	h.powerDownOnRequest()
	h.client.PrependReactor("delete", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver unavailable")
	})

	if err := h.fleet.DeleteNodes(context.Background(), "pis", []string{"pi-2"}); err != nil {
		t.Fatal(err)
	}
	h.fleet.ops.Wait()

	if p := h.phase("pi-2"); p != PhaseShuttingDown {
		t.Fatalf("phase = %s, want ShuttingDown for retry", p)
	}
	n, _ := h.node("pi-2")
	if _, ok := n.Annotations[AnnotationShutdown]; ok {
		t.Fatal("shutdown annotation left on a powered-off node")
	}
}

func TestServerDeletesUnregisteredByProviderID(t *testing.T) {
	h := newHarness(t, testConfig, allOn(), node("pi-1", nil), node("pi-2", nil))
	if err := h.fleet.IncreaseSize(context.Background(), "pis", 1); err != nil {
		t.Fatal(err)
	}
	h.fleet.ops.Wait()

	_, err := NewServer(h.fleet).NodeGroupDeleteNodes(context.Background(), &protos.NodeGroupDeleteNodesRequest{
		Id:    "pis",
		Nodes: []*protos.ExternalGrpcNode{{Name: "possum://pis/pi-3", ProviderID: "possum://pis/pi-3"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.fleet.ops.Wait()
	if p := h.phase("pi-3"); p != PhaseOff {
		t.Fatalf("phase = %s", p)
	}
}

func TestRefusedNodeHasNoGroup(t *testing.T) {
	h := newHarness(t, testConfig, allOn(), node("pi-1", map[string]string{controlPlaneLabel: ""}))

	if g := h.fleet.GroupFor("pi-1", ""); g != "" {
		t.Fatalf("refused node mapped to group %q", g)
	}
}

func TestNotReadyRejectsCalls(t *testing.T) {
	cfg, _ := config.Parse([]byte(testConfig))
	f, err := NewFleet(Options{Config: cfg, Drivers: map[string]power.Driver{"pis": fake.New(fake.Config{})}, Log: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ReadyInterceptor(f)(context.Background(), nil, nil, func(context.Context, any) (any, error) { return nil, nil })
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("err = %v, want Unavailable", err)
	}
}

func TestGroupDryRunOverridesGlobal(t *testing.T) {
	cfg := strings.Replace(testConfig, "  driver: {name: fake}", "  dryRun: true\n  driver: {name: fake}", 1)
	h := newHarness(t, cfg, allOn(), node("pi-1", nil), node("pi-2", nil))

	if !h.fleet.DryRun("pis") {
		t.Fatal("group dryRun override ignored")
	}
	if err := h.fleet.IncreaseSize(context.Background(), "pis", 1); !errors.Is(err, ErrDryRun) {
		t.Fatalf("IncreaseSize err = %v, want ErrDryRun", err)
	}
}

func TestGroupTaintsAndTemplateResources(t *testing.T) {
	cfg := strings.Replace(testConfig, "  template: {cpu: \"4\", memory: 8Gi}",
		"  taints: [{key: nvidia.com/gpu, value: present, effect: NoSchedule}]\n  template: {cpu: \"4\", memory: 8Gi, resources: {nvidia.com/gpu: \"1\"}}", 1)
	h := newHarness(t, cfg, allOn(), node("pi-1", nil))

	n, _ := h.node("pi-1")
	if len(n.Spec.Taints) != 1 || n.Spec.Taints[0].Key != "nvidia.com/gpu" {
		t.Fatalf("taints = %v", n.Spec.Taints)
	}
	h.fleet.Reconcile(context.Background())
	if n, _ = h.node("pi-1"); len(n.Spec.Taints) != 1 {
		t.Fatalf("taint duplicated: %v", n.Spec.Taints)
	}

	h.fleet.update("pi-1", func(m *Member) { m.Phase = PhaseOff })
	tmpl, err := h.fleet.TemplateNode(context.Background(), "pis")
	if err != nil {
		t.Fatal(err)
	}
	gpu := tmpl.Status.Allocatable["nvidia.com/gpu"]
	if gpu.Value() != 1 || len(tmpl.Spec.Taints) != 1 {
		t.Fatalf("template allocatable=%v taints=%v", tmpl.Status.Allocatable, tmpl.Spec.Taints)
	}
}

func TestDryRunHoldsScaleDown(t *testing.T) {
	cfg := strings.Replace(testConfig, "dryRun: false", "dryRun: true", 1)
	h := newHarness(t, cfg, allOn(), node("pi-1", nil))

	n, _ := h.node("pi-1")
	if n.Annotations[annotationScaleDownDisabled] != "true" {
		t.Fatalf("annotations = %v", n.Annotations)
	}

	h2 := newHarness(t, testConfig, allOn(), n)
	if n, _ = h2.node("pi-1"); n.Annotations[annotationScaleDownDisabled] != "" || n.Annotations[annotationDryRunHold] != "" {
		t.Fatalf("hold not released: %v", n.Annotations)
	}

	own := node("pi-2", nil)
	own.Annotations = map[string]string{annotationScaleDownDisabled: "true"}
	h3 := newHarness(t, testConfig, allOn(), own)
	if n, _ = h3.node("pi-2"); n.Annotations[annotationScaleDownDisabled] != "true" {
		t.Fatal("removed a scale-down-disabled annotation POSSUM did not set")
	}
}
