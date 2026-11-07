// Package agent runs on managed nodes and powers the host off or suspends it when the provider asks.
package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/tpural/possum/internal/provider"
)

// Agent watches its own Node for the shutdown annotation.
type Agent struct {
	Client   kubernetes.Interface
	NodeName string
	PowerOff func(ctx context.Context) error
	Suspend  func(ctx context.Context) error
	Log      logr.Logger
	// Booted overrides the host boot time, which is read from /proc/stat when zero.
	Booted time.Time

	mu       sync.Mutex
	shutting bool
}

// Run publishes the node's MAC address and then waits for a shutdown request.
func (a *Agent) Run(ctx context.Context) error {
	if err := a.publishMAC(ctx); err != nil {
		a.Log.Error(err, "publish MAC address")
	}
	lw := cache.NewListWatchFromClient(a.Client.CoreV1().RESTClient(), "nodes", "",
		fields.OneTermEqualSelector("metadata.name", a.NodeName))
	_, ctrl := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: lw,
		ObjectType:    &corev1.Node{},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj any) { a.handle(ctx, obj) },
			UpdateFunc: func(_, obj any) { a.handle(ctx, obj) },
		},
	})
	ctrl.Run(ctx.Done())
	return nil
}

func (a *Agent) handle(ctx context.Context, obj any) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	raw, requested := node.Annotations[provider.AnnotationShutdown]
	if !requested {
		a.shutting = false
		return
	}
	if a.shutting {
		return
	}
	at, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		a.Log.Error(err, "ignoring malformed shutdown request", "value", raw)
		return
	}
	booted := a.Booted
	if booted.IsZero() {
		if booted, err = bootTime(); err != nil {
			a.Log.Error(err, "read boot time")
			return
		}
	}
	if at.Before(booted) {
		a.Log.Info("ignoring shutdown request from before this boot", "requestedAt", at, "booted", booted)
		return
	}
	op, verb := a.PowerOff, "power off"
	if node.Annotations[provider.AnnotationShutdownMode] == provider.ShutdownModeSuspend {
		op, verb = a.Suspend, "suspend"
	}
	a.Log.Info("shutdown requested", "action", verb, "requestedAt", at)
	if err := op(ctx); err != nil {
		a.Log.Error(err, verb)
		return
	}
	a.shutting = true
}

func (a *Agent) publishMAC(ctx context.Context) error {
	var node *corev1.Node
	err := retry(ctx, func() (err error) {
		node, err = a.Client.CoreV1().Nodes().Get(ctx, a.NodeName, metav1.GetOptions{})
		return err
	})
	if err != nil {
		return err
	}
	mac, err := macFor(node)
	if err != nil {
		return err
	}
	if node.Annotations[provider.AnnotationMAC] == mac {
		return nil
	}
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, provider.AnnotationMAC, mac)
	_, err = a.Client.CoreV1().Nodes().Patch(ctx, a.NodeName, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

func macFor(node *corev1.Node) (string, error) {
	var ip net.IP
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP {
			ip = net.ParseIP(addr.Address)
			break
		}
	}
	if ip == nil {
		return "", errors.New("node has no InternalIP")
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok && n.IP.Equal(ip) && len(iface.HardwareAddr) > 0 {
				return strings.ToLower(iface.HardwareAddr.String()), nil
			}
		}
	}
	return "", fmt.Errorf("no interface holds %s (is hostNetwork enabled?)", ip)
}

func retry(ctx context.Context, fn func() error) error {
	var err error
	for i := range 5 {
		if err = fn(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(i+1) * time.Second):
		}
	}
	return err
}
