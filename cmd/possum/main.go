// Command possum powers idle Kubernetes nodes off and back on.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	_ "github.com/tpural/possum/drivers/fake"
	_ "github.com/tpural/possum/drivers/unifi"
	_ "github.com/tpural/possum/drivers/wol"
	"github.com/tpural/possum/internal/agent"
	"github.com/tpural/possum/internal/config"
	"github.com/tpural/possum/internal/externalgrpc/protos"
	"github.com/tpural/possum/internal/gate"
	"github.com/tpural/possum/internal/provider"
	"github.com/tpural/possum/internal/status"
	"github.com/tpural/possum/pkg/power"
)

var version = "dev"

const usage = `usage: possum <command> [flags]

commands:
  provider   run the Cluster Autoscaler externalgrpc provider
  agent      run the node agent
  gate       scale an HTTP backend to zero and wake it on demand
  version    print the version
`

func main() {
	os.Exit(run())
}

func run() int {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "provider":
		err = runProvider(ctx, os.Args[2:])
	case "gate":
		err = runGate(ctx, os.Args[2:])
	case "agent":
		err = runAgent(ctx, os.Args[2:])
	case "version":
		fmt.Println(version)
	default:
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	defer klog.Flush()
	if err != nil {
		klog.ErrorS(err, "exiting")
		return 1
	}
	return 0
}

func runProvider(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("provider", flag.ExitOnError)
	configPath := fs.String("config", "/etc/possum/config.yaml", "path to the PossumConfig file")
	grpcAddr := fs.String("grpc-addr", ":8086", "externalgrpc listen address")
	httpAddr := fs.String("http-addr", ":8080", "status, health and metrics listen address")
	namespace := fs.String("namespace", envOr("POD_NAMESPACE", "possum"), "namespace for state and events")
	interval := fs.Duration("interval", 15*time.Second, "reconcile interval")
	kubeconfig := fs.String("kubeconfig", "", "kubeconfig path; in-cluster config when empty")
	klog.InitFlags(fs)
	_ = fs.Parse(args)
	log := klog.NewKlogr().WithName("provider")

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	drivers := map[string]power.Driver{}
	for _, g := range cfg.NodeGroups {
		d, err := power.New(g.Driver.Name, g.Driver.Config)
		if err != nil {
			return fmt.Errorf("node group %s: %w", g.Name, err)
		}
		drivers[g.Name] = d
	}
	client, err := kubeClient(*kubeconfig)
	if err != nil {
		return err
	}

	broadcaster := record.NewBroadcaster(record.WithContext(ctx))
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: client.CoreV1().Events("")})
	defer broadcaster.Shutdown()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	fleet, err := provider.NewFleet(provider.Options{
		Config:    cfg,
		Drivers:   drivers,
		Client:    client,
		Namespace: *namespace,
		Recorder:  broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "possum"}),
		Metrics:   provider.NewMetrics(reg),
		Log:       log,
	})
	if err != nil {
		return err
	}
	log.Info("starting", "version", version, "dryRun", cfg.DryRunEnabled(), "drivers", power.Drivers())

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		return err
	}
	gs := grpc.NewServer(grpc.UnaryInterceptor(provider.ReadyInterceptor(fleet)))
	protos.RegisterCloudProviderServer(gs, provider.NewServer(fleet))
	hs := &http.Server{Addr: *httpAddr, Handler: status.Handler(fleet, reg), ReadHeaderTimeout: 10 * time.Second}

	errc := make(chan error, 3)
	go func() { errc <- gs.Serve(lis) }()
	go func() {
		if err := hs.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	go func() { errc <- fleet.Run(ctx, *interval) }()

	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	gs.GracefulStop()
	_ = hs.Shutdown(shutdownCtx)
	return err
}

func runGate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("gate", flag.ExitOnError)
	listen := fs.String("listen", ":8080", "proxy listen address")
	adminAddr := fs.String("admin-addr", ":9090", "health and metrics listen address")
	upstream := fs.String("upstream", "", "backend URL, e.g. http://llama-swap-backend.llama-swap.svc:8080")
	deployment := fs.String("deployment", "", "backend Deployment as namespace/name")
	modelsConfig := fs.String("models-config", "", "llama-swap config.yaml to answer /v1/models from")
	idle := fs.Duration("idle", 30*time.Minute, "scale to zero after this long without requests, unless a rule matches")
	var rules []gate.Rule
	fs.Func("idle-rule", `per-source idle time, repeatable: "host llm.example.com 10m", "cidr 10.0.0.0/8 1h", "header User-Agent=^n8n 10m"`, func(s string) error {
		r, err := gate.ParseRule(s)
		rules = append(rules, r)
		return err
	})
	wakeTimeout := fs.Duration("wake-timeout", 15*time.Minute, "how long a request waits for the backend")
	kubeconfig := fs.String("kubeconfig", "", "kubeconfig path; in-cluster config when empty")
	klog.InitFlags(fs)
	_ = fs.Parse(args)
	log := klog.NewKlogr().WithName("gate")

	u, err := url.Parse(*upstream)
	if err != nil || u.Host == "" {
		return fmt.Errorf("--upstream %q is not a URL", *upstream)
	}
	ns, name, ok := strings.Cut(*deployment, "/")
	if !ok || ns == "" || name == "" {
		return fmt.Errorf("--deployment %q is not namespace/name", *deployment)
	}
	var models []gate.Model
	if *modelsConfig != "" {
		if models, err = gate.LoadLlamaSwapModels(*modelsConfig); err != nil {
			return err
		}
	}
	client, err := kubeClient(*kubeconfig)
	if err != nil {
		return err
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	g, err := gate.New(gate.Options{
		Upstream:    u,
		Scaler:      gate.DeploymentScaler{Client: client, Namespace: ns, Name: name},
		Models:      models,
		Idle:        *idle,
		Rules:       rules,
		WakeTimeout: *wakeTimeout,
		Metrics:     gate.NewMetrics(reg),
		Log:         log,
	})
	if err != nil {
		return err
	}
	log.Info("starting", "version", version, "upstream", u.String(), "deployment", *deployment, "models", len(models), "idle", *idle, "rules", len(rules))

	admin := http.NewServeMux()
	admin.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	admin.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	admin.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(g.Status())
	})
	servers := []*http.Server{
		{Addr: *listen, Handler: g, ReadHeaderTimeout: 10 * time.Second},
		{Addr: *adminAddr, Handler: admin, ReadHeaderTimeout: 10 * time.Second},
	}
	errc := make(chan error, len(servers))
	for _, s := range servers {
		go func() {
			if err := s.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				errc <- err
			}
		}()
	}
	go g.Run(ctx)

	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, s := range servers {
		_ = s.Shutdown(shutdownCtx)
	}
	return err
}

func runAgent(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("agent", flag.ExitOnError)
	nodeName := fs.String("node-name", os.Getenv("NODE_NAME"), "name of the node this agent runs on")
	bus := fs.String("dbus-address", agent.DefaultSystemBus, "host system D-Bus address")
	kubeconfig := fs.String("kubeconfig", "", "kubeconfig path; in-cluster config when empty")
	klog.InitFlags(fs)
	_ = fs.Parse(args)

	if *nodeName == "" {
		return errors.New("--node-name or NODE_NAME is required")
	}
	client, err := kubeClient(*kubeconfig)
	if err != nil {
		return err
	}
	a := &agent.Agent{
		Client:   client,
		NodeName: *nodeName,
		PowerOff: agent.LogindPowerOff(*bus),
		Suspend:  agent.LogindSuspend(*bus),
		Log:      klog.NewKlogr().WithName("agent").WithValues("node", *nodeName),
	}
	return a.Run(ctx)
}

func kubeClient(kubeconfig string) (kubernetes.Interface, error) {
	var (
		cfg *rest.Config
		err error
	)
	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, err
	}
	cfg.UserAgent = "possum/" + version
	return kubernetes.NewForConfig(cfg)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
