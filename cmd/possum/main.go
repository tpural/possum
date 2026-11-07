// Command possum powers idle Kubernetes nodes off and back on.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
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
	"github.com/tpural/possum/internal/provider"
	"github.com/tpural/possum/internal/status"
	"github.com/tpural/possum/pkg/power"
)

var version = "dev"

const usage = `usage: possum <command> [flags]

commands:
  provider   run the Cluster Autoscaler externalgrpc provider
  agent      run the node agent
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
