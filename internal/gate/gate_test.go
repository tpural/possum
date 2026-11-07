package gate

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

type fakeScaler struct {
	mu       sync.Mutex
	desired  int
	ready    int
	scales   []int
	bootTime time.Duration
	never    bool
}

func (f *fakeScaler) Replicas(context.Context) (int, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.desired, f.ready, nil
}

func (f *fakeScaler) Scale(_ context.Context, n int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.desired = n
	f.scales = append(f.scales, n)
	if n == 0 {
		f.ready = 0
		return nil
	}
	if !f.never {
		time.AfterFunc(f.bootTime, func() {
			f.mu.Lock()
			f.ready = n
			f.mu.Unlock()
		})
	}
	return nil
}

func (f *fakeScaler) history() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.scales...)
}

func newTestGate(t *testing.T, s *fakeScaler, upstream http.Handler, idle, wake time.Duration) *Gate {
	t.Helper()
	srv := httptest.NewServer(upstream)
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	g, err := New(Options{
		Upstream:     u,
		Scaler:       s,
		Models:       []Model{{ID: "coder", Name: "Coder", Aliases: []string{"coding"}}},
		Idle:         idle,
		WakeTimeout:  wake,
		PollInterval: 5 * time.Millisecond,
		Log:          logr.Discard(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func echo() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, "upstream %s", r.URL.Path)
	})
}

func TestModelsNeverWake(t *testing.T) {
	s := &fakeScaler{}
	g := newTestGate(t, s, echo(), time.Hour, time.Second)

	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/models", nil))

	var body struct {
		Data []struct {
			ID   string `json:"id"`
			Meta struct {
				Llamaswap struct {
					Aliases []string `json:"aliases"`
				} `json:"llamaswap"`
			} `json:"meta"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || len(body.Data) != 1 || body.Data[0].ID != "coder" {
		t.Fatalf("models = %s (%v)", rec.Body, err)
	}
	if body.Data[0].Meta.Llamaswap.Aliases[0] != "coding" {
		t.Fatalf("aliases missing: %s", rec.Body)
	}
	if h := s.history(); len(h) != 0 {
		t.Fatalf("model listing scaled the backend: %v", h)
	}
}

func TestChatWakesBackendOnce(t *testing.T) {
	s := &fakeScaler{bootTime: 30 * time.Millisecond}
	g := newTestGate(t, s, echo(), time.Hour, time.Second)

	var wg sync.WaitGroup
	codes := make(chan int, 5)
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			g.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
			codes <- rec.Code
		}()
	}
	wg.Wait()
	close(codes)
	for c := range codes {
		if c != http.StatusOK {
			t.Fatalf("status = %d", c)
		}
	}
	if h := s.history(); len(h) != 1 || h[0] != 1 {
		t.Fatalf("scales = %v, want one scale to 1", h)
	}
}

func TestWakeTimeout(t *testing.T) {
	s := &fakeScaler{never: true}
	g := newTestGate(t, s, echo(), time.Hour, 50*time.Millisecond)

	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestIdleScalesToZero(t *testing.T) {
	s := &fakeScaler{desired: 1, ready: 1}
	g := newTestGate(t, s, echo(), 20*time.Millisecond, time.Second)

	g.sleepIfIdle(context.Background())
	if len(s.history()) != 0 {
		t.Fatal("scaled down before the idle period")
	}
	time.Sleep(30 * time.Millisecond)
	g.inflight.Add(1)
	g.sleepIfIdle(context.Background())
	if len(s.history()) != 0 {
		t.Fatal("scaled down with a request in flight")
	}
	g.inflight.Add(-1)
	g.sleepIfIdle(context.Background())
	if h := s.history(); len(h) != 1 || h[0] != 0 {
		t.Fatalf("scales = %v, want [0]", h)
	}
}

func TestStreamsResponses(t *testing.T) {
	release := make(chan struct{})
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-release
		_, _ = io.WriteString(w, "data: second\n\n")
	})
	s := &fakeScaler{desired: 1, ready: 1}
	g := newTestGate(t, s, upstream, time.Hour, time.Second)
	srv := httptest.NewServer(g)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil || line != "data: first\n" {
		t.Fatalf("first event = %q, %v", line, err)
	}
	close(release)
}

func TestLoadLlamaSwapModels(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg := `
healthCheckTimeout: 900
models:
  zeta: {name: Zeta, cmd: x}
  alpha: {name: Alpha, aliases: [a], cmd: y}
  hidden: {cmd: z, unlisted: true}
`
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	models, err := LoadLlamaSwapModels(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 2 || models[0].ID != "alpha" || models[0].Aliases[0] != "a" || models[1].ID != "zeta" {
		t.Fatalf("models = %+v", models)
	}
}

func TestDeploymentScaler(t *testing.T) {
	client := k8sfake.NewClientset(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-swap", Namespace: "llama-swap"},
		Spec:       appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 1},
	})
	d := DeploymentScaler{Client: client, Namespace: "llama-swap", Name: "llama-swap"}
	ctx := context.Background()

	if err := d.Scale(ctx, 0); err != nil {
		t.Fatal(err)
	}
	desired, ready, err := d.Replicas(ctx)
	if err != nil || desired != 0 || ready != 1 {
		t.Fatalf("Replicas = %d, %d, %v", desired, ready, err)
	}
}

func TestParseRule(t *testing.T) {
	tests := []struct {
		spec    string
		req     func() *http.Request
		match   bool
		wantErr bool
	}{
		{"host llm.lan.example.com 10m", func() *http.Request {
			r := httptest.NewRequest(http.MethodPost, "http://LLM.lan.example.com:8080/v1/chat/completions", nil)
			return r
		}, true, false},
		{"host llm.lan.example.com 10m", func() *http.Request {
			return httptest.NewRequest(http.MethodPost, "http://llama-swap.llama-swap.svc:8080/", nil)
		}, false, false},
		{"cidr 10.244.0.0/16 1h", func() *http.Request {
			r := httptest.NewRequest(http.MethodPost, "/", nil)
			r.RemoteAddr = "10.244.3.7:51234"
			return r
		}, true, false},
		{"header User-Agent=^n8n 10m", func() *http.Request {
			r := httptest.NewRequest(http.MethodPost, "/", nil)
			r.Header.Set("User-Agent", "n8n/1.80")
			return r
		}, true, false},
		{"host x", nil, false, true},
		{"cidr nope 1h", nil, false, true},
		{"header NoEquals 1h", nil, false, true},
		{"color red 1h", nil, false, true},
		{"host x 0s", nil, false, true},
	}
	for _, tt := range tests {
		r, err := ParseRule(tt.spec)
		if (err != nil) != tt.wantErr {
			t.Fatalf("ParseRule(%q) err = %v", tt.spec, err)
		}
		if err == nil && r.Matches(tt.req()) != tt.match {
			t.Errorf("%q match = %v, want %v", tt.spec, !tt.match, tt.match)
		}
	}
}

func TestLongestDeadlineWins(t *testing.T) {
	agents, _ := ParseRule("header User-Agent=^agent 50ms")
	s := &fakeScaler{desired: 1, ready: 1}
	g := newTestGate(t, s, echo(), 10*time.Millisecond, time.Second)
	g.opts.Rules = []Rule{agents}
	chat, _ := ParseRule("host chat.example 300ms")
	g.opts.Rules = append(g.opts.Rules, chat)

	req := httptest.NewRequest(http.MethodPost, "http://chat.example/v1/chat/completions", nil)
	g.ServeHTTP(httptest.NewRecorder(), req)
	req = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("User-Agent", "agent/1")
	g.ServeHTTP(httptest.NewRecorder(), req)

	time.Sleep(80 * time.Millisecond)
	g.sleepIfIdle(context.Background())
	if len(s.history()) != 0 {
		t.Fatal("the agent's short idle cut the chat's longer window short")
	}
	time.Sleep(260 * time.Millisecond)
	g.sleepIfIdle(context.Background())
	if h := s.history(); len(h) != 1 || h[0] != 0 {
		t.Fatalf("scales = %v, want [0]", h)
	}
}
