package unifi

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tpural/possum/pkg/power"
)

const switchMAC = "74:ac:b9:55:27:a0"

type cloudKey struct {
	mu          sync.Mutex
	logins      int
	puts        int
	throttle    int
	session     string
	csrf        string
	overrides   []map[string]any
	modes       map[int]string
	clients     []station
	expireOnPut bool
	onPut       func(k *cloudKey)
}

func newCloudKey() *cloudKey {
	return &cloudKey{
		modes: map[int]string{1: "auto", 2: "auto", 4: "auto", 8: "auto"},
		overrides: []map[string]any{
			{"port_idx": 1, "poe_mode": "auto", "name": "Port 1", "native_networkconf_id": "abc"},
			{"port_idx": 8, "poe_mode": "auto", "name": "Control plane"},
		},
		clients: []station{{MAC: "dc:a6:32:da:74:09", SwitchMAC: strings.ToUpper(switchMAC), SwitchPort: 4}},
	}
}

func (k *cloudKey) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.throttle > 0 {
		k.throttle--
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	if r.URL.Path == "/api/auth/login" {
		var creds map[string]string
		_ = json.NewDecoder(r.Body).Decode(&creds)
		if creds["username"] != "possum" || creds["password"] != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		k.logins++
		k.session = "s" + time.Now().String()
		k.csrf = "csrf-" + k.session
		http.SetCookie(w, &http.Cookie{Name: "TOKEN", Value: k.session, Path: "/"})
		w.Header().Set("X-Csrf-Token", k.csrf)
		return
	}
	c, err := r.Cookie("TOKEN")
	if err != nil || c.Value != k.session || k.session == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if r.Method != http.MethodGet && r.Header.Get("X-Csrf-Token") != k.csrf {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	switch {
	case r.URL.Path == "/proxy/network/api/s/default/stat/device":
		k.reply(w, []map[string]any{k.device()})
	case r.URL.Path == "/proxy/network/api/s/default/stat/sta":
		k.reply(w, k.clients)
	case r.Method == http.MethodPut && r.URL.Path == "/proxy/network/api/s/default/rest/device/dev1":
		var body struct {
			Overrides []map[string]any `json:"port_overrides"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		k.puts++
		k.overrides = body.Overrides
		for _, o := range body.Overrides {
			k.modes[int(o["port_idx"].(float64))] = o["poe_mode"].(string)
		}
		if k.onPut != nil {
			k.onPut(k)
		}
		if k.expireOnPut {
			k.session = "expired"
		}
		k.reply(w, []any{})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (k *cloudKey) device() map[string]any {
	ports := []map[string]any{}
	for idx, mode := range k.modes {
		ports = append(ports, map[string]any{"port_idx": idx, "poe_mode": mode, "port_poe": true})
	}
	ports = append(ports, map[string]any{"port_idx": 17, "port_poe": false})
	return map[string]any{"_id": "dev1", "mac": switchMAC, "port_table": ports, "port_overrides": k.overrides}
}

func (k *cloudKey) reply(w http.ResponseWriter, data any) {
	_ = json.NewEncoder(w).Encode(map[string]any{"meta": map[string]string{"rc": "ok"}, "data": data})
}

func newTestDriver(t *testing.T, k *cloudKey) *Driver {
	t.Helper()
	srv := httptest.NewTLSServer(k)
	t.Cleanup(srv.Close)
	d, err := New(Config{URL: srv.URL, Username: "possum", Password: "secret", InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	d.api.backoff = func(int) time.Duration { return time.Millisecond }
	return d
}

func TestPowerOffChangesOnlyTargetPort(t *testing.T) {
	k := newCloudKey()
	d := newTestDriver(t, k)
	ctx := context.Background()

	if err := d.PowerOff(ctx, FormatTarget(switchMAC, 4)); err != nil {
		t.Fatal(err)
	}
	if got, _ := d.State(ctx, FormatTarget(switchMAC, 4)); got != power.StateOff {
		t.Fatalf("port 4 state = %s, want off", got)
	}
	if k.modes[1] != "auto" || k.modes[8] != "auto" {
		t.Fatalf("other ports changed: %v", k.modes)
	}
	if k.overrides[0]["native_networkconf_id"] != "abc" || k.overrides[1]["name"] != "Control plane" {
		t.Fatalf("existing overrides not preserved: %v", k.overrides)
	}
}

func TestPowerIsIdempotent(t *testing.T) {
	k := newCloudKey()
	d := newTestDriver(t, k)
	ctx := context.Background()

	for range 3 {
		if err := d.PowerOn(ctx, FormatTarget(switchMAC, 1)); err != nil {
			t.Fatal(err)
		}
	}
	if k.puts != 0 {
		t.Fatalf("puts = %d, want 0", k.puts)
	}
}

func TestSessionIsReused(t *testing.T) {
	k := newCloudKey()
	d := newTestDriver(t, k)
	ctx := context.Background()

	for range 5 {
		if _, err := d.fetchDevices(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if k.logins != 1 {
		t.Fatalf("logins = %d, want 1", k.logins)
	}
}

func TestReloginOnExpiredSession(t *testing.T) {
	k := newCloudKey()
	k.expireOnPut = true
	d := newTestDriver(t, k)

	if err := d.PowerOff(context.Background(), FormatTarget(switchMAC, 2)); err != nil {
		t.Fatal(err)
	}
	if k.logins != 2 {
		t.Fatalf("logins = %d, want 2", k.logins)
	}
}

func TestBacksOffWhenRateLimited(t *testing.T) {
	k := newCloudKey()
	k.throttle = 3
	d := newTestDriver(t, k)

	if _, err := d.State(context.Background(), FormatTarget(switchMAC, 4)); err != nil {
		t.Fatal(err)
	}

	k.throttle = 100
	d.invalidate()
	_, err := d.State(context.Background(), FormatTarget(switchMAC, 4))
	if !errors.Is(err, power.ErrRateLimited) {
		t.Fatalf("err = %v, want ErrRateLimited", err)
	}
}

func TestErrors(t *testing.T) {
	k := newCloudKey()
	d := newTestDriver(t, k)
	ctx := context.Background()

	tests := []struct {
		target power.Target
		want   error
	}{
		{FormatTarget("aa:bb:cc:dd:ee:ff", 1), power.ErrNotFound},
		{FormatTarget(switchMAC, 12), power.ErrNotFound},
		{FormatTarget(switchMAC, 17), power.ErrUnsupported},
	}
	for _, tt := range tests {
		if err := d.PowerOff(ctx, tt.target); !errors.Is(err, tt.want) {
			t.Errorf("PowerOff(%s) = %v, want %v", tt.target, err, tt.want)
		}
	}
	if k.puts != 0 {
		t.Fatalf("puts = %d, want 0", k.puts)
	}
}

func TestBadCredentials(t *testing.T) {
	k := newCloudKey()
	d := newTestDriver(t, k)
	d.api.password = "wrong"

	_, err := d.State(context.Background(), FormatTarget(switchMAC, 4))
	if err == nil || !strings.Contains(err.Error(), "login rejected") {
		t.Fatalf("err = %v, want login rejected", err)
	}
}

func TestDiscover(t *testing.T) {
	d := newTestDriver(t, newCloudKey())
	mac, _ := net.ParseMAC("DC:A6:32:DA:74:09")
	other, _ := net.ParseMAC("dc:a6:32:00:00:01")

	got, err := d.Discover(context.Background(), []net.HardwareAddr{mac, other})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]power.Target{"dc:a6:32:da:74:09": FormatTarget(switchMAC, 4)}
	if len(got) != 1 || got["dc:a6:32:da:74:09"] != want["dc:a6:32:da:74:09"] {
		t.Fatalf("Discover = %v, want %v", got, want)
	}
}

func TestParseTarget(t *testing.T) {
	tests := []struct {
		in      power.Target
		mac     string
		port    int
		wantErr bool
	}{
		{"74:AC:B9:55:27:A0/4", switchMAC, 4, false},
		{"74:ac:b9:55:27:a0", "", 0, true},
		{"nope/4", "", 0, true},
		{"74:ac:b9:55:27:a0/0", "", 0, true},
	}
	for _, tt := range tests {
		mac, port, err := ParseTarget(tt.in)
		if (err != nil) != tt.wantErr || mac != tt.mac || port != tt.port {
			t.Errorf("ParseTarget(%q) = %q, %d, %v", tt.in, mac, port, err)
		}
	}
}

func TestVerifyToleratesNormalizedOverrides(t *testing.T) {
	k := newCloudKey()
	k.onPut = func(k *cloudKey) {
		for _, o := range k.overrides {
			o["setting_preference"] = "manual"
		}
		k.overrides = append([]map[string]any{k.overrides[len(k.overrides)-1]}, k.overrides[:len(k.overrides)-1]...)
	}
	d := newTestDriver(t, k)

	if err := d.PowerOff(context.Background(), FormatTarget(switchMAC, 4)); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyDetectsOtherPortChange(t *testing.T) {
	k := newCloudKey()
	k.onPut = func(k *cloudKey) { k.overrides[0]["poe_mode"] = "off" }
	d := newTestDriver(t, k)

	err := d.PowerOff(context.Background(), FormatTarget(switchMAC, 4))
	if err == nil || !strings.Contains(err.Error(), "changed unexpectedly") {
		t.Fatalf("err = %v, want unexpected change", err)
	}
}
