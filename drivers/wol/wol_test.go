package wol

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tpural/possum/pkg/power"
)

func TestMagicPacket(t *testing.T) {
	mac, _ := net.ParseMAC("d8:43:ae:24:7c:8a")
	p := MagicPacket(mac)
	if len(p) != 102 || !bytes.Equal(p[:6], bytes.Repeat([]byte{0xff}, 6)) {
		t.Fatalf("bad header: % x", p[:12])
	}
	for i := 6; i < len(p); i += 6 {
		if !bytes.Equal(p[i:i+6], mac) {
			t.Fatalf("repetition at %d is % x", i, p[i:i+6])
		}
	}
}

func TestParseTarget(t *testing.T) {
	tests := []struct {
		in      power.Target
		host    string
		wantErr bool
	}{
		{"d8:43:ae:24:7c:8a@10.1.51.10", "10.1.51.10", false},
		{"D8-43-AE-24-7C-8A@gpu01.lan", "gpu01.lan", false},
		{"d8:43:ae:24:7c:8a", "", true},
		{"d8:43:ae:24:7c:8a@", "", true},
		{"nope@10.1.51.10", "", true},
	}
	for _, tt := range tests {
		_, host, err := ParseTarget(tt.in)
		if (err != nil) != tt.wantErr || host != tt.host {
			t.Errorf("ParseTarget(%q) = %q, %v", tt.in, host, err)
		}
	}
}

func TestPowerOnSendsMagicPackets(t *testing.T) {
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	d, err := New(Config{Broadcast: pc.LocalAddr().String()})
	if err != nil {
		t.Fatal(err)
	}

	if err := d.PowerOn(context.Background(), "d8:43:ae:24:7c:8a@127.0.0.1"); err != nil {
		t.Fatal(err)
	}
	mac, _ := net.ParseMAC("d8:43:ae:24:7c:8a")
	_ = pc.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 200)
	for range 3 {
		n, _, err := pc.ReadFrom(buf)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf[:n], MagicPacket(mac)) {
			t.Fatalf("unexpected payload % x", buf[:n])
		}
	}
}

func TestState(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	open := ln.Addr().(*net.TCPAddr).Port
	closed, _ := net.Listen("tcp", "127.0.0.1:0")
	refused := closed.Addr().(*net.TCPAddr).Port
	_ = closed.Close()
	defer ln.Close()

	tests := []struct {
		name string
		host string
		port int
		want power.State
	}{
		{"listening", "127.0.0.1", open, power.StateOn},
		{"refused means the host is up", "127.0.0.1", refused, power.StateOn},
		{"silent host", "192.0.2.1", 22, power.StateOff},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, _ := New(Config{ProbePort: tt.port, ProbeTimeout: metav1.Duration{Duration: 300 * time.Millisecond}})
			got, err := d.State(context.Background(), power.Target("d8:43:ae:24:7c:8a@"+tt.host))
			if err != nil || got != tt.want {
				t.Fatalf("State = %s, %v; want %s", got, err, tt.want)
			}
		})
	}
}
