package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tpural/possum/internal/provider"
)

func requested(at time.Time) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
		provider.AnnotationShutdown: at.UTC().Format(time.RFC3339),
	}}}
}

func newAgent(next *error, calls *int) *Agent {
	return &Agent{
		Log:      logr.Discard(),
		Booted:   time.Now().Add(-time.Hour),
		PowerOff: func(context.Context) error { *calls++; return *next },
	}
}

func TestHandleRetriesAfterFailureThenPowersOffOnce(t *testing.T) {
	calls := 0
	next := errors.New("dbus down")
	a := newAgent(&next, &calls)
	ctx := context.Background()

	a.handle(ctx, &corev1.Node{})
	a.handle(ctx, requested(time.Now()))
	next = nil
	a.handle(ctx, requested(time.Now()))
	a.handle(ctx, requested(time.Now()))

	if calls != 2 {
		t.Fatalf("PowerOff called %d times, want 2", calls)
	}
}

func TestHandleIgnoresRequestsFromBeforeBoot(t *testing.T) {
	calls := 0
	var next error
	a := newAgent(&next, &calls)

	a.handle(context.Background(), requested(a.Booted.Add(-time.Minute)))
	a.handle(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
		provider.AnnotationShutdown: "requested",
	}}})

	if calls != 0 {
		t.Fatalf("PowerOff called %d times, want 0", calls)
	}
}

func TestHandleActsAgainAfterRequestWithdrawn(t *testing.T) {
	calls := 0
	var next error
	a := newAgent(&next, &calls)
	ctx := context.Background()

	a.handle(ctx, requested(time.Now()))
	a.handle(ctx, &corev1.Node{})
	a.handle(ctx, requested(time.Now()))

	if calls != 2 {
		t.Fatalf("PowerOff called %d times, want 2", calls)
	}
}

func TestBootTime(t *testing.T) {
	bt, err := bootTime()
	if err != nil {
		t.Skip(err)
	}
	if bt.After(time.Now()) || bt.Before(time.Unix(0, 0)) {
		t.Fatalf("boot time %v out of range", bt)
	}
}
