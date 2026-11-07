package provider

import (
	"context"
	"encoding/json"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	stateConfigMap = "possum-state"
	stateKey       = "members.json"
)

type stateStore struct {
	client    kubernetes.Interface
	namespace string

	mu   sync.Mutex
	last string
}

func (s *stateStore) load(ctx context.Context) (map[string]Member, error) {
	cm, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, stateConfigMap, metav1.GetOptions{})
	if isNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := map[string]Member{}
	if raw := cm.Data[stateKey]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &out); err != nil {
			return nil, err
		}
	}
	s.last = cm.Data[stateKey]
	return out, nil
}

func (s *stateStore) store(ctx context.Context, members map[string]Member) error {
	b, err := json.Marshal(members)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if string(b) == s.last {
		return nil
	}
	cms := s.client.CoreV1().ConfigMaps(s.namespace)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: stateConfigMap, Namespace: s.namespace},
		Data:       map[string]string{stateKey: string(b)},
	}
	_, err = cms.Update(ctx, cm, metav1.UpdateOptions{})
	if isNotFound(err) {
		_, err = cms.Create(ctx, cm, metav1.CreateOptions{})
	}
	if err == nil {
		s.last = string(b)
	}
	return err
}

func (f *Fleet) restore(ctx context.Context) error {
	saved, err := f.state.load(ctx)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for name, s := range saved {
		m, ok := f.members[name]
		if !ok || m.Group != s.Group {
			continue
		}
		if m.Target == "" {
			m.Target = s.Target
		}
		if m.MAC == "" {
			m.MAC = s.MAC
		}
		m.Phase, m.Since, m.Message, m.Power = s.Phase, s.Since, s.Message, s.Power
	}
	return nil
}

func (f *Fleet) save(ctx context.Context) {
	f.saveMu.Lock()
	defer f.saveMu.Unlock()
	f.mu.Lock()
	snap := make(map[string]Member, len(f.members))
	for name, m := range f.members {
		snap[name] = *m
	}
	f.mu.Unlock()
	if err := f.state.store(ctx, snap); err != nil {
		f.opts.Log.Error(err, "save state")
	}
}
