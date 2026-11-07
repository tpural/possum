package provider

import (
	"context"
	"errors"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var ErrNoTemplate = errors.New("no running node or template to build a node template from")

const toBeDeletedTaint = "ToBeDeletedByClusterAutoscaler"

// transientTaints describe a node's current state rather than its shape.
var transientTaints = []string{
	toBeDeletedTaint,
	TaintAsleep,
	corev1.TaintNodeUnschedulable,
	corev1.TaintNodeNotReady,
	corev1.TaintNodeUnreachable,
}

// TemplateNode returns the shape of a new node in the group, copied from a
// running member when there is one and built from the configured template otherwise.
func (f *Fleet) TemplateNode(ctx context.Context, groupName string) (*corev1.Node, error) {
	g, err := f.group(groupName)
	if err != nil {
		return nil, err
	}
	for _, m := range f.Members() {
		if m.Group != groupName || m.Phase != PhaseRunning {
			continue
		}
		if node, err := f.getNode(ctx, m.Name); err == nil {
			return templateFromNode(groupName, node, g.cfg.Labels, g.cfg.Taints), nil
		}
	}
	t := g.cfg.Template
	if t == nil {
		return nil, ErrNoTemplate
	}
	labels := map[string]string{corev1.LabelOSStable: "linux"}
	maps.Copy(labels, t.Labels)
	maps.Copy(labels, g.cfg.Labels)
	res := corev1.ResourceList{
		corev1.ResourceCPU:    t.CPU,
		corev1.ResourceMemory: t.Memory,
		corev1.ResourcePods:   *resource.NewQuantity(t.Pods, resource.DecimalSI),
	}
	maps.Copy(res, t.Resources)
	return newTemplate(groupName, labels, g.cfg.Taints, res, res), nil
}

func templateFromNode(group string, node *corev1.Node, extra map[string]string, groupTaints []corev1.Taint) *corev1.Node {
	labels := map[string]string{}
	maps.Copy(labels, node.Labels)
	delete(labels, corev1.LabelHostname)
	maps.Copy(labels, extra)
	var taints []corev1.Taint
	for _, t := range node.Spec.Taints {
		if !slices.Contains(transientTaints, t.Key) {
			taints = append(taints, t)
		}
	}
	taints, _ = withTaints(taints, groupTaints)
	return newTemplate(group, labels, taints, node.Status.Capacity, node.Status.Allocatable)
}

func newTemplate(group string, labels map[string]string, taints []corev1.Taint, capacity, allocatable corev1.ResourceList) *corev1.Node {
	name := "possum-template-" + group
	labels[corev1.LabelHostname] = name
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Spec:       corev1.NodeSpec{Taints: taints},
		Status: corev1.NodeStatus{
			Capacity:    capacity.DeepCopy(),
			Allocatable: allocatable.DeepCopy(),
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			},
		},
	}
}
