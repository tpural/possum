package gate

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// DeploymentScaler scales a Deployment by patching spec.replicas.
type DeploymentScaler struct {
	Client    kubernetes.Interface
	Namespace string
	Name      string
}

func (d DeploymentScaler) Replicas(ctx context.Context) (desired, ready int, err error) {
	dep, err := d.Client.AppsV1().Deployments(d.Namespace).Get(ctx, d.Name, metav1.GetOptions{})
	if err != nil {
		return 0, 0, err
	}
	desired = 1
	if dep.Spec.Replicas != nil {
		desired = int(*dep.Spec.Replicas)
	}
	return desired, int(dep.Status.ReadyReplicas), nil
}

func (d DeploymentScaler) Scale(ctx context.Context, replicas int) error {
	patch := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
	_, err := d.Client.AppsV1().Deployments(d.Namespace).Patch(ctx, d.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}
