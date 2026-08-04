package readiness

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/nuromirg/petri/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Checker struct {
	client     client.Client
	httpClient *http.Client
}

func NewChecker(c client.Client) *Checker {
	return &Checker{
		client:     c,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *Checker) IsReady(ctx context.Context, namespace string, releaseName string, readiness *v1alpha1.ReadinessSpec) (bool, string, error) {
	var (
		deployments  = &appsv1.DeploymentList{}
		statefulSets = &appsv1.StatefulSetList{}
	)

	// TODO we rely on helm for this label for now, but later on we should implement action.Get().
	if err := c.client.List(ctx, deployments, client.InNamespace(namespace), client.MatchingLabels{
		"app.kubernetes.io/instance": releaseName,
	}); err != nil {
		return false, "", err
	}
	if err := c.client.List(ctx, statefulSets, client.InNamespace(namespace), client.MatchingLabels{
		"app.kubernetes.io/instance": releaseName,
	}); err != nil {
		return false, "", err
	}

	if len(deployments.Items) == 0 && len(statefulSets.Items) == 0 {
		return false, "no workloads found for release " + releaseName, nil
	}

	for _, deployment := range deployments.Items {
		var desired int32 = 1
		if deployment.Spec.Replicas != nil {
			desired = *deployment.Spec.Replicas
		}

		if deployment.Status.AvailableReplicas < desired {
			return false, fmt.Sprintf("deployment %s: %d/%d replicas available", deployment.Name, deployment.Status.AvailableReplicas, desired), nil
		}
	}

	for _, statefulSet := range statefulSets.Items {
		var desired int32 = 1
		if statefulSet.Spec.Replicas != nil {
			desired = *statefulSet.Spec.Replicas
		}

		if statefulSet.Status.ReadyReplicas < desired {
			return false, fmt.Sprintf("statefulSet %s: %d/%d replicas available", statefulSet.Name, statefulSet.Status.ReadyReplicas, desired), nil
		}
	}

	if readiness == nil || readiness.HTTPGet == nil {
		return true, "", nil
	}

	services := &corev1.ServiceList{}
	if err := c.client.List(ctx, services, client.InNamespace(namespace), client.MatchingLabels{"app.kubernetes.io/instance": releaseName}); err != nil {
		return false, "", err
	}

	// TODO consider to iterate and find first result with non-empty readiness.httpget.port
	if len(services.Items) == 0 {
		return false, "no service found for release " + releaseName, nil
	}

	serviceName := findServiceByPort(services.Items, readiness.HTTPGet.Port)
	if serviceName == "" {
		return false, fmt.Sprintf("no service with port %d found for release %s", readiness.HTTPGet.Port, releaseName), nil
	}

	url := buildServiceURL(serviceName, namespace, readiness.HTTPGet.Port, readiness.HTTPGet.Path)
	if err := c.check(ctx, url); err != nil {
		return false, "http GET " + url + ": " + err.Error(), nil //nolint:nilerr // probe failure is reported as not-ready, not as an error
	}

	return true, "", nil
}

func (c *Checker) check(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	return nil
}

func buildServiceURL(releaseName, namespace string, port int32, path string) string {
	return fmt.Sprintf("http://%s.%s.svc.cluster.local:%d%s", releaseName, namespace, port, path)
}

func findServiceByPort(services []corev1.Service, port int32) string {
	for _, svc := range services {
		for _, p := range svc.Spec.Ports {
			if p.Port == port {
				return svc.Name
			}
		}
	}
	return ""
}
