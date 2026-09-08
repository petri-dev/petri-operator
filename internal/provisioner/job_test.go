package provisioner

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	"github.com/petri-dev/petri-operator/api/v1alpha1"
	"github.com/petri-dev/petri-operator/internal/deployer"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestBuildJob(t *testing.T) {
	t.Parallel()
	p := &JobProvisioner{}
	opts := ProvisionOptions{
		EnvName:              "pr-1",
		ComponentName:        "postgres",
		SharedName:           "postgres-preview",
		ProvisionerSecretRef: "shared-postgres-preview-provision-pr-1",
		Script: v1alpha1.JobScript{
			Image:   "postgres:15",
			Script:  "psql -c 'CREATE DATABASE x'",
			EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "admin-creds"}}}},
		},
	}

	job := p.buildJob(opts, OpProvision, jobName(OpProvision, opts.EnvName, opts.ComponentName))

	if job.Namespace != sharedNamespace {
		t.Fatalf("namespace = %q, want %q", job.Namespace, sharedNamespace)
	}

	c := job.Spec.Template.Spec.Containers[0]

	// script form wraps in /bin/sh -c
	if len(c.Command) != 3 || c.Command[0] != "/bin/sh" || c.Command[2] != opts.Script.Script {
		t.Fatalf("script command wrong: %v", c.Command)
	}

	// provider envFrom + appended provision secret
	if len(c.EnvFrom) != 2 || c.EnvFrom[1].SecretRef.Name != opts.ProvisionerSecretRef {
		t.Fatalf("envFrom wiring wrong: %+v", c.EnvFrom)
	}
}

func TestProvisionJobLifecycle(t *testing.T) {
	t.Parallel()
	for _, op := range []string{OpProvision, OpDeprovision} {
		for _, phase := range []batchv1.JobConditionType{"", batchv1.JobComplete, batchv1.JobFailed} {
			for _, changed := range []bool{false, true} {
				t.Run(op+"/"+string(phase)+"/"+map[bool]string{false: "same", true: "changed"}[changed], func(t *testing.T) {
					t.Parallel()
					g := gomega.NewWithT(t)
					s := runtime.NewScheme()
					g.Expect(batchv1.AddToScheme(s)).To(gomega.Succeed())
					g.Expect(corev1.AddToScheme(s)).To(gomega.Succeed())
					deletes := 0
					c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&batchv1.Job{}).WithInterceptorFuncs(interceptor.Funcs{
						Delete: func(_ context.Context, _ client.WithWatch, _ client.Object, options ...client.DeleteOption) error {
							deletes++
							del := &client.DeleteOptions{}
							for _, option := range options {
								option.ApplyToDelete(del)
							}
							g.Expect(del.Preconditions).NotTo(gomega.BeNil())
							g.Expect(del.Preconditions.UID).NotTo(gomega.BeNil())
							g.Expect(*del.PropagationPolicy).To(gomega.Equal(metav1.DeletePropagationForeground))
							return nil // Accepted deletion, but the old name is still occupied.
						},
					}).Build()
					p := &JobProvisioner{Client: c, Reader: c}
					opts := ProvisionOptions{EnvName: "env", ComponentName: "db", Script: v1alpha1.JobScript{Image: "busybox", Script: "echo first"}}
					g.Expect(p.submit(t.Context(), opts, op)).To(gomega.Succeed())
					job := new(batchv1.Job)
					key := client.ObjectKey{Name: jobName(op, opts.EnvName, opts.ComponentName), Namespace: sharedNamespace}
					g.Expect(c.Get(t.Context(), key, job)).To(gomega.Succeed())
					if phase != "" {
						job.Status.Conditions = []batchv1.JobCondition{{Type: phase, Status: corev1.ConditionTrue}}
						g.Expect(c.Status().Update(t.Context(), job)).To(gomega.Succeed())
					}
					if changed {
						opts.Script.Script = "echo second"
					}
					state, err := p.observe(t.Context(), opts, op)
					g.Expect(err).NotTo(gomega.HaveOccurred())
					want := deployer.RunningJobPhase
					if phase == batchv1.JobComplete {
						want = deployer.SucceededJobPhase
					}
					if phase == batchv1.JobFailed {
						want = deployer.FailedJobPhase
					}
					if phase != "" && changed {
						want = deployer.PendingJobPhase
					}
					g.Expect(state.Phase).To(gomega.Equal(want))
					g.Expect(p.submit(t.Context(), opts, op)).To(gomega.Succeed())
					wantDeletes := 0
					if phase == batchv1.JobFailed || (phase == batchv1.JobComplete && changed) {
						wantDeletes = 1
					}
					g.Expect(deletes).To(gomega.Equal(wantDeletes))
					jobs := new(batchv1.JobList)
					g.Expect(c.List(t.Context(), jobs)).To(gomega.Succeed())
					g.Expect(jobs.Items).To(gomega.HaveLen(1))
				})
			}
		}
	}
}

func TestBuildJobCommandForm(t *testing.T) {
	t.Parallel()
	p := &JobProvisioner{}
	opts := ProvisionOptions{
		EnvName:       "pr-1",
		ComponentName: "minio",
		Script: v1alpha1.JobScript{
			Image:   "minio/mc",
			Command: []string{"/mc", "mb", "local/pr-1-bucket"},
		},
	}

	c := p.buildJob(opts, OpProvision, "petri-provision-pr-1-minio").Spec.Template.Spec.Containers[0]

	// command form passes through verbatim, no /bin/sh wrapping
	if len(c.Command) != 3 || c.Command[0] != "/mc" {
		t.Fatalf("command form should pass through: %v", c.Command)
	}
}

func TestJobNameTruncation(t *testing.T) {
	t.Parallel()
	name := jobName(OpDeprovision, "pr-a-very-long-environment-name-that-goes-on", "postgres-component-x")
	if len(name) > 63 {
		t.Fatalf("name %q len %d exceeds 63", name, len(name))
	}
}
