package deployer

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/petri-dev/petri-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestJobPayloadAndReplacement(t *testing.T) {
	t.Parallel()
	for _, op := range []string{OpDeploy, OpUndeploy} {
		for _, tc := range []struct {
			name       string
			condition  batchv1.JobConditionType
			change     bool
			attempt    int32
			deleting   bool
			wantPhase  JobPhase
			wantDelete bool
		}{
			{name: "running identical", wantPhase: RunningJobPhase},
			{name: "running changed", change: true, wantPhase: RunningJobPhase},
			{name: "complete identical", condition: batchv1.JobComplete, wantPhase: SucceededJobPhase},
			{name: "complete changed", condition: batchv1.JobComplete, change: true, wantPhase: PendingJobPhase, wantDelete: true},
			{name: "readiness retry", condition: batchv1.JobComplete, attempt: 1, wantPhase: PendingJobPhase, wantDelete: true},
			{name: "failed", condition: batchv1.JobFailed, wantPhase: FailedJobPhase, wantDelete: true},
			{name: "failed previous attempt", condition: batchv1.JobFailed, attempt: 1, wantPhase: PendingJobPhase, wantDelete: true},
			{name: "deleting", condition: batchv1.JobFailed, deleting: true, wantPhase: PendingJobPhase},
		} {
			t.Run(op+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				scheme := runtime.NewScheme()
				if err := batchv1.AddToScheme(scheme); err != nil {
					t.Fatal(err)
				}
				if err := corev1.AddToScheme(scheme); err != nil {
					t.Fatal(err)
				}
				opts := DeployOptions{Namespace: "workload", ReleaseName: "env-app", Component: v1alpha1.ComponentSpec{
					Name: "app", Helm: &v1alpha1.HelmSpec{Chart: "app", Values: map[string]string{"replicaCount": "1"}},
				}}
				payload, err := json.Marshal(opts)
				if err != nil {
					t.Fatal(err)
				}
				j := &JobDeployer{Image: "deployer:test"}
				old := j.buildJob(opts, op, string(payload))
				old.UID = "original"
				if tc.condition != "" {
					old.Status.Conditions = []batchv1.JobCondition{{Type: tc.condition, Status: corev1.ConditionTrue}}
				}
				if tc.deleting {
					now := metav1.Now()
					old.DeletionTimestamp = &now
					old.Finalizers = []string{"test/hold"}
				}
				deletes := 0
				j.Client = fake.NewClientBuilder().WithScheme(scheme).WithObjects(old).WithInterceptorFuncs(interceptor.Funcs{
					Delete: func(_ context.Context, _ client.WithWatch, obj client.Object, options ...client.DeleteOption) error {
						deletes++
						del := &client.DeleteOptions{}
						for _, option := range options {
							option.ApplyToDelete(del)
						}
						if del.Preconditions == nil || del.Preconditions.UID == nil || *del.Preconditions.UID != old.UID {
							t.Fatal("delete must guard the observed UID")
						}
						if del.PropagationPolicy == nil || *del.PropagationPolicy != metav1.DeletePropagationForeground {
							t.Fatal("delete must wait for old Pods")
						}
						if obj.GetUID() != old.UID {
							t.Fatal("unexpected deletion")
						}
						return nil // API accepted deletion, but the name is still occupied.
					},
				}).Build()
				j.Reader = j.Client
				if tc.change {
					opts.Component.Helm.Values["replicaCount"] = "2"
				}
				opts.Attempt = tc.attempt
				state, err := j.observe(ctx, opts, op)
				if err != nil || state.Phase != tc.wantPhase {
					t.Fatalf("Observe = %+v, %v; want %s", state, err, tc.wantPhase)
				}
				if err := j.submit(ctx, opts, op); err != nil {
					t.Fatal(err)
				}
				if (deletes == 1) != tc.wantDelete {
					t.Fatalf("deletes = %d", deletes)
				}
				jobs := &batchv1.JobList{}
				if err := j.Client.List(ctx, jobs); err != nil {
					t.Fatal(err)
				}
				if len(jobs.Items) != 1 || jobs.Items[0].UID != old.UID {
					t.Fatal("duplicated or replaced a Job before deletion completed")
				}
			})
		}
	}
}

func TestJobCreateRace(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	base := fake.NewClientBuilder().WithScheme(scheme).Build()
	j := &JobDeployer{Client: base, Reader: base, Image: "deployer:test"}
	opts := DeployOptions{Namespace: "workload", ReleaseName: "env-app"}
	if err := j.Submit(t.Context(), opts); err != nil {
		t.Fatal(err)
	}
	// A stale cache says NotFound, but the API already has the running Job.
	j.Client = fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, _ client.WithWatch, obj client.Object, options ...client.CreateOption) error {
			return base.Create(ctx, obj, options...)
		},
	}).Build()
	j.Reader = base
	if err := j.Submit(t.Context(), opts); err != nil {
		t.Fatal(err)
	}
	job := &batchv1.Job{}
	if err := base.Get(t.Context(), client.ObjectKey{Namespace: opts.Namespace, Name: jobName(OpDeploy, opts.ReleaseName)}, job); err != nil {
		t.Fatal(err)
	}
}
