package deployer_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/onsi/gomega"
	"github.com/petri-dev/petri-operator/internal/deployer"
	"github.com/petri-dev/petri-operator/internal/provisioner"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestCachedJobs(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"deploy", "undeploy", "provision", "deprovision"} {
		for _, terminal := range []batchv1.JobConditionType{batchv1.JobComplete, batchv1.JobFailed} {
			for _, race := range []string{"same", "replaced", "missing", "deleting", "running", "payload", "conflict", "cache-miss", "foreign"} {
				t.Run(op+"/"+string(terminal)+"/"+race, func(t *testing.T) {
					t.Parallel()
					g := gomega.NewWithT(t)
					s := runtime.NewScheme()
					g.Expect(batchv1.AddToScheme(s)).To(gomega.Succeed())
					g.Expect(corev1.AddToScheme(s)).To(gomega.Succeed())
					live := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&batchv1.Job{}).Build()
					jd := &deployer.JobDeployer{Client: live, Reader: live}
					jp := &provisioner.JobProvisioner{Client: live, Reader: live}
					do := deployer.DeployOptions{Namespace: "workload", ReleaseName: "app"}
					po := provisioner.ProvisionOptions{EnvName: "env", ComponentName: "db"}
					submit := func() error { return jd.Submit(t.Context(), do) }
					observe := func() (deployer.JobState, error) { return jd.Observe(t.Context(), do) }
					switch op {
					case "undeploy":
						submit = func() error { return jd.SubmitUndeploy(t.Context(), do) }
						observe = func() (deployer.JobState, error) { return jd.ObserveUndeploy(t.Context(), do) }
					case "provision":
						submit = func() error { return jp.SubmitProvision(t.Context(), po) }
						observe = func() (deployer.JobState, error) { return jp.ObserveProvision(t.Context(), po) }
					case "deprovision":
						submit = func() error { return jp.SubmitDeprovision(t.Context(), po) }
						observe = func() (deployer.JobState, error) { return jp.ObserveDeprovision(t.Context(), po) }
					}
					g.Expect(submit()).To(gomega.Succeed())
					jobs := new(batchv1.JobList)
					g.Expect(live.List(t.Context(), jobs)).To(gomega.Succeed())
					a := jobs.Items[0].DeepCopy()
					a.UID = "A"
					g.Expect(live.Update(t.Context(), a)).To(gomega.Succeed())
					a.Status.Conditions = []batchv1.JobCondition{{Type: terminal, Status: corev1.ConditionTrue}}
					g.Expect(live.Status().Update(t.Context(), a)).To(gomega.Succeed())
					cacheObjects := []client.Object{a.DeepCopy()}
					b := a.DeepCopy()
					switch race {
					case "replaced":
						b.UID = "B"
					case "missing":
						g.Expect(live.Delete(t.Context(), a)).To(gomega.Succeed())
					case "deleting":
						b.Finalizers = []string{"test/hold"}
					case "running":
						b.Status.Conditions = nil
					case "payload":
						b.Annotations = nil
						b.Spec.Template.Spec.Containers[0].Env = nil
					case "cache-miss", "foreign":
						cacheObjects = nil
						if race == "foreign" {
							b.Labels = nil
						}
					}
					if race != "missing" {
						g.Expect(live.Update(t.Context(), b)).To(gomega.Succeed())
						if race == "running" {
							b.Status.Conditions = nil
						}
						g.Expect(live.Status().Update(t.Context(), b)).To(gomega.Succeed())
						if race == "deleting" {
							g.Expect(live.Delete(t.Context(), b)).To(gomega.Succeed())
						}
					}
					deletes, gets := 0, 0
					cached := fake.NewClientBuilder().WithScheme(s).WithObjects(cacheObjects...).WithInterceptorFuncs(interceptor.Funcs{
						Create: func(ctx context.Context, _ client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
							return live.Create(ctx, obj, opts...)
						},
						Delete: func(_ context.Context, _ client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
							deletes++
							d := new(client.DeleteOptions)
							for _, opt := range opts {
								opt.ApplyToDelete(d)
							}
							g.Expect(*d.Preconditions.UID).To(gomega.Equal(a.UID))
							g.Expect(*d.PropagationPolicy).To(gomega.Equal(metav1.DeletePropagationForeground))
							if race == "conflict" {
								return apierrors.NewConflict(schema.GroupResource{Resource: "jobs"}, obj.GetName(), fmt.Errorf("replaced after GET"))
							}
							return nil
						},
						List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
							t.Fatal("diagnostics must not use cache")
							return nil
						},
					}).Build()
					reader := interceptor.NewClient(live, interceptor.Funcs{
						Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
							gets++
							return c.Get(ctx, key, obj, opts...)
						},
						List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
							lo := new(client.ListOptions)
							for _, opt := range opts {
								opt.ApplyToList(lo)
							}
							g.Expect(lo.Namespace).To(gomega.Equal(a.Namespace))
							g.Expect(lo.LabelSelector.String()).To(gomega.Equal("job-name=" + a.Name))
							return c.List(ctx, list, opts...)
						},
					})
					jd.Client, jd.Reader, jp.Client, jp.Reader = cached, reader, cached, reader
					state, err := observe()
					g.Expect(err).NotTo(gomega.HaveOccurred())
					want := deployer.PendingJobPhase
					if race == "same" || race == "conflict" {
						want = deployer.TerminalPhase(a)
					}
					g.Expect(state.Phase).To(gomega.Equal(want))
					gets = 0
					err = submit()
					if race == "foreign" {
						g.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("not managed")))
					} else {
						g.Expect(err).NotTo(gomega.HaveOccurred())
					}
					wantDeletes := 0
					if terminal == batchv1.JobFailed && (race == "same" || race == "conflict" || race == "payload") {
						wantDeletes = 1
					}
					g.Expect(deletes).To(gomega.Equal(wantDeletes))
					if terminal == batchv1.JobComplete && len(cacheObjects) != 0 {
						g.Expect(gets).To(gomega.BeZero())
					}
				})
			}
		}
	}
}
