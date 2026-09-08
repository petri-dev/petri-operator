package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/petri-dev/petri-operator/api/v1alpha1"
	"github.com/petri-dev/petri-operator/internal/deployer"
	"github.com/petri-dev/petri-operator/internal/provisioner"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	. "github.com/onsi/gomega"
)

func TestStaleProvisionResultRetainsCredentials(t *testing.T) {
	t.Parallel()
	for _, deprovision := range []bool{false, true} {
		for _, terminal := range []batchv1.JobConditionType{batchv1.JobFailed, batchv1.JobComplete} {
			t.Run(map[bool]string{false: "provision", true: "deprovision"}[deprovision]+"/"+string(terminal), func(t *testing.T) {
				t.Parallel()
				g := NewWithT(t)
				r, env := namespaceFixture(t)
				env.Status.TargetNamespace = "workload"
				component := v1alpha1.ComponentSpec{Name: "db", SharedComponentRef: "db"}
				env.Status.Components = []v1alpha1.ComponentStatus{{Name: "db", Phase: v1alpha1.PhaseSubmitting}}
				sc := &v1alpha1.SharedComponent{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: env.Namespace}, Spec: v1alpha1.SharedComponentSpec{Provider: "provider"}}
				script := &v1alpha1.JobScript{Image: "busybox", Script: "true"}
				scp := &v1alpha1.SharedComponentProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: env.Namespace}, Spec: v1alpha1.SharedComponentProviderSpec{Provision: script, Deprovision: script}}
				binding := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: env.Name + "-db-binding", Namespace: env.Status.TargetNamespace}, Data: map[string][]byte{generatedSecretKey: []byte("secret")}}
				for _, obj := range []client.Object{sc, scp, binding} {
					g.Expect(r.Create(t.Context(), obj)).To(Succeed())
				}
				live := r.Client.(client.WithWatch)
				p := &provisioner.JobProvisioner{Client: live, Reader: live}
				opts, err := renderProvisionOptions(env, component, sc, script, "secret", nil)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(r.ensureProvisionSecret(t.Context(), opts.ProvisionerSecretRef, env.Name, "secret")).To(Succeed())
				if deprovision {
					g.Expect(p.SubmitDeprovision(t.Context(), opts)).To(Succeed())
				} else {
					g.Expect(p.SubmitProvision(t.Context(), opts)).To(Succeed())
				}
				jobs := new(batchv1.JobList)
				g.Expect(live.List(t.Context(), jobs)).To(Succeed())
				a := jobs.Items[0].DeepCopy()
				a.UID = "A"
				a.Status.Conditions = []batchv1.JobCondition{{Type: terminal, Status: corev1.ConditionTrue}}
				cache := fake.NewClientBuilder().WithScheme(live.Scheme()).WithObjects(a).Build()
				b := jobs.Items[0].DeepCopy()
				b.UID = "B"
				g.Expect(live.Update(t.Context(), b)).To(Succeed())
				b.Status = a.Status
				g.Expect(live.Status().Update(t.Context(), b)).To(Succeed())
				p.Client = interceptor.NewClient(live, interceptor.Funcs{
					Get: func(ctx context.Context, _ client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						return cache.Get(ctx, key, obj, opts...)
					},
					Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
						t.Fatal("stale result must not delete replacement")
						return nil
					},
				})
				r.Provisioner, r.APIReader = p, live
				var done bool
				if deprovision {
					done, _, err = r.deprovisionShared(t.Context(), env, env.Status.TargetNamespace, &v1alpha1.EnvironmentTemplate{Spec: v1alpha1.EnvironmentTemplateSpec{Components: []v1alpha1.ComponentSpec{component}}})
				} else {
					done, err = r.observeShared(t.Context(), env, component)
				}
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(done).To(BeFalse())
				g.Expect(findComponent(env, "db").DeployRetries).To(BeZero())
				g.Expect(live.Get(t.Context(), client.ObjectKeyFromObject(binding), new(corev1.Secret))).To(Succeed())
				g.Expect(live.Get(t.Context(), client.ObjectKey{Name: opts.ProvisionerSecretRef, Namespace: sharedNamespace}, new(corev1.Secret))).To(Succeed())
			})
		}
	}
}

func TestSharedUninstallLiveBarrier(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"missing", "running", "deleting", "terminal"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			live, cached, sc, _ := sharedAdmissionClients(t, 0)
			g.Expect(live.Delete(t.Context(), sc)).To(Succeed())
			g.Expect(live.Get(t.Context(), client.ObjectKeyFromObject(sc), sc)).To(Succeed())
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: deployer.DeployJobName("shared-db"), Namespace: sharedNamespace}}
			if state == "terminal" || state == "deleting" {
				job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			}
			if state == "deleting" {
				job.Finalizers = []string{"test/hold"}
			}
			if state != "missing" {
				g.Expect(live.Create(t.Context(), job)).To(Succeed())
			}
			if state == "deleting" {
				g.Expect(live.Delete(t.Context(), job)).To(Succeed())
			}
			fd := newFakeDeployer()
			sr := &SharedComponentReconciler{Client: cached, APIReader: live, Deployer: fd}
			_, err := sr.reconcileDelete(t.Context(), sc)
			g.Expect(err).NotTo(HaveOccurred())
			if state == "running" || state == "deleting" {
				g.Expect(fd.calls).To(BeEmpty())
			} else {
				g.Expect(fd.undeployOrder()).To(Equal([]string{"shared-db"}))
			}
		})
	}
}

func TestRunningUndeployDoesNotSubmit(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	r, env := namespaceFixture(t)
	j := &deployer.JobDeployer{Client: r.Client, Reader: r.Client}
	r.Deployer = j
	component := v1alpha1.ComponentSpec{Name: "app"}
	g.Expect(j.SubmitUndeploy(t.Context(), r.deployOpts(env, "workload", component))).To(Succeed())
	gets := 0
	j.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			gets++
			return c.Get(ctx, key, obj, opts...)
		},
		Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
			t.Fatal("running undeploy submitted")
			return nil
		},
	})
	done, _, err := r.undeployAll(t.Context(), env, "workload", []v1alpha1.ComponentSpec{component})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(done).To(BeFalse())
	g.Expect(gets).To(Equal(1))
}

func TestSubmitRacesDoNotChargeRetry(t *testing.T) {
	t.Parallel()
	for _, race := range []string{"cache-miss", "delete-conflict"} {
		t.Run(race, func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			r, env := namespaceFixture(t)
			live := r.Client.(client.WithWatch)
			j := &deployer.JobDeployer{Client: live, Reader: live}
			r.Deployer = j
			component := v1alpha1.ComponentSpec{Name: "app"}
			g.Expect(j.Submit(t.Context(), r.deployOpts(env, "workload", component))).To(Succeed())
			job := new(batchv1.Job)
			key := client.ObjectKey{Namespace: "workload", Name: deployer.DeployJobName(env.Name + "-app")}
			g.Expect(live.Get(t.Context(), key, job)).To(Succeed())
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
			g.Expect(live.Status().Update(t.Context(), job)).To(Succeed())
			j.Client = interceptor.NewClient(live, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if race == "cache-miss" {
						return apierrors.NewNotFound(schema.GroupResource{Resource: "jobs"}, key.Name)
					}
					return c.Get(ctx, key, obj, opts...)
				},
				Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
					return apierrors.NewConflict(schema.GroupResource{Resource: "jobs"}, key.Name, fmt.Errorf("replaced"))
				},
			})
			_, err := r.submitDeploys(t.Context(), env, "workload", []v1alpha1.ComponentSpec{component})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(findComponent(env, component.Name).DeployRetries).To(BeZero())
			g.Expect(findComponent(env, component.Name).Phase).To(Equal(v1alpha1.PhaseSubmitting))
		})
	}
}

func TestNamespaceCandidateCacheMissChecksLiveOwnership(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	r, env := namespaceFixture(t)
	live := r.Client
	r.APIReader = live
	g.Expect(r.allocateNamespace(t.Context(), env, 8)).To(Succeed())
	foreign := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: env.Status.TargetNamespace}}
	g.Expect(live.Create(t.Context(), foreign)).To(Succeed())
	r.Client = fake.NewClientBuilder().WithScheme(live.Scheme()).Build()
	g.Expect(r.allocateNamespace(t.Context(), env, 8)).To(Succeed())
	g.Expect(env.Status.TargetNamespace).To(Equal(foreign.Name))
	g.Expect(r.createNamespace(t.Context(), env)).To(MatchError(ContainSubstring("not managed")))
	accounts := new(corev1.ServiceAccountList)
	g.Expect(r.List(t.Context(), accounts)).To(Succeed())
	g.Expect(accounts.Items).To(BeEmpty())
}
