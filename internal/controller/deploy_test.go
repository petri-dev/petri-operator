package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/petri-dev/petri-operator/api/v1alpha1"
	"github.com/petri-dev/petri-operator/internal/deployer"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestParallelSharedSubmitStatus(t *testing.T) {
	t.Parallel()
	for _, existingStatus := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing-status=%t", existingStatus), func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			s := runtime.NewScheme()
			g.Expect(scheme.AddToScheme(s)).To(Succeed())
			g.Expect(v1alpha1.AddToScheme(s)).To(Succeed())
			env := &v1alpha1.EphemeralEnvironment{ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "management"}}
			env.Spec.Template = "template"
			level := []v1alpha1.ComponentSpec{
				{Name: "db1", SharedComponentRef: "db1"},
				{Name: "db2", SharedComponentRef: "db2"},
				{Name: "db3", SharedComponentRef: "db3"},
				{Name: "db4", SharedComponentRef: "db4"},
				{Name: "app", Helm: helmSpec()},
			}
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "workload", Labels: map[string]string{}}}
			provider := &v1alpha1.SharedComponentProvider{
				ObjectMeta: metav1.ObjectMeta{Name: "provisioned", Namespace: env.Namespace},
				Spec:       v1alpha1.SharedComponentProviderSpec{Provision: &v1alpha1.JobScript{Image: "busybox", Script: "true"}},
			}
			objects := []client.Object{provider, &v1alpha1.EnvironmentTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: env.Spec.Template, Namespace: env.Namespace},
				Spec:       v1alpha1.EnvironmentTemplateSpec{Components: level},
			}}
			for i, component := range level {
				if existingStatus {
					env.Status.Components = append(env.Status.Components, v1alpha1.ComponentStatus{
						Name: component.Name, Phase: v1alpha1.ComponentPhasePending, DeployRetries: 2, LastFailureReason: "previous failure",
					})
				}
				if component.SharedComponentRef == "" {
					continue
				}
				ns.Labels[sharedLabel(component.SharedComponentRef)] = "true"
				providerName := "provider"
				if i >= 2 {
					providerName = provider.Name
				}
				objects = append(objects, &v1alpha1.SharedComponent{
					ObjectMeta: metav1.ObjectMeta{Name: component.SharedComponentRef, Namespace: env.Namespace},
					Spec:       v1alpha1.SharedComponentSpec{Provider: providerName},
					Status:     v1alpha1.SharedComponentStatus{Ready: true},
				})
			}
			objects = append(objects, ns, &v1alpha1.SharedComponentProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: env.Namespace}})
			live := fake.NewClientBuilder().WithScheme(s).WithObjects(objects...).Build()
			entered, release := make(chan struct{}, 4), make(chan struct{})
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			blocked := interceptor.NewClient(live, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					err := c.Create(ctx, obj, opts...)
					if _, ok := obj.(*corev1.Secret); ok && obj.GetNamespace() == ns.Name {
						entered <- struct{}{}
						select {
						case <-release:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return err
				},
			})
			r := &EphemeralEnvironmentReconciler{Client: blocked, APIReader: live, Deployer: newFakeDeployer(), Provisioner: newFakeProvisioner()}
			before := env.DeepCopy()
			result := make(chan error, 1)
			go func() {
				_, err := r.processLevel(ctx, env, ns.Name, level, nil, time.Minute)
				result <- err
			}()
			for range 4 {
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("shared submissions did not reach binding creation concurrently")
				}
			}
			g.Expect(env).To(Equal(before))
			close(release)
			g.Expect(<-result).To(Succeed())
			g.Expect(env.Status.Components).To(HaveLen(len(level)))
			for i, component := range level {
				phase := v1alpha1.ComponentPhaseSubmitting
				if i < 2 {
					phase = v1alpha1.ComponentPhaseReady
				}
				want := &v1alpha1.ComponentStatus{Name: component.Name, Phase: phase, Shared: component.SharedComponentRef != ""}
				if existingStatus && phase != v1alpha1.ComponentPhaseReady {
					want.DeployRetries, want.LastFailureReason = 2, "previous failure"
				}
				g.Expect(findComponent(env, component.Name)).To(Equal(want))
			}
			g.Expect(env.ObjectMeta).To(Equal(before.ObjectMeta))
			g.Expect(env.Spec).To(Equal(before.Spec))
		})
	}
}

func TestReadinessRetryLifecycle(t *testing.T) {
	t.Parallel()
	env := &v1alpha1.EphemeralEnvironment{}
	component := v1alpha1.ComponentSpec{Name: "app"}
	setComponentPhase(env, component.Name, v1alpha1.ComponentPhaseDeploying)
	setComponentDeployingSince(env, component.Name, metav1.NewTime(time.Now().Add(-time.Hour)))
	sibling := v1alpha1.ComponentSpec{Name: "sibling"}
	setComponentPhase(env, sibling.Name, v1alpha1.ComponentPhaseDeploying)
	checker := newFakeChecker()
	checker.setReady("-sibling", true)
	r := &EphemeralEnvironmentReconciler{Checker: checker}
	components := []v1alpha1.ComponentSpec{component, sibling}
	res, err := r.checkReadiness(t.Context(), env, "workload", components, time.Minute)
	if err != nil || res.RequeueAfter != deployBackoff(env, components) {
		t.Fatalf("readiness retry: %+v, %v", res, err)
	}
	if findComponent(env, sibling.Name).Phase != v1alpha1.ComponentPhaseReady {
		t.Fatal("timeout must not skip checking the next component")
	}
	cs := findComponent(env, component.Name)
	if cs.Phase != v1alpha1.ComponentPhasePending || cs.DeployRetries != 1 || cs.DeployingSince != nil {
		t.Fatalf("retry status: %+v", cs)
	}
	if r.deployOpts(env, "workload", component).Attempt != 1 {
		t.Fatal("retry must change Job identity")
	}
	for i := int32(2); i <= maxDeployRetries; i++ {
		exhausted := recordRuntimeFailure(env, component.Name, "failed")
		if exhausted != (i == maxDeployRetries) {
			t.Fatalf("exhausted=%v at attempt %d", exhausted, i)
		}
	}
	resetComponentFailure(env, component.Name)
	if cs.DeployRetries != 0 || cs.LastFailureReason != "" {
		t.Fatalf("ready reset: %+v", cs)
	}
}

var _ = Describe("Deploy Job lifecycle", func() {
	It("applies rendered values to a new Job and retries a failed Job with a new UID", func(ctx SpecContext) {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "deploy-lifecycle-"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		component := v1alpha1.ComponentSpec{Name: "svc", Helm: helmSpec()}
		tmpl := &v1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "tmpl", Namespace: ns.Name}, Spec: v1alpha1.EnvironmentTemplateSpec{Components: []v1alpha1.ComponentSpec{component}}}
		Expect(k8sClient.Create(ctx, tmpl)).To(Succeed())
		env := &v1alpha1.EphemeralEnvironment{ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: ns.Name}, Spec: v1alpha1.EphemeralEnvironmentSpec{
			Template: tmpl.Name, Source: v1alpha1.SourceSpec{Repo: "https://example.com/repo", Branch: "main"}, Values: map[string]string{"replicaCount": "1"},
		}}
		Expect(k8sClient.Create(ctx, env)).To(Succeed())
		key := client.ObjectKeyFromObject(env)
		checker := newFakeChecker()
		checker.setReady("env-svc", true)
		r := &EphemeralEnvironmentReconciler{Client: k8sClient, Scheme: scheme.Scheme, Checker: checker,
			Deployer: &deployer.JobDeployer{Client: k8sClient, Reader: k8sClient, Image: "deployer:test"}}
		step := func() {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, key, env)).To(Succeed())
		}
		job := &batchv1.Job{}
		Eventually(func() error {
			step()
			return k8sClient.Get(ctx, client.ObjectKey{Namespace: env.Status.TargetNamespace, Name: "petri-deploy-env-svc"}, job)
		}, "10s", "10ms").Should(Succeed())
		jobKey := client.ObjectKeyFromObject(job)
		finish := func(condition batchv1.JobConditionType) {
			finishJob(ctx, job, condition)
		}
		finish(batchv1.JobComplete)
		Eventually(func() v1alpha1.EnvironmentPhase { step(); return env.Status.Phase }, "10s", "10ms").Should(Equal(v1alpha1.EnvironmentPhaseReady))
		originalUID := job.UID
		env.Spec.Values["replicaCount"] = "2"
		Expect(k8sClient.Update(ctx, env)).To(Succeed())
		// Envtest has no garbage collector. Hold the foreground deletion to check
		// repeated reconciles do not consume retries or accept the old success.
		Eventually(func() bool {
			step()
			Expect(k8sClient.Get(ctx, jobKey, job)).To(Succeed())
			return !job.DeletionTimestamp.IsZero()
		}, "10s", "10ms").Should(BeTrue())
		for range 4 {
			step()
		}
		Expect(env.Status.Phase).NotTo(Equal(v1alpha1.EnvironmentPhaseReady))
		Expect(findComponent(env, "svc").DeployRetries).To(BeZero())
		Expect(job.UID).To(Equal(originalUID))
		job.Finalizers = nil
		Expect(k8sClient.Update(ctx, job)).To(Succeed())
		Eventually(func() bool { step(); return k8sClient.Get(ctx, jobKey, job) == nil && job.UID != originalUID }, "10s", "10ms").Should(BeTrue())
		var payload deployer.DeployOptions
		for _, ev := range job.Spec.Template.Spec.Containers[0].Env {
			if ev.Name == deployer.EnvSpec {
				Expect(json.Unmarshal([]byte(ev.Value), &payload)).To(Succeed())
			}
		}
		Expect(payload.Component.Helm.Values["replicaCount"]).To(Equal("2"))
		finish(batchv1.JobFailed)
		failedUID := job.UID
		Eventually(func() bool {
			step()
			Expect(k8sClient.Get(ctx, jobKey, job)).To(Succeed())
			return !job.DeletionTimestamp.IsZero()
		}, "10s", "10ms").Should(BeTrue())
		for range 4 {
			step()
		}
		Expect(findComponent(env, "svc").DeployRetries).To(Equal(int32(1)))
		job.Finalizers = nil
		Expect(k8sClient.Update(ctx, job)).To(Succeed())
		Eventually(func() bool { step(); return k8sClient.Get(ctx, jobKey, job) == nil && job.UID != failedUID }, "10s", "10ms").Should(BeTrue())
		Expect(findComponent(env, "svc").DeployRetries).To(Equal(int32(1)))
		for _, ev := range job.Spec.Template.Spec.Containers[0].Env {
			if ev.Name == deployer.EnvSpec {
				Expect(json.Unmarshal([]byte(ev.Value), &payload)).To(Succeed())
			}
		}
		Expect(payload.Attempt).To(Equal(int32(1)))
		finish(batchv1.JobComplete)
		Eventually(func() v1alpha1.EnvironmentPhase { step(); return env.Status.Phase }, "10s", "10ms").Should(Equal(v1alpha1.EnvironmentPhaseReady))
		Expect(findComponent(env, "svc").DeployRetries).To(BeZero())
	})
})

func finishJob(ctx SpecContext, job *batchv1.Job, condition batchv1.JobConditionType) {
	now := metav1.Now()
	job.Status.StartTime = &now
	prerequisite := batchv1.JobFailureTarget
	if condition == batchv1.JobComplete {
		prerequisite = batchv1.JobSuccessCriteriaMet
		job.Status.CompletionTime = &now
		job.Status.Succeeded = 1
	}
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: prerequisite, Status: corev1.ConditionTrue, Reason: "TestResult"},
		{Type: condition, Status: corev1.ConditionTrue, Reason: "TestResult"},
	}
	Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
}
