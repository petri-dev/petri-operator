package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/petri-dev/petri-operator/api/v1alpha1"
	"github.com/petri-dev/petri-operator/internal/deployer"
	"github.com/petri-dev/petri-operator/internal/provisioner"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type deprovisionOptionsRecorder struct {
	provisioner.Provisioner
	observed, submitted []provisioner.ProvisionOptions
}

func (p *deprovisionOptionsRecorder) ObserveDeprovision(_ context.Context, opts provisioner.ProvisionOptions) (deployer.JobState, error) {
	p.observed = append(p.observed, opts)
	return deployer.JobState{Phase: deployer.PendingJobPhase}, nil
}

func (p *deprovisionOptionsRecorder) SubmitDeprovision(_ context.Context, opts provisioner.ProvisionOptions) error {
	p.submitted = append(p.submitted, opts)
	return nil
}

func TestDeprovisionInstanceOptions(t *testing.T) {
	t.Parallel()
	component := v1alpha1.ComponentSpec{Name: "db", SharedComponentRef: "db"}
	tmpl := &v1alpha1.EnvironmentTemplate{Spec: v1alpha1.EnvironmentTemplateSpec{Components: []v1alpha1.ComponentSpec{component}}}
	setup := func(t *testing.T, script *v1alpha1.JobScript) (*EphemeralEnvironmentReconciler, *v1alpha1.EphemeralEnvironment, *corev1.Secret) {
		t.Helper()
		g := NewWithT(t)
		r, env := namespaceFixture(t)
		scp := &v1alpha1.SharedComponentProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: env.Namespace}, Spec: v1alpha1.SharedComponentProviderSpec{
			InstanceSecret: &v1alpha1.InstanceSecret{Name: "instance"}, Deprovision: script,
		}}
		sc := &v1alpha1.SharedComponent{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: env.Namespace}, Spec: v1alpha1.SharedComponentSpec{Provider: scp.Name}}
		binding := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: env.Name + "-db-binding", Namespace: "workload"}, Data: map[string][]byte{generatedSecretKey: []byte("generated")}}
		for _, obj := range []client.Object{scp, sc, binding} {
			g.Expect(r.Create(t.Context(), obj)).To(Succeed())
		}
		for _, ns := range []string{env.Namespace, binding.Namespace, sharedNamespace} {
			g.Expect(r.Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "instance", Namespace: ns}, Data: map[string][]byte{"host": []byte(ns)}})).To(Succeed())
		}
		return r, env, binding
	}

	for _, tt := range []struct {
		name             string
		script, rendered v1alpha1.JobScript
	}{
		{
			name:     "script",
			script:   v1alpha1.JobScript{Image: "busybox", Script: "delete {{ .Instance.host }} {{ .Env.Name }}"},
			rendered: v1alpha1.JobScript{Image: "busybox", Script: "delete " + sharedNamespace + " shared"},
		},
		{
			name:     "command",
			script:   v1alpha1.JobScript{Image: "busybox", Command: []string{"delete", "{{ .Instance.host }}", "{{ .Env.Name }}"}},
			rendered: v1alpha1.JobScript{Image: "busybox", Command: []string{"delete", sharedNamespace, "shared"}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			r, env, binding := setup(t, &tt.script)
			p := new(deprovisionOptionsRecorder)
			r.Provisioner = p

			done, res, err := r.deprovisionShared(t.Context(), env, binding.Namespace, tmpl)
			g.Expect(done).To(BeFalse())
			g.Expect(r.Get(t.Context(), client.ObjectKeyFromObject(binding), new(corev1.Secret))).To(Succeed())
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			g.Expect(p.observed).To(HaveLen(1))
			g.Expect(p.submitted).To(Equal(p.observed))
			g.Expect(p.observed[0]).To(Equal(provisioner.ProvisionOptions{
				EnvUID: env.UID, EnvName: env.Name, ComponentName: component.Name, SharedName: component.SharedComponentRef,
				Script: tt.rendered, ProvisionerSecretRef: provisioner.ProvisionJobName(env.Name, component.Name) + "-credentials",
			}))
		})
	}

	t.Run("missing", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)
		r, env, binding := setup(t, &v1alpha1.JobScript{Image: "busybox", Script: "delete {{ .Instance.host }} {{ .Env.Name }}"})
		g.Expect(r.Delete(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "instance", Namespace: sharedNamespace}})).To(Succeed())
		p := new(deprovisionOptionsRecorder)
		r.Provisioner = p

		done, _, err := r.deprovisionShared(t.Context(), env, binding.Namespace, tmpl)
		g.Expect(done).To(BeFalse())
		g.Expect(err).To(HaveOccurred())
		g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
		g.Expect(r.Get(t.Context(), client.ObjectKeyFromObject(binding), new(corev1.Secret))).To(Succeed())
		g.Expect(p.observed).To(BeEmpty())
		g.Expect(p.submitted).To(BeEmpty())
	})

	t.Run("read-error", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)
		r, env, binding := setup(t, &v1alpha1.JobScript{Image: "busybox", Script: "delete {{ .Instance.host }} {{ .Env.Name }}"})
		failure := errors.New("instance secret unavailable")
		r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if key.Name == "instance" && key.Namespace == sharedNamespace {
					return failure
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})
		p := new(deprovisionOptionsRecorder)
		r.Provisioner = p

		done, _, err := r.deprovisionShared(t.Context(), env, binding.Namespace, tmpl)
		g.Expect(done).To(BeFalse())
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError(failure))
		g.Expect(r.Get(t.Context(), client.ObjectKeyFromObject(binding), new(corev1.Secret))).To(Succeed())
		g.Expect(p.observed).To(BeEmpty())
		g.Expect(p.submitted).To(BeEmpty())
	})
}

var _ = Describe("Shared Job lifecycle", func() {
	It("recovers after repeated namespace deletion and failed finalizer patches", func(ctx SpecContext) {
		tctx = context.Background()
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "cleanup-recovery-"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		makeTemplate(ns.Name, "template", []v1alpha1.ComponentSpec{{Name: "app", Helm: helmSpec()}})
		key := makeEnv(ns.Name, "env", "template")
		r := newReconciler(newFakeDeployer(), newFakeChecker())
		Expect(reconcileUntilTerminal(r, key, 15)).To(Equal(v1alpha1.EnvironmentPhaseReady))

		env := getEnv(key)
		Expect(k8sClient.Delete(ctx, env)).To(Succeed())
		for range 5 {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			env = getEnv(key)
			if meta.IsStatusConditionTrue(env.Status.Conditions, cleanupComplete) {
				break
			}
		}
		Expect(meta.IsStatusConditionTrue(env.Status.Conditions, cleanupComplete)).To(BeTrue())

		live, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		failure := errors.New("finalizer patch unavailable")
		faults := interceptor.NewClient(live, interceptor.Funcs{
			Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
				return failure
			},
			SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
				Fail("must not patch status after attempting finalizer removal")
				return nil
			},
		})
		r = &EphemeralEnvironmentReconciler{Client: faults, APIReader: live}
		for range 2 {
			_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			Expect(err).To(MatchError(failure))
			Expect(getEnv(key).Finalizers).To(ContainElement(finalizer))
		}

		target := new(corev1.Namespace)
		Expect(live.Get(ctx, client.ObjectKey{Name: env.Status.TargetNamespace}, target)).To(Succeed())
		Expect(target.DeletionTimestamp.IsZero()).To(BeFalse())
		// Envtest has no namespace controller to finish an accepted DELETE.
		target.Spec.Finalizers = nil
		Expect(live.SubResource("finalize").Update(ctx, target)).To(Succeed())
		Expect(apierrors.IsNotFound(live.Get(ctx, client.ObjectKeyFromObject(target), target))).To(BeTrue())

		r = &EphemeralEnvironmentReconciler{Client: live, APIReader: live}
		_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(apierrors.IsNotFound(live.Get(ctx, key, env))).To(BeTrue())
	})

	It("retains shared instance success and replaces failed and changed instance Jobs", func(ctx SpecContext) {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "instance-lifecycle-"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		scp := &v1alpha1.SharedComponentProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: ns.Name}, Spec: v1alpha1.SharedComponentProviderSpec{Helm: helmSpec()}}
		Expect(k8sClient.Create(ctx, scp)).To(Succeed())
		sc := &v1alpha1.SharedComponent{ObjectMeta: metav1.ObjectMeta{Name: ns.Name, Namespace: ns.Name}, Spec: v1alpha1.SharedComponentSpec{Provider: scp.Name}}
		Expect(k8sClient.Create(ctx, sc)).To(Succeed())
		r := &SharedComponentReconciler{Client: k8sClient, Deployer: &deployer.JobDeployer{Client: k8sClient, Reader: k8sClient, Image: "deployer:test"}}
		reconcile := func() {
			_, err := r.reconcile(ctx, sc)
			Expect(err).NotTo(HaveOccurred())
		}

		reconcile()
		job := new(batchv1.Job)
		key := client.ObjectKey{Name: "petri-deploy-shared-" + sc.Name, Namespace: sharedNamespace}
		Expect(k8sClient.Get(ctx, key, job)).To(Succeed())
		finishJob(ctx, job, batchv1.JobFailed)
		uid := job.UID

		reconcile()
		Expect(sc.Status.Ready).To(BeFalse())
		Expect(k8sClient.Get(ctx, key, job)).To(Succeed())
		Expect(job.DeletionTimestamp.IsZero()).To(BeFalse())
		for range 3 {
			reconcile()
		}

		job.Finalizers = nil // Envtest has no garbage collector.
		Expect(k8sClient.Update(ctx, job)).To(Succeed())
		reconcile()
		Expect(k8sClient.Get(ctx, key, job)).To(Succeed())
		Expect(job.UID).NotTo(Equal(uid))
		finishJob(ctx, job, batchv1.JobComplete)
		uid = job.UID

		for range 3 {
			reconcile()
		}
		Expect(sc.Status.Ready).To(BeTrue())
		Expect(k8sClient.Get(ctx, key, job)).To(Succeed())
		Expect(job.UID).To(Equal(uid))
		Expect(job.DeletionTimestamp.IsZero()).To(BeTrue())

		scp.Spec.Helm.Values = map[string]string{"replicaCount": "2"}
		Expect(k8sClient.Update(ctx, scp)).To(Succeed())
		reconcile()
		Expect(sc.Status.Ready).To(BeFalse())
		Expect(k8sClient.Get(ctx, key, job)).To(Succeed())
		Expect(job.DeletionTimestamp.IsZero()).To(BeFalse())
	})

	It("reuses provisioning on values updates and genuinely retries a failed provision Job", func(ctx SpecContext) {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "provision-lifecycle-"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		err := k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: sharedNamespace, Labels: map[string]string{managedLabel: "true"}}})
		Expect(client.IgnoreAlreadyExists(err)).To(Succeed())
		scp := &v1alpha1.SharedComponentProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: ns.Name}, Spec: v1alpha1.SharedComponentProviderSpec{
			Provision: &v1alpha1.JobScript{Image: "busybox", Script: "echo {{ .Env.Name }}"},
		}}
		Expect(k8sClient.Create(ctx, scp)).To(Succeed())
		sc := &v1alpha1.SharedComponent{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: ns.Name}, Spec: v1alpha1.SharedComponentSpec{Provider: scp.Name}}
		Expect(k8sClient.Create(ctx, sc)).To(Succeed())
		sc.Status.Ready = true
		Expect(k8sClient.Status().Update(ctx, sc)).To(Succeed())
		env := &v1alpha1.EphemeralEnvironment{ObjectMeta: metav1.ObjectMeta{Name: ns.Name, Namespace: ns.Name, UID: types.UID(ns.Name)}, Status: v1alpha1.EphemeralEnvironmentStatus{TargetNamespace: ns.Name}}
		component := v1alpha1.ComponentSpec{Name: "db", SharedComponentRef: sc.Name}
		r := &EphemeralEnvironmentReconciler{Client: k8sClient, Provisioner: &provisioner.JobProvisioner{Client: k8sClient, Reader: k8sClient}}
		submit := func() {
			phase, err := r.submitShared(ctx, env, ns.Name, component)
			Expect(err).NotTo(HaveOccurred())
			Expect(phase).To(Equal(v1alpha1.ComponentPhaseSubmitting))
			setComponentPhase(env, component.Name, phase)
			setComponentShared(env, component.Name)
		}
		observe := func() bool {
			done, err := r.observeShared(ctx, env, component)
			Expect(err).NotTo(HaveOccurred())
			return done
		}

		submit()
		key := client.ObjectKey{Name: provisioner.ProvisionJobName(env.Name, component.Name), Namespace: sharedNamespace}
		job := new(batchv1.Job)
		Expect(k8sClient.Get(ctx, key, job)).To(Succeed())
		finishJob(ctx, job, batchv1.JobFailed)
		uid := job.UID
		Expect(observe()).To(BeFalse())
		Expect(findComponent(env, component.Name).DeployRetries).To(Equal(int32(1)))
		Expect(findComponent(env, component.Name).Phase).To(Equal(v1alpha1.ComponentPhasePending))

		submit()
		Expect(k8sClient.Get(ctx, key, job)).To(Succeed())
		for range 3 {
			Expect(observe()).To(BeFalse())
			submit()
		}
		Expect(findComponent(env, component.Name).DeployRetries).To(Equal(int32(1)))

		job.Finalizers = nil
		Expect(k8sClient.Update(ctx, job)).To(Succeed())
		submit()
		Expect(k8sClient.Get(ctx, key, job)).To(Succeed())
		Expect(job.UID).NotTo(Equal(uid))
		finishJob(ctx, job, batchv1.JobComplete)
		uid = job.UID

		sibling := v1alpha1.ComponentSpec{Name: "db2", SharedComponentRef: sc.Name}
		phase, err := r.submitShared(ctx, env, ns.Name, sibling)
		Expect(err).NotTo(HaveOccurred())
		Expect(phase).To(Equal(v1alpha1.ComponentPhaseSubmitting))
		Expect(observe()).To(BeTrue())
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: provisioner.ProvisionJobName(env.Name, sibling.Name) + "-credentials", Namespace: sharedNamespace}, new(corev1.Secret))).To(Succeed())

		env.Spec.Values = map[string]string{"replicaCount": "2"}
		env.Status.Components = nil
		submit()
		Expect(observe()).To(BeTrue())
		Expect(k8sClient.Get(ctx, key, job)).To(Succeed())
		Expect(job.UID).To(Equal(uid))
		Expect(job.DeletionTimestamp.IsZero()).To(BeTrue())

		scp.Spec.Provision.Script = "echo changed"
		Expect(k8sClient.Update(ctx, scp)).To(Succeed())
		submit()
		Expect(observe()).To(BeFalse())
		Expect(findComponent(env, component.Name).Phase).To(Equal(v1alpha1.ComponentPhasePending))
	})
})

func TestDeprovisionExhaustionDoesNotSubmit(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	ctx := t.Context()
	scheme := runtime.NewScheme()
	g.Expect(corev1.AddToScheme(scheme)).To(Succeed())
	g.Expect(batchv1.AddToScheme(scheme)).To(Succeed())
	g.Expect(v1alpha1.AddToScheme(scheme)).To(Succeed())

	component := v1alpha1.ComponentSpec{Name: "db", SharedComponentRef: "db"}
	env := &v1alpha1.EphemeralEnvironment{
		ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "management"},
		Status: v1alpha1.EphemeralEnvironmentStatus{
			Components: []v1alpha1.ComponentStatus{{Name: "db", DeployRetries: maxDeployRetries - 1}},
		},
	}
	shared := &v1alpha1.SharedComponent{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: env.Namespace},
		Spec:       v1alpha1.SharedComponentSpec{Provider: "provider"},
	}
	provider := &v1alpha1.SharedComponentProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: env.Namespace},
		Spec: v1alpha1.SharedComponentProviderSpec{
			Deprovision: &v1alpha1.JobScript{Image: "busybox", Script: "exit 1"},
		},
	}
	binding := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "env-db-binding", Namespace: "workload"},
		Data:       map[string][]byte{generatedSecretKey: []byte("secret")},
	}
	template := &v1alpha1.EnvironmentTemplate{
		Spec: v1alpha1.EnvironmentTemplateSpec{Components: []v1alpha1.ComponentSpec{component}},
	}

	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&batchv1.Job{}).
		WithObjects(shared, provider, binding).
		Build()
	jobProvisioner := &provisioner.JobProvisioner{Client: kubeClient, Reader: kubeClient}
	reconciler := &EphemeralEnvironmentReconciler{Client: kubeClient, Provisioner: jobProvisioner}

	opts, err := renderProvisionOptions(env, component, shared, provider.Spec.Deprovision, "secret", nil)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(jobProvisioner.SubmitDeprovision(ctx, opts)).To(Succeed())

	job := new(batchv1.Job)
	jobKey := client.ObjectKey{
		Name:      provisioner.DeprovisionJobName(env.Name, component.Name),
		Namespace: sharedNamespace,
	}
	g.Expect(kubeClient.Get(ctx, jobKey, job)).To(Succeed())
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
	}
	g.Expect(kubeClient.Status().Update(ctx, job)).To(Succeed())

	done, _, err := reconciler.deprovisionShared(ctx, env, binding.Namespace, template)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(done).To(BeTrue())

	retained := new(batchv1.Job)
	g.Expect(kubeClient.Get(ctx, jobKey, retained)).To(Succeed())
	g.Expect(retained.DeletionTimestamp.IsZero()).To(BeTrue())
	g.Expect(deployer.TerminalPhase(retained)).To(Equal(deployer.FailedJobPhase))
	bindingErr := kubeClient.Get(ctx, client.ObjectKeyFromObject(binding), new(corev1.Secret))
	g.Expect(apierrors.IsNotFound(bindingErr)).To(BeTrue())

	done, _, err = reconciler.deprovisionShared(ctx, env, binding.Namespace, template)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(done).To(BeTrue())
}
