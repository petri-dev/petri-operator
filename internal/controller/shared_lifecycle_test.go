package controller

import (
	"testing"

	"github.com/petri-dev/petri-operator/api/v1alpha1"
	"github.com/petri-dev/petri-operator/internal/deployer"
	"github.com/petri-dev/petri-operator/internal/provisioner"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Shared Job lifecycle", func() {
	It("retains shared instance success and replaces failed and changed instance Jobs", func(ctx SpecContext) {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "instance-lifecycle-"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		scp := &v1alpha1.SharedComponentProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: ns.Name}, Spec: v1alpha1.SharedComponentProviderSpec{Helm: helmSpec()}}
		Expect(k8sClient.Create(ctx, scp)).To(Succeed())
		sc := &v1alpha1.SharedComponent{ObjectMeta: metav1.ObjectMeta{Name: ns.Name, Namespace: ns.Name}, Spec: v1alpha1.SharedComponentSpec{Provider: scp.Name}}
		Expect(k8sClient.Create(ctx, sc)).To(Succeed())
		r := &SharedComponentReconciler{Client: k8sClient, Deployer: &deployer.JobDeployer{Client: k8sClient, Reader: k8sClient, Image: "deployer:test"}}
		step := func() { _, err := r.reconcile(ctx, sc); Expect(err).NotTo(HaveOccurred()) }
		step()
		job := new(batchv1.Job)
		key := client.ObjectKey{Name: "petri-deploy-shared-" + sc.Name, Namespace: sharedNamespace}
		Expect(k8sClient.Get(ctx, key, job)).To(Succeed())
		finishJob(ctx, job, batchv1.JobFailed)
		uid := job.UID
		step()
		Expect(sc.Status.Ready).To(BeFalse())
		Expect(k8sClient.Get(ctx, key, job)).To(Succeed())
		Expect(job.DeletionTimestamp.IsZero()).To(BeFalse())
		for range 3 {
			step()
		}
		job.Finalizers = nil // Envtest has no garbage collector.
		Expect(k8sClient.Update(ctx, job)).To(Succeed())
		step()
		Expect(k8sClient.Get(ctx, key, job)).To(Succeed())
		Expect(job.UID).NotTo(Equal(uid))
		finishJob(ctx, job, batchv1.JobComplete)
		uid = job.UID
		for range 3 {
			step()
		}
		Expect(sc.Status.Ready).To(BeTrue())
		Expect(k8sClient.Get(ctx, key, job)).To(Succeed())
		Expect(job.UID).To(Equal(uid))
		Expect(job.DeletionTimestamp.IsZero()).To(BeTrue())
		scp.Spec.Helm.Values = map[string]string{"replicaCount": "2"}
		Expect(k8sClient.Update(ctx, scp)).To(Succeed())
		step()
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
		submit := func() { Expect(r.submitShared(ctx, env, ns.Name, component)).To(Succeed()) }
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
		Expect(findComponent(env, component.Name).Phase).To(Equal(v1alpha1.PhasePending))
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
		Expect(r.submitShared(ctx, env, ns.Name, sibling)).To(Succeed())
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
		Expect(findComponent(env, component.Name).Phase).To(Equal(v1alpha1.PhasePending))
	})
})

func TestDeprovisionExhaustionDoesNotSubmit(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())
	g.Expect(v1alpha1.AddToScheme(s)).To(Succeed())
	component := v1alpha1.ComponentSpec{Name: "db", SharedComponentRef: "db"}
	env := &v1alpha1.EphemeralEnvironment{ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "management"}}
	env.Status.Components = []v1alpha1.ComponentStatus{{Name: "db", DeployRetries: maxDeployRetries - 1}}
	sc := &v1alpha1.SharedComponent{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: env.Namespace}, Spec: v1alpha1.SharedComponentSpec{Provider: "provider"}}
	scp := &v1alpha1.SharedComponentProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: env.Namespace}, Spec: v1alpha1.SharedComponentProviderSpec{Deprovision: &v1alpha1.JobScript{Image: "busybox", Script: "exit 1"}}}
	binding := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "env-db-binding", Namespace: "workload"}, Data: map[string][]byte{generatedSecretKey: []byte("secret")}}
	c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&batchv1.Job{}).WithObjects(sc, scp, binding).Build()
	p := &provisioner.JobProvisioner{Client: c, Reader: c}
	opts, err := renderProvisionOptions(env, component, sc, scp.Spec.Deprovision, "secret", nil)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(p.SubmitDeprovision(t.Context(), opts)).To(Succeed())
	job := new(batchv1.Job)
	key := client.ObjectKey{Name: provisioner.DeprovisionJobName(env.Name, component.Name), Namespace: sharedNamespace}
	g.Expect(c.Get(t.Context(), key, job)).To(Succeed())
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	g.Expect(c.Status().Update(t.Context(), job)).To(Succeed())
	r := &EphemeralEnvironmentReconciler{Client: c, Provisioner: p}
	tmpl := &v1alpha1.EnvironmentTemplate{Spec: v1alpha1.EnvironmentTemplateSpec{Components: []v1alpha1.ComponentSpec{component}}}
	done, _, err := r.deprovisionShared(t.Context(), env, "workload", tmpl)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(done).To(BeTrue())
	retained := new(batchv1.Job)
	g.Expect(c.Get(t.Context(), key, retained)).To(Succeed())
	g.Expect(retained.DeletionTimestamp.IsZero()).To(BeTrue())
	g.Expect(deployer.TerminalPhase(retained)).To(Equal(deployer.FailedJobPhase))
	g.Expect(apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(binding), new(corev1.Secret)))).To(BeTrue())
	done, _, err = r.deprovisionShared(t.Context(), env, "workload", tmpl)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(done).To(BeTrue())
}
