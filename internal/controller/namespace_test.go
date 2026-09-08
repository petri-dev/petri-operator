package controller

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/petri-dev/petri-operator/api/v1alpha1"
	"github.com/petri-dev/petri-operator/internal/deployer"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	. "github.com/onsi/gomega"
)

func namespaceFixture(t *testing.T) (*EphemeralEnvironmentReconciler, *v1alpha1.EphemeralEnvironment) {
	t.Helper()
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())
	g.Expect(rbacv1.AddToScheme(s)).To(Succeed())
	g.Expect(v1alpha1.AddToScheme(s)).To(Succeed())

	env := &v1alpha1.EphemeralEnvironment{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "control", UID: "12345678-1234-1234-1234-123456789abc", Finalizers: []string{finalizer}},
		Spec:       v1alpha1.EphemeralEnvironmentSpec{Template: "template"},
	}
	tmpl := &v1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: env.Namespace}}

	c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(env).WithObjects(env, tmpl).Build()
	return &EphemeralEnvironmentReconciler{Client: c}, env
}

func namespaceStep(t *testing.T, r *EphemeralEnvironmentReconciler, env *v1alpha1.EphemeralEnvironment) {
	t.Helper()
	g := NewWithT(t)

	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(r.Get(t.Context(), client.ObjectKeyFromObject(env), env)).To(Succeed())
}

func TestNamespaceAllocation(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	r, env := namespaceFixture(t)
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(env.UID)))
	short := nsPrefix + digest[:8]
	// Neither the shared runtime nor a managed-only/foreign candidate can be adopted.
	for name, labels := range map[string]map[string]string{
		sharedNamespace:        {managedLabel: "true"},
		short:                  {managedLabel: "true"},
		nsPrefix + digest[:12]: {managedLabel: "true", ownerUIDLabel: "another-uid"},
	} {
		g.Expect(r.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}})).To(Succeed())
	}

	namespaceStep(t, r, env)
	g.Expect(env.Status.TargetNamespace).To(Equal(nsPrefix + digest[:16]))

	ns := new(corev1.Namespace)
	g.Expect(apierrors.IsNotFound(r.Get(t.Context(), client.ObjectKey{Name: env.Status.TargetNamespace}, ns))).To(BeTrue())
	// A fresh reconciler must resume from the persisted assignment, not the shortest free name.
	g.Expect(r.Delete(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: short}})).To(Succeed())

	r = &EphemeralEnvironmentReconciler{Client: r.Client}
	namespaceStep(t, r, env)
	g.Expect(env.Status.TargetNamespace).To(Equal(nsPrefix + digest[:16]))
	g.Expect(meta.IsStatusConditionTrue(env.Status.Conditions, namespaceBound)).To(BeTrue())
	g.Expect(r.Get(t.Context(), client.ObjectKey{Name: env.Status.TargetNamespace}, ns)).To(Succeed())
	g.Expect(ns.Labels[ownerUIDLabel]).To(Equal(string(env.UID)))

	namespaceStep(t, r, env)
	g.Expect(env.Status.Phase).To(Equal(v1alpha1.EnvironmentPhaseReady))

	// A generation reset must not permit moving an already-bound assignment.
	ns.Labels[ownerUIDLabel] = "replacement-owner"
	g.Expect(r.Update(t.Context(), ns)).To(Succeed())

	env.Generation++
	g.Expect(r.Update(t.Context(), env)).To(Succeed())

	namespaceStep(t, r, env)
	g.Expect(env.Status.TargetNamespace).To(Equal(nsPrefix + digest[:16]))
	g.Expect(failureReason(env)).To(Equal("NamespaceNotManaged"))

	other := &v1alpha1.EphemeralEnvironment{ObjectMeta: metav1.ObjectMeta{Name: env.Name, Namespace: "other-control", UID: "different-uid"}}
	g.Expect(r.allocateNamespace(t.Context(), other, 8)).To(Succeed())
	g.Expect(other.Status.TargetNamespace).NotTo(Equal(env.Status.TargetNamespace))
}

func TestNamespacePersistenceFailureAndCreateRace(t *testing.T) {
	t.Parallel()
	for _, owner := range []string{"", "foreign", "12345678-1234-1234-1234-123456789abc"} {
		t.Run("owner="+owner, func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			r, env := namespaceFixture(t)
			base := r.Client.(client.WithWatch)
			r.Client = interceptor.NewClient(base, interceptor.Funcs{
				SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
					return errors.New("status unavailable")
				},
			})
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)})
			g.Expect(err).To(HaveOccurred())
			namespaces := new(corev1.NamespaceList)
			g.Expect(base.List(t.Context(), namespaces)).To(Succeed())
			g.Expect(namespaces.Items).To(BeEmpty())
			r.Client = base
			namespaceStep(t, r, env)
			candidate := env.Status.TargetNamespace
			r.Client = interceptor.NewClient(base, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if ns, ok := obj.(*corev1.Namespace); ok {
						competitor := ns.DeepCopy()
						competitor.Labels = map[string]string{managedLabel: "true", ownerUIDLabel: owner}
						g.Expect(c.Create(ctx, competitor)).To(Succeed())
					}
					return c.Create(ctx, obj, opts...)
				},
			})
			namespaceStep(t, r, env)
			if owner == string(env.UID) {
				g.Expect(env.Status.TargetNamespace).To(Equal(candidate))
				g.Expect(meta.IsStatusConditionTrue(env.Status.Conditions, namespaceBound)).To(BeTrue())
			} else {
				g.Expect(env.Status.TargetNamespace).To(HaveLen(len(candidate) + 4))
				g.Expect(apierrors.IsNotFound(base.Get(t.Context(), client.ObjectKey{Name: env.Status.TargetNamespace}, new(corev1.Namespace)))).To(BeTrue())
				accounts := new(corev1.ServiceAccountList)
				g.Expect(base.List(t.Context(), accounts)).To(Succeed())
				g.Expect(accounts.Items).To(BeEmpty())
			}
		})
	}
}

func TestNamespaceDeleteGuard(t *testing.T) {
	t.Parallel()
	for _, owner := range []string{"absent", "", "foreign", "12345678-1234-1234-1234-123456789abc"} {
		t.Run("owner="+owner, func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			r, env := namespaceFixture(t)
			tmpl := new(v1alpha1.EnvironmentTemplate)
			g.Expect(r.Get(t.Context(), client.ObjectKey{Namespace: env.Namespace, Name: env.Spec.Template}, tmpl)).To(Succeed())
			tmpl.Spec.Components = []v1alpha1.ComponentSpec{{Name: "app", Helm: helmSpec()}}
			g.Expect(r.Update(t.Context(), tmpl)).To(Succeed())
			// No Deployer is configured: an unbound or foreign candidate must never invoke it.
			namespaceStep(t, r, env)
			name := env.Status.TargetNamespace
			if owner != "absent" {
				g.Expect(r.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
					Name: name, Labels: map[string]string{managedLabel: "true", ownerUIDLabel: owner},
				}})).To(Succeed())
			}
			g.Expect(r.Delete(t.Context(), env)).To(Succeed())
			if owner == string(env.UID) {
				namespaceStep(t, r, env)
				g.Expect(meta.IsStatusConditionTrue(env.Status.Conditions, cleanupComplete)).To(BeTrue())
			}
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(apierrors.IsNotFound(r.Get(t.Context(), client.ObjectKeyFromObject(env), env))).To(BeTrue())
			err = r.Get(t.Context(), client.ObjectKey{Name: name}, new(corev1.Namespace))
			if owner == "absent" || owner == string(env.UID) {
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
			} else {
				g.Expect(err).NotTo(HaveOccurred())
				accounts := new(corev1.ServiceAccountList)
				g.Expect(r.List(t.Context(), accounts)).To(Succeed())
				g.Expect(accounts.Items).To(BeEmpty())
			}
		})
	}
}

func TestNamespaceCleanupRecovery(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"deleted", "terminating", "foreign", "delete-race", "checkpoint-error", "checkpoint-conflict"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			r, env := namespaceFixture(t)
			namespaceStep(t, r, env)
			namespaceStep(t, r, env)
			g.Expect(r.Delete(t.Context(), env)).To(Succeed())
			base := r.Client.(client.WithWatch)
			failure := errors.New("injected patch failure")
			deletes := 0
			r.Client = interceptor.NewClient(base, interceptor.Funcs{
				SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
					if mode == "checkpoint-error" {
						return failure
					}
					if mode == "checkpoint-conflict" {
						current := new(v1alpha1.EphemeralEnvironment)
						g.Expect(c.Get(ctx, client.ObjectKeyFromObject(env), current)).To(Succeed())
						current.Annotations = map[string]string{"concurrent": "write"}
						g.Expect(c.Update(ctx, current)).To(Succeed())
					}
					return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
				},
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if _, ok := obj.(*corev1.Namespace); ok {
						deletes++
					}
					return c.Delete(ctx, obj, opts...)
				},
			})
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)}
			res, err := r.Reconcile(t.Context(), req)
			g.Expect(deletes).To(BeZero())
			g.Expect(base.Get(t.Context(), req.NamespacedName, env)).To(Succeed())
			ns := new(corev1.Namespace)
			g.Expect(base.Get(t.Context(), client.ObjectKey{Name: env.Status.TargetNamespace}, ns)).To(Succeed())
			if mode == "checkpoint-error" || mode == "checkpoint-conflict" {
				g.Expect(err).To(HaveOccurred())
				g.Expect(meta.IsStatusConditionTrue(env.Status.Conditions, cleanupComplete)).To(BeFalse())
				g.Expect(env.Finalizers).To(ContainElement(finalizer))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			g.Expect(meta.IsStatusConditionTrue(env.Status.Conditions, cleanupComplete)).To(BeTrue())
			// A changed template and nil workers make repeated cleanup fail after restart.
			tmpl := new(v1alpha1.EnvironmentTemplate)
			g.Expect(base.Get(t.Context(), client.ObjectKey{Name: env.Spec.Template, Namespace: env.Namespace}, tmpl)).To(Succeed())
			tmpl.Spec.Components = []v1alpha1.ComponentSpec{{Name: "app", Helm: helmSpec()}}
			g.Expect(base.Update(t.Context(), tmpl)).To(Succeed())
			if mode == "terminating" {
				ns.Finalizers = []string{"test/hold"}
				g.Expect(base.Update(t.Context(), ns)).To(Succeed())
			}
			if mode == "foreign" {
				ns.Labels[ownerUIDLabel] = "foreign"
				g.Expect(base.Update(t.Context(), ns)).To(Succeed())
			}
			faults := interceptor.NewClient(base, interceptor.Funcs{
				Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
					return failure
				},
				SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
					g.Expect(mode).To(Or(Equal("foreign"), Equal("delete-race")))
					return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
				},
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					deletes++
					options := (&client.DeleteOptions{}).ApplyOptions(opts)
					g.Expect(options.Preconditions).NotTo(BeNil())
					g.Expect(*options.Preconditions.UID).To(Equal(ns.UID))
					g.Expect(*options.Preconditions.ResourceVersion).To(Equal(ns.ResourceVersion))
					if mode == "delete-race" {
						ns.Labels[ownerUIDLabel] = "foreign"
						g.Expect(c.Update(ctx, ns)).To(Succeed())
					}
					return c.Delete(ctx, obj, opts...)
				},
			})
			r = &EphemeralEnvironmentReconciler{Client: faults, APIReader: base}
			_, err = r.Reconcile(t.Context(), req)
			g.Expect(base.Get(t.Context(), req.NamespacedName, env)).To(Succeed())
			g.Expect(env.Finalizers).To(ContainElement(finalizer))
			if mode == "foreign" || mode == "delete-race" {
				g.Expect(base.Get(t.Context(), client.ObjectKeyFromObject(ns), ns)).To(Succeed())
				g.Expect(ns.DeletionTimestamp.IsZero()).To(BeTrue())
				if mode == "foreign" {
					g.Expect(deletes).To(BeZero())
				} else {
					g.Expect(apierrors.IsConflict(err)).To(BeTrue())
				}
				return
			}
			g.Expect(err).To(MatchError(failure))
			if mode == "deleted" {
				g.Expect(apierrors.IsNotFound(base.Get(t.Context(), client.ObjectKeyFromObject(ns), ns))).To(BeTrue())
			} else {
				g.Expect(base.Get(t.Context(), client.ObjectKeyFromObject(ns), ns)).To(Succeed())
				g.Expect(ns.DeletionTimestamp.IsZero()).To(BeFalse())
			}
			r = &EphemeralEnvironmentReconciler{Client: base, APIReader: base}
			_, err = r.Reconcile(t.Context(), req)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(apierrors.IsNotFound(base.Get(t.Context(), req.NamespacedName, env))).To(BeTrue())
		})
	}
}

func TestNamespaceBindingCrashRecovery(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	r, env := namespaceFixture(t)
	fd := newFakeDeployer()
	r.Deployer = fd
	tmpl := new(v1alpha1.EnvironmentTemplate)
	g.Expect(r.Get(t.Context(), client.ObjectKey{Namespace: env.Namespace, Name: env.Spec.Template}, tmpl)).To(Succeed())
	tmpl.Spec.Components = []v1alpha1.ComponentSpec{{Name: "app", Helm: helmSpec()}}
	g.Expect(r.Update(t.Context(), tmpl)).To(Succeed())
	namespaceStep(t, r, env)
	candidate := env.Status.TargetNamespace
	base := r.Client.(client.WithWatch)
	r.Client = interceptor.NewClient(base, interceptor.Funcs{
		SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
			return errors.New("crash before binding persisted")
		},
	})
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(env)})
	g.Expect(err).To(HaveOccurred())
	g.Expect(base.Get(t.Context(), client.ObjectKey{Name: candidate}, new(corev1.Namespace))).To(Succeed())
	g.Expect(base.Get(t.Context(), client.ObjectKeyFromObject(env), env)).To(Succeed())
	g.Expect(meta.IsStatusConditionTrue(env.Status.Conditions, namespaceBound)).To(BeFalse())
	g.Expect(fd.submitOrder()).To(BeEmpty())
	r = &EphemeralEnvironmentReconciler{Client: base, Deployer: fd}
	namespaceStep(t, r, env)
	g.Expect(meta.IsStatusConditionTrue(env.Status.Conditions, namespaceBound)).To(BeTrue())
	g.Expect(fd.submitOrder()).To(BeEmpty())
	fd.setOutcome(env.Name+"-app", deployer.PendingJobPhase, "")
	namespaceStep(t, r, env)
	g.Expect(fd.SubmitCount(env.Name + "-app")).To(Equal(1))
	g.Expect(env.Status.TargetNamespace).To(Equal(candidate))
}

func TestNamespaceBoundOwnershipLoss(t *testing.T) {
	t.Parallel()
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			r, env := namespaceFixture(t)
			namespaceStep(t, r, env)
			namespaceStep(t, r, env)
			ns := new(corev1.Namespace)
			g.Expect(r.Get(t.Context(), client.ObjectKey{Name: env.Status.TargetNamespace}, ns)).To(Succeed())
			if missing {
				g.Expect(r.Delete(t.Context(), ns)).To(Succeed())
				g.Expect(r.createNamespace(t.Context(), env)).NotTo(Succeed())
			} else {
				ns.Labels[ownerUIDLabel] = "replacement"
				g.Expect(r.Update(t.Context(), ns)).To(Succeed())
			}
			g.Expect(r.Delete(t.Context(), env)).To(Succeed())
			namespaceStep(t, r, env)
			g.Expect(env.Finalizers).To(ContainElement(finalizer))
			g.Expect(failureReason(env)).To(Equal("NamespaceNotManaged"))
		})
	}
}

func TestNamespaceExhaustion(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	r, env := namespaceFixture(t)
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(env.UID)))
	for n := 8; n <= 52; n += 4 {
		g.Expect(r.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsPrefix + digest[:n]}})).To(Succeed())
	}
	g.Expect(r.allocateNamespace(t.Context(), env, 8)).NotTo(Succeed())
	g.Expect(env.Status.TargetNamespace).To(BeEmpty())
	g.Expect(r.Delete(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsPrefix + digest[:52]}})).To(Succeed())
	g.Expect(r.allocateNamespace(t.Context(), env, 8)).To(Succeed())
	g.Expect(env.Status.TargetNamespace).To(HaveLen(62))
	for _, name := range []string{sharedNamespace, "petri-system", nsPrefix + digest[:56], nsPrefix + "12345678"} {
		env.Status.TargetNamespace = name
		_, err := r.targetNamespace(env)
		g.Expect(err).To(HaveOccurred())
	}
	env.UID = types.UID("")
	g.Expect(r.allocateNamespace(t.Context(), env, 8)).NotTo(Succeed())
}

func TestNamespaceInvalidAssignment(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"ready", "ttl", "delete"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			r, env := namespaceFixture(t)
			env.Status.TargetNamespace = "petri-" + env.Name
			env.Status.Phase = v1alpha1.EnvironmentPhaseReady
			g.Expect(r.Status().Update(t.Context(), env)).To(Succeed())
			env.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
			if mode == "ttl" {
				env.Spec.TTL = "1s"
			}
			g.Expect(r.Update(t.Context(), env)).To(Succeed())
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: env.Status.TargetNamespace,
				Labels: map[string]string{managedLabel: "true", ownerUIDLabel: string(env.UID)}}}
			g.Expect(r.Create(t.Context(), ns)).To(Succeed())
			if mode == "delete" {
				g.Expect(r.Delete(t.Context(), env)).To(Succeed())
			}
			namespaceStep(t, r, env)
			g.Expect(failureReason(env)).To(Equal("InvalidConfiguration"))
			g.Expect(env.Status.TargetNamespace).To(Equal(ns.Name))
			g.Expect(env.Finalizers).To(ContainElement(finalizer))
			g.Expect(env.DeletionTimestamp.IsZero()).To(Equal(mode != "delete"))
			g.Expect(meta.IsStatusConditionTrue(env.Status.Conditions, namespaceBound)).To(BeFalse())
			unchanged := new(corev1.Namespace)
			g.Expect(r.Get(t.Context(), client.ObjectKeyFromObject(ns), unchanged)).To(Succeed())
			g.Expect(unchanged).To(Equal(ns))
			accounts := new(corev1.ServiceAccountList)
			g.Expect(r.List(t.Context(), accounts)).To(Succeed())
			g.Expect(accounts.Items).To(BeEmpty())
		})
	}
}

func TestNamespaceAllocationWithPhase(t *testing.T) {
	t.Parallel()
	for _, phase := range []v1alpha1.EnvironmentPhase{"", v1alpha1.EnvironmentPhasePending, v1alpha1.EnvironmentPhaseReady, v1alpha1.EnvironmentPhaseFailed} {
		t.Run(string(phase), func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			r, env := namespaceFixture(t)
			env.Name = "preview"
			env.ResourceVersion = ""
			g.Expect(r.Create(t.Context(), env)).To(Succeed())
			env.Status.Phase = phase
			g.Expect(r.Status().Update(t.Context(), env)).To(Succeed())
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "petri-" + env.Name,
				Labels: map[string]string{managedLabel: "true"}}}
			g.Expect(r.Create(t.Context(), ns)).To(Succeed())
			namespaceStep(t, r, env)
			digest := fmt.Sprintf("%x", sha256.Sum256([]byte(env.UID)))
			g.Expect(env.Status.TargetNamespace).To(Equal(nsPrefix + digest[:8]))
			g.Expect(env.Status.Phase).To(Equal(phase))
			g.Expect(apierrors.IsNotFound(r.Get(t.Context(), client.ObjectKey{Name: env.Status.TargetNamespace}, new(corev1.Namespace)))).To(BeTrue())
			unchanged := new(corev1.Namespace)
			g.Expect(r.Get(t.Context(), client.ObjectKeyFromObject(ns), unchanged)).To(Succeed())
			g.Expect(unchanged).To(Equal(ns))
		})
	}
}
