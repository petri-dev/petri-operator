package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/petri-dev/petri-operator/api/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	. "github.com/onsi/gomega"
)

func sharedAdmissionClients(t *testing.T, maxConsumers int32) (client.WithWatch, client.WithWatch, *v1alpha1.SharedComponent, *v1alpha1.EphemeralEnvironment) {
	t.Helper()
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(scheme.AddToScheme(s)).To(Succeed())
	g.Expect(v1alpha1.AddToScheme(s)).To(Succeed())
	sc := &v1alpha1.SharedComponent{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "management", UID: "sc-uid", Finalizers: []string{sharedFinalizer}},
		Spec:       v1alpha1.SharedComponentSpec{Provider: "provider", MaxConsumers: maxConsumers},
		Status:     v1alpha1.SharedComponentStatus{Ready: true},
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "workload", UID: "ns-uid"}}
	provider := &v1alpha1.SharedComponentProvider{ObjectMeta: metav1.ObjectMeta{Name: "provider", Namespace: sc.Namespace}}
	live := fake.NewClientBuilder().WithScheme(s).WithObjects(sc, ns, provider).Build()
	stale := fake.NewClientBuilder().WithScheme(s).WithObjects(sc.DeepCopy(), ns.DeepCopy(), provider.DeepCopy()).Build()
	// Cached reads never see subsequent API writes, including consumer labels,
	// deletion timestamps and leases. Writes still go to the authoritative API.
	cached := interceptor.NewClient(live, interceptor.Funcs{
		Get: func(ctx context.Context, _ client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			return stale.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, _ client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			return stale.List(ctx, list, opts...)
		},
	})
	return live, cached, sc, &v1alpha1.EphemeralEnvironment{ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: sc.Namespace}}
}

func TestSharedDeletionAuthoritativeConsumers(t *testing.T) {
	t.Parallel()
	for _, listError := range []bool{false, true} {
		t.Run(fmt.Sprint("listError=", listError), func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			live, cached, sc, _ := sharedAdmissionClients(t, 0)
			g.Expect(live.Delete(t.Context(), sc)).To(Succeed())
			g.Expect(live.Get(t.Context(), client.ObjectKeyFromObject(sc), sc)).To(Succeed())
			ns := new(corev1.Namespace)
			g.Expect(live.Get(t.Context(), client.ObjectKey{Name: "workload"}, ns)).To(Succeed())
			ns.Labels = map[string]string{sharedLabel(sc.Name): "true"}
			g.Expect(live.Update(t.Context(), ns)).To(Succeed())
			failure := errors.New("consumer LIST unavailable")
			reader := interceptor.NewClient(live, interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if listError {
						return failure
					}
					return c.List(ctx, list, opts...)
				},
			})
			fd := newFakeDeployer()
			r := &SharedComponentReconciler{Client: cached, APIReader: reader, Deployer: fd}
			res, err := r.reconcileDelete(t.Context(), sc)
			if listError {
				g.Expect(err).To(MatchError(failure))
			} else {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			}
			g.Expect(fd.calls).To(BeEmpty())
			g.Expect(live.Get(t.Context(), client.ObjectKeyFromObject(sc), sc)).To(Succeed())
			g.Expect(sc.Finalizers).To(ContainElement(sharedFinalizer))
		})
	}
}

func TestSharedAdmissionDeletionOrdering(t *testing.T) {
	t.Parallel()
	for _, maxConsumers := range []int32{0, 1} {
		for _, order := range []string{"admitted-first", "registration-in-flight", "expired-in-flight", "deletion-first"} {
			t.Run(fmt.Sprintf("max=%d/%s", maxConsumers, order), func(t *testing.T) {
				t.Parallel()
				g := NewWithT(t)
				live, cached, sc, env := sharedAdmissionClients(t, maxConsumers)
				fd := newFakeDeployer()
				d := &SharedComponentReconciler{Client: cached, APIReader: live, Deployer: fd}
				r := &EphemeralEnvironmentReconciler{Client: cached, APIReader: live}
				component := v1alpha1.ComponentSpec{Name: "db", SharedComponentRef: sc.Name}
				deleteSC := func() {
					g.Expect(live.Delete(t.Context(), sc)).To(Succeed())
					g.Expect(live.Get(t.Context(), client.ObjectKeyFromObject(sc), sc)).To(Succeed())
				}
				switch order {
				case "admitted-first":
					g.Expect(r.submitShared(t.Context(), env, "workload", component)).To(Succeed())
					deleteSC()
					_, err := d.reconcileDelete(t.Context(), sc)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(fd.calls).To(BeEmpty())
				case "registration-in-flight", "expired-in-flight":
					entered, resume := make(chan struct{}), make(chan struct{})
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					r.Client = interceptor.NewClient(cached, interceptor.Funcs{
						Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
							close(entered)
							select {
							case <-resume:
								return c.Patch(ctx, obj, patch, opts...)
							case <-ctx.Done():
								return ctx.Err()
							}
						},
					})
					result := make(chan error, 1)
					go func() { result <- r.submitShared(ctx, env, "workload", component) }()
					select {
					case <-entered:
					case <-ctx.Done():
						t.Fatal("registration did not reach label commit")
					}
					deleteSC()
					if order == "expired-in-flight" {
						lease := new(coordinationv1.Lease)
						g.Expect(live.Get(ctx, client.ObjectKey{Name: "petri-provision-db", Namespace: sc.Namespace}, lease)).To(Succeed())
						lease.Spec.RenewTime = new(metav1.NewMicroTime(time.Now().Add(-time.Minute)))
						g.Expect(live.Update(ctx, lease)).To(Succeed())
					}
					_, err := d.reconcileDelete(ctx, sc)
					g.Expect(err).NotTo(HaveOccurred())
					if order == "registration-in-flight" {
						g.Expect(fd.calls).To(BeEmpty())
					} else {
						g.Expect(fd.undeployOrder()).To(HaveLen(1))
					}
					close(resume)
					g.Expect(<-result).To(MatchError(errSharedNotReady))
					g.Expect(apierrors.IsNotFound(live.Get(ctx, client.ObjectKey{Name: "env-db-binding", Namespace: "workload"}, new(corev1.Secret)))).To(BeTrue())
				case "deletion-first":
					deleteSC()
					d.APIReader = interceptor.NewClient(live, interceptor.Funcs{
						List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
							err := c.List(ctx, list, opts...)
							g.Expect(r.submitShared(ctx, env, "workload", component)).To(MatchError(errSharedNotReady))
							return err
						},
					})
					_, err := d.reconcileDelete(t.Context(), sc)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(fd.undeployOrder()).To(HaveLen(1))
				}
				// Even a cache that still says Ready cannot admit after deletion.
				g.Expect(r.submitShared(t.Context(), env, "workload", component)).To(MatchError(errSharedNotReady))
			})
		}
	}
}

func TestProvisionLeaseSafety(t *testing.T) {
	t.Parallel()
	g := NewWithT(t)
	live, cached, sc, _ := sharedAdmissionClients(t, 0)
	ctx := t.Context()
	lease, err := acquireProvisionLease(ctx, cached, live, sc.Name, sc.Namespace, "first")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(lease).NotTo(BeNil())
	other, err := acquireProvisionLease(ctx, cached, live, sc.Name, sc.Namespace, "foreign")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(other).To(BeNil())
	// A stale NotFound followed by AlreadyExists is contention, not ownership.
	other, err = acquireProvisionLease(ctx, cached, cached, sc.Name, sc.Namespace, "foreign")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(other).To(BeNil())
	other, err = acquireProvisionLease(ctx, cached, live, sc.Name, "other-management", "other")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(other).NotTo(BeNil())
	releaseProvisionLease(ctx, cached, other)
	changed := lease.DeepCopy()
	changed.Spec.HolderIdentity = new("replacement")
	g.Expect(live.Update(ctx, changed)).To(Succeed())
	releaseProvisionLease(ctx, cached, lease)
	g.Expect(live.Get(ctx, client.ObjectKeyFromObject(lease), changed)).To(Succeed())
	g.Expect(*changed.Spec.HolderIdentity).To(Equal("replacement"))
	// Release must still work on cancellation, but only for the acquired version.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	releaseProvisionLease(canceled, cached, changed)
	g.Expect(apierrors.IsNotFound(live.Get(ctx, client.ObjectKeyFromObject(lease), changed))).To(BeTrue())
	failure := errors.New("lease GET forbidden")
	reader := interceptor.NewClient(live, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return failure
		},
	})
	other, err = acquireProvisionLease(ctx, cached, reader, sc.Name, sc.Namespace, "first")
	g.Expect(err).To(MatchError(failure))
	g.Expect(other).To(BeNil())
}

func TestSharedAdmissionAPIFailures(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"lease-create", "sc-get", "consumer-list", "label-patch", "post-label-get", "cancellation", "capacity"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			live, cached, sc, env := sharedAdmissionClients(t, 1)
			failure := errors.New("injected API failure")
			scGets := 0
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reader := interceptor.NewClient(live, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*v1alpha1.SharedComponent); ok {
						scGets++
						if stage == "sc-get" || (stage == "post-label-get" && scGets == 2) {
							return failure
						}
					}
					return c.Get(ctx, key, obj, opts...)
				},
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if stage == "consumer-list" {
						return failure
					}
					return c.List(ctx, list, opts...)
				},
			})
			writer := interceptor.NewClient(cached, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if stage == "lease-create" {
						return failure
					}
					return c.Create(ctx, obj, opts...)
				},
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if stage == "label-patch" {
						return failure
					}
					if stage == "cancellation" {
						cancel()
						return ctx.Err()
					}
					return c.Patch(ctx, obj, patch, opts...)
				},
			})
			if stage == "capacity" {
				g.Expect(live.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "existing-consumer", Labels: map[string]string{sharedLabel(sc.Name): "true"}}})).To(Succeed())
			}
			r := &EphemeralEnvironmentReconciler{Client: writer, APIReader: reader}
			err := r.submitShared(ctx, env, "workload", v1alpha1.ComponentSpec{Name: "db", SharedComponentRef: sc.Name})
			switch stage {
			case "cancellation":
				g.Expect(err).To(MatchError(context.Canceled))
			case "capacity":
				g.Expect(err).To(MatchError(errAtCapacity))
			default:
				g.Expect(err).To(MatchError(failure))
			}
			g.Expect(apierrors.IsNotFound(live.Get(t.Context(), client.ObjectKey{Name: "env-db-binding", Namespace: "workload"}, new(corev1.Secret)))).To(BeTrue())
			g.Expect(apierrors.IsNotFound(live.Get(t.Context(), client.ObjectKey{Name: "petri-provision-db", Namespace: sc.Namespace}, new(coordinationv1.Lease)))).To(BeTrue())
		})
	}
}
