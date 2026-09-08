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

	// Cached reads never see subsequent API writes, including consumer labels, deletion timestamps and leases. Writes still go to the authoritative API.
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

type sharedAdmissionTest struct {
	live      client.WithWatch
	cached    client.WithWatch
	sc        *v1alpha1.SharedComponent
	env       *v1alpha1.EphemeralEnvironment
	admission *EphemeralEnvironmentReconciler
	deletion  *SharedComponentReconciler
	deployer  *fakeDeployer
}

func newSharedAdmissionTest(t *testing.T, maxConsumers int32) *sharedAdmissionTest {
	t.Helper()
	live, cached, sc, env := sharedAdmissionClients(t, maxConsumers)
	fd := newFakeDeployer()
	return &sharedAdmissionTest{
		live: live, cached: cached, sc: sc, env: env, deployer: fd,
		admission: &EphemeralEnvironmentReconciler{Client: cached, APIReader: live},
		deletion:  &SharedComponentReconciler{Client: cached, APIReader: live, Deployer: fd},
	}
}

func (f *sharedAdmissionTest) submit(ctx context.Context) error {
	return f.admission.submitShared(ctx, f.env, "workload", v1alpha1.ComponentSpec{Name: "db", SharedComponentRef: f.sc.Name})
}

func (f *sharedAdmissionTest) deleteComponent(t *testing.T) {
	t.Helper()
	g := NewWithT(t)
	g.Expect(f.live.Delete(t.Context(), f.sc)).To(Succeed())
	g.Expect(f.live.Get(t.Context(), client.ObjectKeyFromObject(f.sc), f.sc)).To(Succeed())
}

func (f *sharedAdmissionTest) expectNoBindingOrLease(t *testing.T) {
	t.Helper()
	g := NewWithT(t)
	g.Expect(apierrors.IsNotFound(f.live.Get(t.Context(), client.ObjectKey{Name: "env-db-binding", Namespace: "workload"}, new(corev1.Secret)))).To(BeTrue())
	g.Expect(apierrors.IsNotFound(f.live.Get(t.Context(), client.ObjectKey{Name: "petri-provision-db", Namespace: f.sc.Namespace}, new(coordinationv1.Lease)))).To(BeTrue())
}

func (f *sharedAdmissionTest) pauseRegistration(t *testing.T) (chan<- struct{}, <-chan error) {
	t.Helper()
	entered, resume := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	f.admission.Client = interceptor.NewClient(f.cached, interceptor.Funcs{
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
	go func() { result <- f.submit(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("registration did not reach label commit")
	}
	return resume, result
}

func TestSharedDeletionAuthoritativeConsumers(t *testing.T) {
	t.Parallel()
	setup := func(t *testing.T) *sharedAdmissionTest {
		t.Helper()
		f := newSharedAdmissionTest(t, 0)
		f.deleteComponent(t)
		ns := new(corev1.Namespace)
		g := NewWithT(t)
		g.Expect(f.live.Get(t.Context(), client.ObjectKey{Name: "workload"}, ns)).To(Succeed())
		ns.Labels = map[string]string{sharedLabel(f.sc.Name): "true"}
		g.Expect(f.live.Update(t.Context(), ns)).To(Succeed())
		return f
	}
	retained := func(t *testing.T, f *sharedAdmissionTest) {
		t.Helper()
		g := NewWithT(t)
		g.Expect(f.deployer.calls).To(BeEmpty())
		g.Expect(f.live.Get(t.Context(), client.ObjectKeyFromObject(f.sc), f.sc)).To(Succeed())
		g.Expect(f.sc.Finalizers).To(ContainElement(sharedFinalizer))
	}

	t.Run("existing-consumer", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)
		f := setup(t)

		res, err := f.deletion.reconcileDelete(t.Context(), f.sc)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(res.RequeueAfter).To(BeNumerically(">", 0))
		retained(t, f)
	})

	t.Run("consumer-list-error", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)
		f := setup(t)
		failure := errors.New("consumer LIST unavailable")
		f.deletion.APIReader = interceptor.NewClient(f.live, interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return failure
			},
		})

		_, err := f.deletion.reconcileDelete(t.Context(), f.sc)
		g.Expect(err).To(MatchError(failure))
		retained(t, f)
	})
}

func TestSharedAdmissionDeletionOrdering(t *testing.T) {
	t.Parallel()
	for _, maxConsumers := range []int32{0, 1} {
		t.Run(fmt.Sprintf("max=%d/admitted-first", maxConsumers), func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			f := newSharedAdmissionTest(t, maxConsumers)
			g.Expect(f.submit(t.Context())).To(Succeed())
			f.deleteComponent(t)

			_, err := f.deletion.reconcileDelete(t.Context(), f.sc)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(f.deployer.calls).To(BeEmpty())
			// Even a cache that still says Ready cannot admit after deletion.
			g.Expect(f.submit(t.Context())).To(MatchError(errSharedNotReady))
		})

		t.Run(fmt.Sprintf("max=%d/registration-in-flight", maxConsumers), func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			f := newSharedAdmissionTest(t, maxConsumers)
			resume, result := f.pauseRegistration(t)
			f.deleteComponent(t)

			_, err := f.deletion.reconcileDelete(t.Context(), f.sc)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(f.deployer.calls).To(BeEmpty())
			close(resume)
			g.Expect(<-result).To(MatchError(errSharedNotReady))
			f.expectNoBindingOrLease(t)
			g.Expect(f.submit(t.Context())).To(MatchError(errSharedNotReady))
		})

		t.Run(fmt.Sprintf("max=%d/expired-in-flight", maxConsumers), func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			f := newSharedAdmissionTest(t, maxConsumers)
			resume, result := f.pauseRegistration(t)
			f.deleteComponent(t)
			lease := new(coordinationv1.Lease)
			g.Expect(f.live.Get(t.Context(), client.ObjectKey{Name: "petri-provision-db", Namespace: f.sc.Namespace}, lease)).To(Succeed())
			lease.Spec.RenewTime = new(metav1.NewMicroTime(time.Now().Add(-time.Minute)))
			g.Expect(f.live.Update(t.Context(), lease)).To(Succeed())

			_, err := f.deletion.reconcileDelete(t.Context(), f.sc)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(f.deployer.undeployOrder()).To(HaveLen(1))
			close(resume)
			g.Expect(<-result).To(MatchError(errSharedNotReady))
			f.expectNoBindingOrLease(t)
			g.Expect(f.submit(t.Context())).To(MatchError(errSharedNotReady))
		})

		t.Run(fmt.Sprintf("max=%d/deletion-first", maxConsumers), func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			f := newSharedAdmissionTest(t, maxConsumers)
			f.deleteComponent(t)
			f.deletion.APIReader = interceptor.NewClient(f.live, interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					err := c.List(ctx, list, opts...)
					g.Expect(f.submit(ctx)).To(MatchError(errSharedNotReady))
					return err
				},
			})

			_, err := f.deletion.reconcileDelete(t.Context(), f.sc)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(f.deployer.undeployOrder()).To(HaveLen(1))
			g.Expect(f.submit(t.Context())).To(MatchError(errSharedNotReady))
		})
	}
}

func TestProvisionLeaseSafety(t *testing.T) {
	t.Parallel()
	setup := func(t *testing.T) (*sharedAdmissionTest, *coordinationv1.Lease) {
		t.Helper()
		g := NewWithT(t)
		f := newSharedAdmissionTest(t, 0)
		lease, err := acquireProvisionLease(t.Context(), f.cached, f.live, f.sc.Name, f.sc.Namespace, "first")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(lease).NotTo(BeNil())
		return f, lease
	}

	t.Run("foreign-holder", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)
		f, _ := setup(t)

		other, err := acquireProvisionLease(t.Context(), f.cached, f.live, f.sc.Name, f.sc.Namespace, "foreign")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(other).To(BeNil())
	})

	t.Run("stale-cache-miss", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)
		f, _ := setup(t)

		// A stale NotFound followed by AlreadyExists is contention, not ownership.
		other, err := acquireProvisionLease(t.Context(), f.cached, f.cached, f.sc.Name, f.sc.Namespace, "foreign")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(other).To(BeNil())
	})

	t.Run("other-namespace", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)
		f, _ := setup(t)

		other, err := acquireProvisionLease(t.Context(), f.cached, f.live, f.sc.Name, "other-management", "other")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(other).NotTo(BeNil())
		releaseProvisionLease(t.Context(), f.cached, other)
		g.Expect(apierrors.IsNotFound(f.live.Get(t.Context(), client.ObjectKeyFromObject(other), other))).To(BeTrue())
	})

	t.Run("release-precondition", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)
		f, lease := setup(t)
		changed := lease.DeepCopy()
		changed.Spec.HolderIdentity = new("replacement")
		g.Expect(f.live.Update(t.Context(), changed)).To(Succeed())

		releaseProvisionLease(t.Context(), f.cached, lease)
		g.Expect(f.live.Get(t.Context(), client.ObjectKeyFromObject(lease), changed)).To(Succeed())
		g.Expect(*changed.Spec.HolderIdentity).To(Equal("replacement"))
	})

	t.Run("release-after-cancellation", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)
		f, lease := setup(t)
		changed := lease.DeepCopy()
		changed.Spec.HolderIdentity = new("replacement")
		g.Expect(f.live.Update(t.Context(), changed)).To(Succeed())

		// Release must still work on cancellation, but only for the acquired version.
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		releaseProvisionLease(canceled, f.cached, changed)
		g.Expect(apierrors.IsNotFound(f.live.Get(t.Context(), client.ObjectKeyFromObject(lease), changed))).To(BeTrue())
	})

	t.Run("getter-error", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)
		live, cached, sc, _ := sharedAdmissionClients(t, 0)
		failure := errors.New("lease GET forbidden")
		reader := interceptor.NewClient(live, interceptor.Funcs{
			Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return failure
			},
		})

		lease, err := acquireProvisionLease(t.Context(), cached, reader, sc.Name, sc.Namespace, "first")
		g.Expect(err).To(MatchError(failure))
		g.Expect(lease).To(BeNil())
	})
}

func TestSharedAdmissionAPIFailures(t *testing.T) {
	t.Parallel()
	failure := errors.New("injected API failure")

	t.Run("lease-create", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)
		f := newSharedAdmissionTest(t, 1)
		f.admission.Client = interceptor.NewClient(f.cached, interceptor.Funcs{
			Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
				return failure
			},
		})

		g.Expect(f.submit(t.Context())).To(MatchError(failure))
		f.expectNoBindingOrLease(t)
	})

	t.Run("sc-get", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)
		f := newSharedAdmissionTest(t, 1)
		f.admission.APIReader = interceptor.NewClient(f.live, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*v1alpha1.SharedComponent); ok {
					return failure
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})

		g.Expect(f.submit(t.Context())).To(MatchError(failure))
		f.expectNoBindingOrLease(t)
	})

	t.Run("consumer-list", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)
		f := newSharedAdmissionTest(t, 1)
		f.admission.APIReader = interceptor.NewClient(f.live, interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return failure
			},
		})

		g.Expect(f.submit(t.Context())).To(MatchError(failure))
		f.expectNoBindingOrLease(t)
	})

	t.Run("label-patch", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)
		f := newSharedAdmissionTest(t, 1)
		f.admission.Client = interceptor.NewClient(f.cached, interceptor.Funcs{
			Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
				return failure
			},
		})

		g.Expect(f.submit(t.Context())).To(MatchError(failure))
		f.expectNoBindingOrLease(t)
	})

	t.Run("post-label-get", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)
		f := newSharedAdmissionTest(t, 1)
		scGets := 0
		f.admission.APIReader = interceptor.NewClient(f.live, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*v1alpha1.SharedComponent); ok {
					scGets++
					if scGets == 2 {
						return failure
					}
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})

		g.Expect(f.submit(t.Context())).To(MatchError(failure))
		f.expectNoBindingOrLease(t)
	})

	t.Run("cancellation", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)
		f := newSharedAdmissionTest(t, 1)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		f.admission.Client = interceptor.NewClient(f.cached, interceptor.Funcs{
			Patch: func(ctx context.Context, _ client.WithWatch, _ client.Object, _ client.Patch, _ ...client.PatchOption) error {
				cancel()
				return ctx.Err()
			},
		})

		g.Expect(f.submit(ctx)).To(MatchError(context.Canceled))
		f.expectNoBindingOrLease(t)
	})

	t.Run("capacity", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)
		f := newSharedAdmissionTest(t, 1)
		g.Expect(f.live.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name: "existing-consumer", Labels: map[string]string{sharedLabel(f.sc.Name): "true"},
		}})).To(Succeed())

		g.Expect(f.submit(t.Context())).To(MatchError(errAtCapacity))
		f.expectNoBindingOrLease(t)
	})
}
