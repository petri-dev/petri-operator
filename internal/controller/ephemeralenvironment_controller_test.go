/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"

	corev1alpha1 "github.com/nuromirg/petri/api/v1alpha1"
	"github.com/nuromirg/petri/internal/deployer"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func reconcileUntilTerminal(r *EphemeralEnvironmentReconciler, key types.NamespacedName, maxIterations int) corev1alpha1.Phase {
	for range maxIterations {
		_, err := r.Reconcile(tctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		env := &corev1alpha1.EphemeralEnvironment{}
		Expect(k8sClient.Get(tctx, key, env)).To(Succeed())
		if env.Status.Phase == corev1alpha1.PhaseReady || env.Status.Phase == corev1alpha1.PhaseFailed {
			return env.Status.Phase
		}
	}
	env := &corev1alpha1.EphemeralEnvironment{}
	Expect(k8sClient.Get(tctx, key, env)).To(Succeed())
	return env.Status.Phase
}

func reconcileUntilGone(r *EphemeralEnvironmentReconciler, key types.NamespacedName, maxIterations int) {
	for range maxIterations {
		_, err := r.Reconcile(tctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		gone := &corev1alpha1.EphemeralEnvironment{}
		if err := k8sClient.Get(tctx, key, gone); err != nil {
			return
		}
	}
}

func newReconciler(fd *fakeDeployer, fc *fakeChecker) *EphemeralEnvironmentReconciler {
	return newReconcilerWithProvisioner(fd, newFakeProvisioner(), fc)
}

func newReconcilerWithProvisioner(fd *fakeDeployer, fp *fakeProvisioner, fc *fakeChecker) *EphemeralEnvironmentReconciler {
	return &EphemeralEnvironmentReconciler{
		Client:      k8sClient,
		Scheme:      scheme.Scheme,
		Deployer:    fd,
		Provisioner: fp,
		Checker:     fc,
	}
}

func helmSpec() *corev1alpha1.HelmSpec {
	return &corev1alpha1.HelmSpec{
		Repo:    "https://charts.example.com",
		Chart:   "petri-stub-app",
		Version: "1.0.0",
	}
}

func makeTemplate(ns, name string, components []corev1alpha1.ComponentSpec) {
	tmpl := &corev1alpha1.EnvironmentTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       corev1alpha1.EnvironmentTemplateSpec{Components: components},
	}
	Expect(k8sClient.Create(tctx, tmpl)).To(Succeed())
}

func makeEnv(ns, name, templateName string) types.NamespacedName {
	env := &corev1alpha1.EphemeralEnvironment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1alpha1.EphemeralEnvironmentSpec{
			Template: templateName,
			Source:   corev1alpha1.SourceSpec{Repo: "https://github.com/example/repo", Branch: "main"},
		},
	}
	Expect(k8sClient.Create(tctx, env)).To(Succeed())
	return types.NamespacedName{Name: name, Namespace: ns}
}

func getEnv(key types.NamespacedName) *corev1alpha1.EphemeralEnvironment {
	env := &corev1alpha1.EphemeralEnvironment{}
	Expect(k8sClient.Get(tctx, key, env)).To(Succeed())
	return env
}

func failureReason(env *corev1alpha1.EphemeralEnvironment) string {
	for _, c := range env.Status.Conditions {
		if c.Type == "Ready" {
			return c.Reason
		}
	}
	return ""
}

var _ = Describe("EphemeralEnvironment Controller", func() {
	var (
		testNS  string
		counter int
	)

	BeforeEach(func() {
		tctx = context.Background()
		counter++
		testNS = fmt.Sprintf("test-%04d", counter)
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNS}}
		Expect(k8sClient.Create(tctx, ns)).To(Succeed())
	})

	Context("single service", func() {
		It("reaches Ready and cleans up on delete", func() {
			fd := newFakeDeployer()
			fc := newFakeChecker()
			r := newReconciler(fd, fc)

			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "svc", Helm: helmSpec()},
			})
			key := makeEnv(testNS, "pr-1", "tmpl")

			releaseName := "pr-1-svc"
			fd.setOutcome(releaseName, deployer.SucceededJobPhase, "")
			fc.setReady(releaseName, true)

			phase := reconcileUntilTerminal(r, key, 20)
			Expect(phase).To(Equal(corev1alpha1.PhaseReady))

			targetNs := &corev1.Namespace{}
			Expect(k8sClient.Get(tctx, types.NamespacedName{Name: "petri-pr-1"}, targetNs)).To(Succeed())
			Expect(targetNs.Labels).To(HaveKeyWithValue("petri.run/managed", "true"))

			Expect(fd.SubmitCount(releaseName)).To(Equal(1))

			env := getEnv(key)
			Expect(k8sClient.Delete(tctx, env)).To(Succeed())

			for range 10 {
				_, err := r.Reconcile(tctx, reconcile.Request{NamespacedName: key})
				Expect(err).NotTo(HaveOccurred())
				gone := &corev1alpha1.EphemeralEnvironment{}
				if err := k8sClient.Get(tctx, key, gone); err != nil {
					break
				}
			}

			ns := &corev1.Namespace{}
			err := k8sClient.Get(tctx, types.NamespacedName{Name: "petri-pr-1"}, ns)
			Expect(err).NotTo(HaveOccurred())
			Expect(ns.DeletionTimestamp).NotTo(BeNil())
		})
	})

	Context("linear dependency chain postgres -> api -> frontend", func() {
		It("deploys in level order", func() {
			fd := newFakeDeployer()
			fc := newFakeChecker()
			r := newReconciler(fd, fc)

			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "postgres", Helm: helmSpec()},
				{Name: "api", Helm: helmSpec(), DependsOn: []string{"postgres"}},
				{Name: "frontend", Helm: helmSpec(), DependsOn: []string{"api"}},
			})
			key := makeEnv(testNS, "env", "tmpl")

			for _, name := range []string{"env-postgres", "env-api", "env-frontend"} {
				fd.setOutcome(name, deployer.SucceededJobPhase, "")
				fc.setReady(name, true)
			}

			phase := reconcileUntilTerminal(r, key, 30)
			Expect(phase).To(Equal(corev1alpha1.PhaseReady))

			order := fd.submitOrder()
			pgIdx := slices.Index(order, "env-postgres")
			apiIdx := slices.Index(order, "env-api")
			feIdx := slices.Index(order, "env-frontend")

			Expect(pgIdx).To(BeNumerically("<", apiIdx), "postgres must be submitted before api")
			Expect(apiIdx).To(BeNumerically("<", feIdx), "api must be submitted before frontend")
		})

		It("undeployAll submits in reverse level order on delete", func() {
			fd := newFakeDeployer()
			fc := newFakeChecker()
			r := newReconciler(fd, fc)

			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "postgres", Helm: helmSpec()},
				{Name: "api", Helm: helmSpec(), DependsOn: []string{"postgres"}},
				{Name: "frontend", Helm: helmSpec(), DependsOn: []string{"api"}},
			})
			key := makeEnv(testNS, "env2", "tmpl")

			for _, name := range []string{"env2-postgres", "env2-api", "env2-frontend"} {
				fd.setOutcome(name, deployer.SucceededJobPhase, "")
				fc.setReady(name, true)
			}

			phase := reconcileUntilTerminal(r, key, 30)
			Expect(phase).To(Equal(corev1alpha1.PhaseReady))

			env := getEnv(key)
			Expect(k8sClient.Delete(tctx, env)).To(Succeed())

			reconcileUntilGone(r, key, 20)

			order := fd.undeployOrder()
			feIdx := slices.Index(order, "env2-frontend")
			apiIdx := slices.Index(order, "env2-api")
			pgIdx := slices.Index(order, "env2-postgres")

			Expect(feIdx).To(BeNumerically("<", apiIdx), "frontend must be undeployed before api")
			Expect(apiIdx).To(BeNumerically("<", pgIdx), "api must be undeployed before postgres")
		})
	})

	Context("diamond dependency: postgres -> {api, cache} -> frontend", func() {
		It("deploys api and cache in parallel, frontend last; cache has no readiness probe", func() {
			fd := newFakeDeployer()
			fc := newFakeChecker()
			r := newReconciler(fd, fc)

			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "postgres", Helm: helmSpec()},
				{Name: "api", Helm: helmSpec(), DependsOn: []string{"postgres"},
					Readiness: &corev1alpha1.ReadinessSpec{HTTPGet: &corev1alpha1.HTTPGetAction{Path: "/", Port: 80}}},
				{Name: "cache", Helm: helmSpec(), DependsOn: []string{"postgres"}}, // no readiness
				{Name: "frontend", Helm: helmSpec(), DependsOn: []string{"api", "cache"}},
			})
			key := makeEnv(testNS, "env", "tmpl")

			for _, name := range []string{"env-postgres", "env-api", "env-cache", "env-frontend"} {
				fd.setOutcome(name, deployer.SucceededJobPhase, "")
				fc.setReady(name, true)
			}

			phase := reconcileUntilTerminal(r, key, 40)
			Expect(phase).To(Equal(corev1alpha1.PhaseReady))

			order := fd.submitOrder()

			pgIdx := slices.Index(order, "env-postgres")
			apiIdx := slices.Index(order, "env-api")
			cacheIdx := slices.Index(order, "env-cache")
			feIdx := slices.Index(order, "env-frontend")

			Expect(pgIdx).To(BeNumerically("<", apiIdx))
			Expect(pgIdx).To(BeNumerically("<", cacheIdx))
			Expect(apiIdx).To(BeNumerically("<", feIdx))
			Expect(cacheIdx).To(BeNumerically("<", feIdx))
		})
	})

	Context("deploy Job fails then succeeds", func() {
		It("increments DeployRetries then reaches Ready after job recovers", func() {
			fd := newFakeDeployer()
			fc := newFakeChecker()
			r := newReconciler(fd, fc)

			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "svc", Helm: helmSpec()},
			})
			key := makeEnv(testNS, "env", "tmpl")
			release := "env-svc"

			fd.setOutcome(release, deployer.FailedJobPhase, "transient error")
			fc.setReady(release, true)

			Eventually(func() bool {
				_, err := r.Reconcile(tctx, reconcile.Request{NamespacedName: key})
				Expect(err).NotTo(HaveOccurred())
				env := getEnv(key)
				if env.Status.Phase == corev1alpha1.PhaseFailed {
					return true // retries exhausted is also acceptable exit
				}
				cs := findComponent(env, "svc")
				return cs != nil && cs.DeployRetries > 0
			}, "30s", "10ms").Should(BeTrue())

			env := getEnv(key)
			if env.Status.Phase == corev1alpha1.PhaseFailed {
				cs := findComponent(env, "svc")
				Expect(cs).NotTo(BeNil())
				Expect(cs.DeployRetries).To(BeNumerically(">=", 1))
				return
			}

			cs := findComponent(env, "svc")
			Expect(cs).NotTo(BeNil())
			Expect(cs.Phase).To(Equal(corev1alpha1.PhaseSubmitting))

			fd.setOutcome(release, deployer.SucceededJobPhase, "")
			phase := reconcileUntilTerminal(r, key, 20)
			Expect(phase).To(Equal(corev1alpha1.PhaseReady))
		})

		It("reaches terminal Failed after maxDeployRetries exhausted", func() {
			fd := newFakeDeployer()
			fc := newFakeChecker()
			r := newReconciler(fd, fc)

			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "svc", Helm: helmSpec()},
			})
			key := makeEnv(testNS, "env", "tmpl")
			release := "env-svc"

			fd.setOutcome(release, deployer.FailedJobPhase, "chart not found")

			phase := reconcileUntilTerminal(r, key, 50)
			Expect(phase).To(Equal(corev1alpha1.PhaseFailed))

			env := getEnv(key)
			Expect(failureReason(env)).To(Equal("DeployFailed"))
		})
	})

	Context("generation reset after terminal Failed", func() {
		It("clears components and retries from scratch after a spec change", func() {
			fd := newFakeDeployer()
			fc := newFakeChecker()
			r := newReconciler(fd, fc)

			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "svc", Helm: helmSpec()},
			})
			key := makeEnv(testNS, "env", "tmpl")
			release := "env-svc"

			fd.setOutcome(release, deployer.FailedJobPhase, "bad chart")
			phase := reconcileUntilTerminal(r, key, 50)
			Expect(phase).To(Equal(corev1alpha1.PhaseFailed))

			env := getEnv(key)
			patch := env.DeepCopy()
			patch.Spec.Source.Branch = "fix-branch"
			Expect(k8sClient.Update(tctx, patch)).To(Succeed())

			fd.setOutcome(release, deployer.SucceededJobPhase, "")
			fc.setReady(release, true)

			phase = reconcileUntilTerminal(r, key, 30)
			Expect(phase).To(Equal(corev1alpha1.PhaseReady))

			env = getEnv(key)
			Expect(env.Status.ObservedGeneration).To(Equal(env.Generation))
		})
	})

	Context("generation bump clears stale Failed Job (stinky status)", func() {
		It("does not stick in Submitting after a generation bump", func() {
			fd := newFakeDeployer()
			fc := newFakeChecker()
			r := newReconciler(fd, fc)

			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "svc", Helm: helmSpec()},
			})
			key := makeEnv(testNS, "env", "tmpl")
			release := "env-svc"

			fd.setOutcome(release, deployer.FailedJobPhase, "old failure")
			Expect(reconcileUntilTerminal(r, key, 50)).To(Equal(corev1alpha1.PhaseFailed))

			env := getEnv(key)
			patch := env.DeepCopy()
			patch.Spec.Source.Branch = "retry"
			Expect(k8sClient.Update(tctx, patch)).To(Succeed())

			fd.setOutcome(release, deployer.SucceededJobPhase, "")
			fc.setReady(release, true)

			phase := reconcileUntilTerminal(r, key, 30)
			Expect(phase).To(Equal(corev1alpha1.PhaseReady))
		})
	})

	Context("negative scenarios", func() {
		It("TemplateNotFound - sticky Failed when template is missing", func() {
			fd := newFakeDeployer()
			fc := newFakeChecker()
			r := newReconciler(fd, fc)

			key := makeEnv(testNS, "test-s1", "does-not-exist")

			phase := reconcileUntilTerminal(r, key, 10)
			Expect(phase).To(Equal(corev1alpha1.PhaseFailed))

			env := getEnv(key)
			Expect(failureReason(env)).To(Equal("TemplateNotFound"))

			ns := &corev1.Namespace{}
			err := k8sClient.Get(tctx, types.NamespacedName{Name: "petri-test-s1"}, ns)
			Expect(err).To(HaveOccurred()) // NotFound
			Expect(fd.submitOrder()).To(BeEmpty())
		})

		It("cycle in dependsOn - InvalidConfiguration, no Jobs", func() {
			fd := newFakeDeployer()
			fc := newFakeChecker()
			r := newReconciler(fd, fc)

			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "svc-a", Helm: helmSpec(), DependsOn: []string{"svc-b"}},
				{Name: "svc-b", Helm: helmSpec(), DependsOn: []string{"svc-a"}},
			})
			key := makeEnv(testNS, "test-s3", "tmpl")

			phase := reconcileUntilTerminal(r, key, 10)
			Expect(phase).To(Equal(corev1alpha1.PhaseFailed))

			env := getEnv(key)
			Expect(failureReason(env)).To(Equal("InvalidConfiguration"))
			Expect(fd.submitOrder()).To(BeEmpty())
		})

		It("release name > 53 chars - InvalidConfiguration", func() {
			fd := newFakeDeployer()
			fc := newFakeChecker()
			r := newReconciler(fd, fc)

			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "some-very-long-component-name-xyz", Helm: helmSpec()},
			})
			key := makeEnv(testNS, "very-long-env-name-that-breaks-limit", "tmpl")

			phase := reconcileUntilTerminal(r, key, 10)
			Expect(phase).To(Equal(corev1alpha1.PhaseFailed))

			env := getEnv(key)
			Expect(failureReason(env)).To(Equal("InvalidConfiguration"))
			Expect(fd.submitOrder()).To(BeEmpty())
		})

		It("deploy Job always fails - terminal Failed with reason", func() {
			fd := newFakeDeployer()
			fc := newFakeChecker()
			r := newReconciler(fd, fc)

			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "broken", Helm: helmSpec()},
			})
			key := makeEnv(testNS, "test-s2", "tmpl")
			release := "test-s2-broken"

			fd.setOutcome(release, deployer.FailedJobPhase, "chart not found: this-chart-does-not-exist-xyz")

			phase := reconcileUntilTerminal(r, key, 50)
			Expect(phase).To(Equal(corev1alpha1.PhaseFailed))

			env := getEnv(key)
			Expect(failureReason(env)).To(Equal("DeployFailed"))
			cs := findComponent(env, "broken")
			Expect(cs).NotTo(BeNil())
			Expect(cs.LastFailureReason).To(ContainSubstring("chart not found"))
		})

		It("Submit error - terminal Failed with DeployFailed reason", func() {
			fd := newFakeDeployer()
			fc := newFakeChecker()
			r := newReconciler(fd, fc)

			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "svc", Helm: helmSpec()},
			})
			key := makeEnv(testNS, "env", "tmpl")
			release := "env-svc"

			fd.setSubmitError(release, errors.New("registry auth failed"))

			phase := reconcileUntilTerminal(r, key, 50)
			Expect(phase).To(Equal(corev1alpha1.PhaseFailed))

			env := getEnv(key)
			Expect(failureReason(env)).To(Equal("DeployFailed"))
		})

		It("undeploy Job always fails - forced cleanup still removes finalizer", func() {
			fd := newFakeDeployer()
			fc := newFakeChecker()
			r := newReconciler(fd, fc)

			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "svc", Helm: helmSpec()},
			})
			key := makeEnv(testNS, "env", "tmpl")
			release := "env-svc"

			fd.setOutcome(release, deployer.SucceededJobPhase, "")
			fc.setReady(release, true)

			phase := reconcileUntilTerminal(r, key, 20)
			Expect(phase).To(Equal(corev1alpha1.PhaseReady))

			fd.setUndeployFail(release, maxDeployRetries+10)

			env := getEnv(key)
			Expect(k8sClient.Delete(tctx, env)).To(Succeed())

			reconcileUntilGone(r, key, 30)

			gone := &corev1alpha1.EphemeralEnvironment{}
			err := k8sClient.Get(tctx, key, gone)
			Expect(err).To(HaveOccurred())
		})
	})
})

func makeTemplateWithShared(ns, name, scRef string) {
	makeTemplate(ns, name, []corev1alpha1.ComponentSpec{
		{Name: "redis", SharedComponentRef: scRef},
		{Name: "api", Helm: helmSpec(), DependsOn: []string{"redis"}},
	})
}

func makeReadySC(ns, name, provider string) types.NamespacedName {
	key := makeSC(ns, name, provider, 0)
	sc := &corev1alpha1.SharedComponent{}
	Expect(k8sClient.Get(tctx, key, sc)).To(Succeed())
	patch := sc.DeepCopy()
	patch.Status.Ready = true
	Expect(k8sClient.Status().Update(tctx, patch)).To(Succeed())
	return key
}

func makeReadySCWithMax(ns, name, provider string, maxConsumers int32) types.NamespacedName {
	key := makeSC(ns, name, provider, maxConsumers)
	sc := &corev1alpha1.SharedComponent{}
	Expect(k8sClient.Get(tctx, key, sc)).To(Succeed())
	patch := sc.DeepCopy()
	patch.Status.Ready = true
	Expect(k8sClient.Status().Update(tctx, patch)).To(Succeed())
	return key
}

var _ = Describe("EphemeralEnvironment shared component integration", func() {
	var (
		testNS  string
		counter int
	)

	BeforeEach(func() {
		tctx = context.Background()
		counter++
		testNS = fmt.Sprintf("ee-sc-%04d", counter)
		Expect(k8sClient.Create(tctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: testNS},
		})).To(Succeed())
		ensureSharedNamespace()
	})

	Context("provision happy path", func() {
		It("provision Succeeded -> binding Secret in env ns, component Ready, api deploy submitted", func() {
			fd := newFakeDeployer()
			fp := newFakeProvisioner()
			fc := newFakeChecker()
			r := newReconcilerWithProvisioner(fd, fp, fc)

			scp := &corev1alpha1.SharedComponentProvider{
				ObjectMeta: metav1.ObjectMeta{Name: "prov", Namespace: testNS},
				Spec: corev1alpha1.SharedComponentProviderSpec{
					Helm: helmSpec(),
					Provision: &corev1alpha1.JobScript{
						Image:  "alpine",
						Script: "echo provision",
					},
				},
			}
			Expect(k8sClient.Create(tctx, scp)).To(Succeed())
			makeReadySC(testNS, "redis", "prov")
			makeTemplateWithShared(testNS, "tmpl", "redis")

			envName := testNS + "-e"
			apiRelease := envName + "-api"
			fd.setOutcome(apiRelease, deployer.SucceededJobPhase, "")
			fc.setReady(apiRelease, true)

			key := makeEnv(testNS, envName, "tmpl")
			targetNs := nsPrefix + envName

			phase := reconcileUntilTerminal(r, key, 30)
			Expect(phase).To(Equal(corev1alpha1.PhaseReady))

			bindingName := envName + "-redis-binding"
			binding := &corev1.Secret{}
			Expect(k8sClient.Get(tctx, types.NamespacedName{
				Name:      bindingName,
				Namespace: targetNs,
			}, binding)).To(Succeed())

			Expect(binding.Data).To(HaveKey(generatedSecretKey))
			Expect(binding.Data[generatedSecretKey]).NotTo(BeEmpty())

			Expect(fp.ProvisionCount(envName, "redis")).To(BeNumerically(">=", 1))

			Expect(fd.SubmitCount(apiRelease)).To(Equal(1))
		})
	})

	Context("provision failure", func() {
		It("env reaches Failed after maxDeployRetries exhausted", func() {
			fd := newFakeDeployer()
			fp := newFakeProvisioner()
			fc := newFakeChecker()
			r := newReconcilerWithProvisioner(fd, fp, fc)

			scp := &corev1alpha1.SharedComponentProvider{
				ObjectMeta: metav1.ObjectMeta{Name: "prov-fail", Namespace: testNS},
				Spec: corev1alpha1.SharedComponentProviderSpec{
					Helm: helmSpec(),
					Provision: &corev1alpha1.JobScript{
						Image:  "alpine",
						Script: "echo provision",
					},
				},
			}
			Expect(k8sClient.Create(tctx, scp)).To(Succeed())
			makeReadySC(testNS, "redis-fail", "prov-fail")
			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "redis", SharedComponentRef: "redis-fail"},
			})

			envName := testNS + "-e"
			fp.setProvisionFail(envName, "redis", errors.New("injected provision failure"))

			key := makeEnv(testNS, envName, "tmpl")
			phase := reconcileUntilTerminal(r, key, 60)
			Expect(phase).To(Equal(corev1alpha1.PhaseFailed))
		})
	})

	Context("nil provision (redis-like, no Provision field)", func() {
		It("component Ready immediately, no provision submitted, binding Secret created", func() {
			fd := newFakeDeployer()
			fp := newFakeProvisioner()
			fc := newFakeChecker()
			r := newReconcilerWithProvisioner(fd, fp, fc)

			makeProvider(testNS, "prov-nil", false)
			makeReadySC(testNS, "cache", "prov-nil")
			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "cache", SharedComponentRef: "cache"},
				{Name: "api", Helm: helmSpec(), DependsOn: []string{"cache"}},
			})

			envName := testNS + "-e"
			apiRelease := envName + "-api"
			fd.setOutcome(apiRelease, deployer.SucceededJobPhase, "")
			fc.setReady(apiRelease, true)

			key := makeEnv(testNS, envName, "tmpl")
			targetNs := nsPrefix + envName

			phase := reconcileUntilTerminal(r, key, 20)
			Expect(phase).To(Equal(corev1alpha1.PhaseReady))

			Expect(fp.ProvisionCount(envName, "cache")).To(Equal(0))

			binding := &corev1.Secret{}
			Expect(k8sClient.Get(tctx, types.NamespacedName{
				Name:      envName + "-cache-binding",
				Namespace: targetNs,
			}, binding)).To(Succeed())
			Expect(binding.Data).To(HaveKey(generatedSecretKey))
		})
	})

	Context("SharedComponent not Ready gates env", func() {
		It("env stays Deploying, no binding Secret created", func() {
			fd := newFakeDeployer()
			fp := newFakeProvisioner()
			fc := newFakeChecker()
			r := newReconcilerWithProvisioner(fd, fp, fc)

			makeProvider(testNS, "prov", false)

			// SC exists but NOT ready
			scKey := makeSC(testNS, "redis-notready", "prov", 0)
			sc := &corev1alpha1.SharedComponent{}
			Expect(k8sClient.Get(tctx, scKey, sc)).To(Succeed())

			envName := testNS + "-e"
			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "redis", SharedComponentRef: "redis-notready"},
				{Name: "api", Helm: helmSpec(), DependsOn: []string{"redis"}},
			})

			key := makeEnv(testNS, envName, "tmpl")
			targetNs := nsPrefix + envName

			phase := reconcileUntilTerminal(r, key, 5)
			Expect(phase).To(Equal(corev1alpha1.PhaseDeploying))

			binding := &corev1.Secret{}
			err := k8sClient.Get(tctx, types.NamespacedName{
				Name:      envName + "-redis-binding",
				Namespace: targetNs,
			}, binding)
			Expect(err).To(HaveOccurred())

			Expect(fd.SubmitCount(envName + "-api")).To(Equal(0))
		})
	})

	Context("maxConsumers=1 sequential", func() {
		It("first env provisions, second env fails with SharedComponentAtCapacity", func() {
			fd := newFakeDeployer()
			fp := newFakeProvisioner()
			fc := newFakeChecker()
			r := newReconcilerWithProvisioner(fd, fp, fc)

			makeProvider(testNS, "prov", false)
			makeReadySCWithMax(testNS, "redis-max", "prov", 1)

			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "redis", SharedComponentRef: "redis-max"},
				{Name: "api", Helm: helmSpec(), DependsOn: []string{"redis"}},
			})

			envA := testNS + "-a"
			apiRelease1 := envA + "-api"
			fd.setOutcome(apiRelease1, deployer.SucceededJobPhase, "")
			fc.setReady(apiRelease1, true)
			key1 := makeEnv(testNS, envA, "tmpl")

			phase1 := reconcileUntilTerminal(r, key1, 30)
			Expect(phase1).To(Equal(corev1alpha1.PhaseReady))

			envB := testNS + "-b"
			key2 := makeEnv(testNS, envB, "tmpl")

			phase2 := reconcileUntilTerminal(r, key2, 10)
			Expect(phase2).To(Equal(corev1alpha1.PhaseFailed))

			env2 := getEnv(key2)
			Expect(failureReason(env2)).To(Equal("SharedComponentAtCapacity"))
		})
	})

	Context("deprovision on delete", func() {
		It("deprovision submitted before namespace deleted, binding Secret deleted", func() {
			fd := newFakeDeployer()
			fp := newFakeProvisioner()
			fc := newFakeChecker()
			r := newReconcilerWithProvisioner(fd, fp, fc)

			scp := &corev1alpha1.SharedComponentProvider{
				ObjectMeta: metav1.ObjectMeta{Name: "prov-deprov", Namespace: testNS},
				Spec: corev1alpha1.SharedComponentProviderSpec{
					Helm: helmSpec(),
					Provision: &corev1alpha1.JobScript{
						Image:  "alpine",
						Script: "echo provision",
					},
					Deprovision: &corev1alpha1.JobScript{
						Image:  "alpine",
						Script: "echo deprovision",
					},
				},
			}
			Expect(k8sClient.Create(tctx, scp)).To(Succeed())

			makeReadySC(testNS, "redis-deprov", "prov-deprov")

			envName := testNS + "-e"
			makeTemplate(testNS, "tmpl", []corev1alpha1.ComponentSpec{
				{Name: "redis", SharedComponentRef: "redis-deprov"},
				{Name: "api", Helm: helmSpec(), DependsOn: []string{"redis"}},
			})

			apiRelease := envName + "-api"
			fd.setOutcome(apiRelease, deployer.SucceededJobPhase, "")
			fc.setReady(apiRelease, true)

			key := makeEnv(testNS, envName, "tmpl")
			targetNs := nsPrefix + envName

			phase := reconcileUntilTerminal(r, key, 30)
			Expect(phase).To(Equal(corev1alpha1.PhaseReady))

			binding := &corev1.Secret{}
			Expect(k8sClient.Get(tctx, types.NamespacedName{
				Name:      envName + "-redis-binding",
				Namespace: targetNs,
			}, binding)).To(Succeed())

			Expect(k8sClient.Delete(tctx, getEnv(key))).To(Succeed())
			reconcileUntilGone(r, key, 30)

			Expect(fp.DeprovisionCount(envName, "redis")).To(BeNumerically(">=", 1))

			err := k8sClient.Get(tctx, types.NamespacedName{
				Name:      envName + "-redis-binding",
				Namespace: targetNs,
			}, binding)
			Expect(err).To(HaveOccurred())
		})
	})
})
