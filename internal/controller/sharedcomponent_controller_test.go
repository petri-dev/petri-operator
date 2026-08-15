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
	"fmt"

	corev1alpha1 "github.com/petri-dev/petri-operator/api/v1alpha1"
	"github.com/petri-dev/petri-operator/internal/deployer"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func newSCReconciler(fd *fakeDeployer) *SharedComponentReconciler {
	return &SharedComponentReconciler{
		Client:   k8sClient,
		Scheme:   scheme.Scheme,
		Deployer: fd,
	}
}

func reconcileSC(r *SharedComponentReconciler, key types.NamespacedName, n int) {
	for range n {
		_, err := r.Reconcile(tctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
	}
}

func getSC(key types.NamespacedName) *corev1alpha1.SharedComponent {
	sc := &corev1alpha1.SharedComponent{}
	Expect(k8sClient.Get(tctx, key, sc)).To(Succeed())
	return sc
}

func makeProvider(ns, name string, helmNil bool) {
	spec := corev1alpha1.SharedComponentProviderSpec{}
	if !helmNil {
		spec.Helm = &corev1alpha1.HelmSpec{
			Repo:    "https://charts.example.com",
			Chart:   "petri-stub-app",
			Version: "1.0.0",
		}
	}
	scp := &corev1alpha1.SharedComponentProvider{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       spec,
	}
	Expect(k8sClient.Create(tctx, scp)).To(Succeed())
}

func makeProviderWithInstanceSecret(ns, name, secretName string) {
	scp := &corev1alpha1.SharedComponentProvider{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1alpha1.SharedComponentProviderSpec{
			Helm: &corev1alpha1.HelmSpec{
				Repo:    "https://charts.example.com",
				Chart:   "petri-stub-app",
				Version: "1.0.0",
			},
			InstanceSecret: &corev1alpha1.InstanceSecret{
				Name: secretName,
				Keys: map[string]corev1alpha1.InstanceKey{
					"generated-key": {Generate: &corev1alpha1.GenerateSpec{Length: 16, Charset: "alphanumeric"}},
					"static-key":    {Value: "hello"},
				},
			},
		},
	}
	Expect(k8sClient.Create(tctx, scp)).To(Succeed())
}

func makeSC(ns, name, provider string, maxConsumers int32) types.NamespacedName {
	sc := &corev1alpha1.SharedComponent{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1alpha1.SharedComponentSpec{
			Provider:     provider,
			MaxConsumers: maxConsumers,
		},
	}
	Expect(k8sClient.Create(tctx, sc)).To(Succeed())
	return types.NamespacedName{Name: name, Namespace: ns}
}

func ensureSharedNamespace() {
	ns := &corev1.Namespace{}
	err := k8sClient.Get(tctx, types.NamespacedName{Name: sharedNamespace}, ns)
	if err == nil {
		return
	}
	Expect(k8sClient.Create(tctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   sharedNamespace,
			Labels: map[string]string{managedLabel: "true"},
		},
	})).To(Succeed())
}

var _ = Describe("SharedComponent Controller", func() {
	var (
		testNS  string
		counter int
	)

	BeforeEach(func() {
		tctx = context.Background()
		counter++
		testNS = fmt.Sprintf("sc-test-%04d", counter)
		Expect(k8sClient.Create(tctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: testNS},
		})).To(Succeed())
		ensureSharedNamespace()
	})

	Context("provider not found", func() {
		It("SC stays not Ready, no error", func() {
			fd := newFakeDeployer()
			r := newSCReconciler(fd)

			key := makeSC(testNS, "redis", "missing-provider", 0)

			reconcileSC(r, key, 3)

			sc := getSC(key)
			Expect(sc.Status.Ready).To(BeFalse())
			Expect(fd.submitOrder()).To(BeEmpty())
		})
	})

	Context("provider has no helm spec", func() {
		It("SC stays not Ready", func() {
			fd := newFakeDeployer()
			r := newSCReconciler(fd)

			makeProvider(testNS, "prov-no-helm", true)
			key := makeSC(testNS, "redis", "prov-no-helm", 0)

			reconcileSC(r, key, 3)

			sc := getSC(key)
			Expect(sc.Status.Ready).To(BeFalse())
			Expect(fd.submitOrder()).To(BeEmpty())
		})
	})

	Context("provider found with helm", func() {
		It("SC becomes Ready after deploy succeeds", func() {
			fd := newFakeDeployer()
			r := newSCReconciler(fd)

			makeProvider(testNS, "prov", false)
			key := makeSC(testNS, "redis", "prov", 0)

			releaseName := "shared-redis"
			fd.setOutcome(releaseName, deployer.SucceededJobPhase, "")

			reconcileSC(r, key, 5)

			sc := getSC(key)
			Expect(sc.Status.Ready).To(BeTrue())
			Expect(fd.SubmitCount(releaseName)).To(BeNumerically(">=", 1))
		})

		It("instanceSecret: generate key gets random value, value key gets literal", func() {
			fd := newFakeDeployer()
			r := newSCReconciler(fd)

			secretName := "redis-instance-secret"
			makeProviderWithInstanceSecret(testNS, "prov-is", secretName)
			key := makeSC(testNS, "redis-is", "prov-is", 0)

			releaseName := "shared-redis-is"
			fd.setOutcome(releaseName, deployer.SucceededJobPhase, "")

			reconcileSC(r, key, 5)

			sc := getSC(key)
			Expect(sc.Status.Ready).To(BeTrue())

			secret := &corev1.Secret{}
			Expect(k8sClient.Get(tctx, types.NamespacedName{
				Name:      secretName,
				Namespace: sharedNamespace,
			}, secret)).To(Succeed())

			Expect(secret.Data).To(HaveKey("static-key"))
			Expect(string(secret.Data["static-key"])).To(Equal("hello"))

			Expect(secret.Data).To(HaveKey("generated-key"))
			Expect(secret.Data["generated-key"]).To(HaveLen(16))

			firstVal := string(secret.Data["generated-key"])
			reconcileSC(r, key, 3)
			Expect(k8sClient.Get(tctx, types.NamespacedName{
				Name:      secretName,
				Namespace: sharedNamespace,
			}, secret)).To(Succeed())
			Expect(string(secret.Data["generated-key"])).To(Equal(firstVal))
		})
	})

	Context("SC delete with no consumers", func() {
		It("submits undeploy", func() {
			fd := newFakeDeployer()
			r := newSCReconciler(fd)

			makeProvider(testNS, "prov", false)
			scName := testNS + "-redis"
			key := makeSC(testNS, scName, "prov", 0)

			releaseName := "shared-" + scName
			fd.setOutcome(releaseName, deployer.SucceededJobPhase, "")

			reconcileSC(r, key, 5)
			Expect(getSC(key).Status.Ready).To(BeTrue())

			sc := getSC(key)
			Expect(k8sClient.Delete(tctx, sc)).To(Succeed())

			for range 10 {
				_, err := r.Reconcile(tctx, reconcile.Request{NamespacedName: key})
				Expect(err).NotTo(HaveOccurred())
				gone := &corev1alpha1.SharedComponent{}
				if err := k8sClient.Get(tctx, key, gone); err != nil {
					break
				}
			}

			Expect(fd.undeployOrder()).To(ContainElement(releaseName))
		})
	})
})
