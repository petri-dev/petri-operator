//go:build e2e
// +build e2e

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

package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/petri-dev/petri-operator/test/utils"
)

var (
	// managerImage is the manager image to be built and loaded for testing.
	managerImage  = "petri-operator:e2e"
	deployerImage = "petri-deployer:e2e"
	// e2eChartRef is the OCI ref for the stub chart pushed to the local registry. Set during BeforeSuite after the registry is ready.
	e2eChartRef = ""
	// shouldCleanupCertManager tracks whether CertManager was installed by this suite.
	shouldCleanupCertManager = false
)

// TestE2E runs the e2e test suite to validate the solution in an isolated environment.
// The default setup requires Kind and CertManager.
//
// To enable kubectl kuberc (use custom kubectl configurations), set: KUBECTL_KUBERC=true
// By default, kuberc is disabled to ensure consistent test behavior across different environments.
// To skip CertManager installation, set: CERT_MANAGER_INSTALL_SKIP=true
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting petri e2e test suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func(ctx context.Context) {
	By("building the manager image")
	cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", managerImage))
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the manager image")

	By("loading the manager image on Kind")
	err = utils.LoadImageToKindClusterWithName(ctx, managerImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the manager image into Kind")

	By("building the deployer image")
	if v := os.Getenv("DEPLOYER_IMG"); v != "" {
		deployerImage = v
	}
	cmd = exec.Command("make", "docker-build-deployer", fmt.Sprintf("DEPLOYER_IMG=%s", deployerImage))
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the deployer image")

	By("loading the deployer image on Kind")
	err = utils.LoadImageToKindClusterWithName(ctx, deployerImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the deployer image into Kind")

	By("deploying the operator with both images")
	cmd = exec.Command("make", "deploy",
		fmt.Sprintf("IMG=%s", managerImage),
		fmt.Sprintf("DEPLOYER_IMG=%s", deployerImage),
		"HELM_EXTRA_ARGS=--set metrics.enabled=true",
	)
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to deploy the operator")

	By("waiting for CRDs to be established")
	cmd = exec.Command("kubectl", "wait", "--for=condition=Established",
		"crd/ephemeralenvironments.core.petri.run",
		"crd/environmenttemplates.core.petri.run",
		"--timeout=60s")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "CRDs not established in time")

	By("pushing the stub chart to the local OCI registry")
	registryPort := os.Getenv("E2E_REGISTRY_PORT")
	if registryPort == "" {
		registryPort = "5001"
	}
	e2eChartRef = utils.PushStubChart(ctx, registryPort)

	configureKubectlKubeRC()
	setupCertManager(ctx)
})

var _ = AfterSuite(func(ctx context.Context) {
	By("undeploying the operator")
	cmd := exec.CommandContext(ctx, "make", "undeploy")
	_, _ = utils.Run(cmd)

	By("uninstalling CRDs")
	cmd = exec.CommandContext(ctx, "make", "uninstall", "ignore-not-found=true")
	_, _ = utils.Run(cmd)

	teardownCertManager(ctx)
})

// Disable kubectl kuberc by default for test isolation.
// This prevents local kubectl configurations from affecting test behavior.
// To enable kuberc, set: KUBECTL_KUBERC=true
func configureKubectlKubeRC() {
	if os.Getenv("KUBECTL_KUBERC") != "true" {
		By("disabling kubectl kuberc for test isolation")
		err := os.Setenv("KUBECTL_KUBERC", "false")
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to disable kubectl kuberc")
		_, _ = fmt.Fprintf(GinkgoWriter,
			"kubectl kuberc disabled for consistent test behavior (override with KUBECTL_KUBERC=true)\n")
	} else {
		_, _ = fmt.Fprintf(GinkgoWriter, "kubectl kuberc enabled (KUBECTL_KUBERC=true)\n")
	}
}

// setupCertManager installs CertManager if needed for webhook tests.
// Skips installation if CERT_MANAGER_INSTALL_SKIP=true or if already present.
func setupCertManager(ctx context.Context) {
	if os.Getenv("CERT_MANAGER_INSTALL_SKIP") == "true" {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager installation (CERT_MANAGER_INSTALL_SKIP=true)\n")
		return
	}

	By("checking if CertManager is already installed")
	if utils.IsCertManagerCRDsInstalled(ctx) {
		_, _ = fmt.Fprintf(GinkgoWriter, "CertManager is already installed. Skipping installation.\n")
		return
	}

	// Mark for cleanup before installation to handle interruptions and partial installs.
	shouldCleanupCertManager = true

	By("installing CertManager")
	Expect(utils.InstallCertManager(ctx)).To(Succeed(), "Failed to install CertManager")
}

// teardownCertManager uninstalls CertManager if it was installed by setupCertManager.
// This ensures we only remove what we installed.
func teardownCertManager(ctx context.Context) {
	if !shouldCleanupCertManager {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager cleanup (not installed by this suite)\n")
		return
	}

	By("uninstalling CertManager")
	utils.UninstallCertManager(ctx)
}
