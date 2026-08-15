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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/petri-dev/petri-operator/test/utils"
)

const namespace = "petri-system"
const serviceAccountName = "petri-controller-manager"
const metricsServiceName = "petri-controller-manager-metrics-service"
const metricsRoleBindingName = "petri-metrics-binding"

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	BeforeAll(func() {
		By("labeling the namespace to enforce the restricted security policy")
		cmd := exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")
	})

	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace, "--ignore-not-found")
		_, _ = utils.Run(cmd)

		By("cleaning up the metrics ClusterRoleBinding")
		cmd = exec.Command("kubectl", "delete", "clusterrolebinding", metricsRoleBindingName, "--ignore-not-found")
		_, _ = utils.Run(cmd)
	})

	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				By("getting the name of the controller-manager pod")
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				By("validating the pod's status")
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			cmd := exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=petri-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
				"--dry-run=client", "-o", "yaml")
			yaml, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to generate ClusterRoleBinding")
			applyCmd := exec.Command("kubectl", "apply", "-f", "-")
			applyCmd.Stdin = strings.NewReader(yaml)
			_, err = utils.Run(applyCmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to apply ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("ensuring the controller pod is ready")
			verifyControllerPodReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", controllerPodName, "-n", namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True"), "Controller pod not ready")
			}
			Eventually(verifyControllerPodReady, 3*time.Minute, time.Second).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("Serving metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted, 3*time.Minute, time.Second).Should(Succeed())

			// +kubebuilder:scaffold:e2e-metrics-webhooks-readiness

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": [
								"for i in $(seq 1 30); do curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics && exit 0 || sleep 2; done; exit 1"
							],
							"securityContext": {
								"readOnlyRootFilesystem": true,
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccountName": "%s"
					}
				}`, token, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			verifyMetricsAvailable := func(g Gomega) {
				metricsOutput, err := getMetricsOutput()
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
				g.Expect(metricsOutput).NotTo(BeEmpty())
				g.Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
			}
			Eventually(verifyMetricsAvailable, 2*time.Minute).Should(Succeed())
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks
	})
})

var _ = Describe("EphemeralEnvironment", func() {
	SetDefaultEventuallyTimeout(5 * time.Minute)
	SetDefaultEventuallyPollingInterval(5 * time.Second)

	var (
		envName  string
		tmplName string
		envNS    = "default"
	)

	BeforeEach(func() {
		envName = fmt.Sprintf("e2e-%s", utils.RandomSuffix())
		tmplName = fmt.Sprintf("tmpl-%s", utils.RandomSuffix())
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			By("Fetching controller logs on failure")
			cmd := exec.Command("kubectl", "logs",
				"-n", "petri-system",
				"-l", "control-plane=controller-manager",
				"--tail=50")
			if out, err := utils.Run(cmd); err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n%s", out)
			}
		}

		cmd := exec.Command("kubectl", "delete", "ephemeralenvironment", envName,
			"-n", envNS, "--ignore-not-found")
		_, _ = utils.Run(cmd)
		cmd = exec.Command("kubectl", "delete", "environmenttemplate", tmplName,
			"-n", envNS, "--ignore-not-found")
		_, _ = utils.Run(cmd)
	})

	applyFixture := func(name string) {
		projectDir, err := utils.GetProjectDir()
		Expect(err).NotTo(HaveOccurred())

		raw, err := os.ReadFile(filepath.Join(projectDir, "test/e2e/testdata", name))
		Expect(err).NotTo(HaveOccurred())

		content := strings.ReplaceAll(string(raw), "PETRI_E2E_CHART_REF", e2eChartRef)
		content = strings.ReplaceAll(content, "PETRI_E2E_ENV_NAME", envName)
		content = strings.ReplaceAll(content, "PETRI_E2E_TMPL_NAME", tmplName)

		tmpFile, err := os.CreateTemp("", "petri-e2e-*.yaml")
		Expect(err).NotTo(HaveOccurred())
		defer os.Remove(tmpFile.Name())
		_, err = tmpFile.WriteString(content)
		Expect(err).NotTo(HaveOccurred())
		Expect(tmpFile.Close()).To(Succeed())

		cmd := exec.Command("kubectl", "apply", "-f", tmpFile.Name())
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
	}

	envPhase := func() string {
		cmd := exec.Command("kubectl", "get", "ephemeralenvironment", envName,
			"-n", envNS, "-o", "jsonpath={.status.phase}")
		out, err := utils.Run(cmd)
		if err != nil {
			return ""
		}
		return strings.TrimSpace(out)
	}

	Context("single service", func() {
		It("reaches Ready and namespace is labelled", func() {
			applyFixture("single_service.yaml")

			By("waiting for EphemeralEnvironment to reach Ready")
			Eventually(envPhase).Should(Equal("Ready"))

			By("asserting the target namespace exists and is labelled managed")
			cmd := exec.Command("kubectl", "get", "namespace", "petri-"+envName,
				"-o", "jsonpath={.metadata.labels.petri\\.run/managed}")
			out, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("true"))

			By("asserting a deploy Job completed successfully")
			cmd = exec.Command("kubectl", "get", "jobs",
				"-n", "petri-"+envName,
				"-o", `jsonpath={range .items[*]}{.status.succeeded}{"\n"}{end}`)
			out, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			for _, l := range utils.GetNonEmptyLines(out) {
				Expect(l).To(Equal("1"), "expected all deploy Jobs to have succeeded=1")
			}
		})

		It("cleans up namespace on delete (finalizer path)", func() {
			applyFixture("single_service.yaml")

			By("waiting for Ready")
			Eventually(envPhase).Should(Equal("Ready"))

			By("deleting the EphemeralEnvironment")
			cmd := exec.Command("kubectl", "delete", "ephemeralenvironment", envName, "-n", envNS)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for target namespace to be gone or terminating")
			Eventually(func() bool {
				cmd := exec.Command("kubectl", "get", "namespace", "petri-"+envName,
					"-o", "jsonpath={.metadata.deletionTimestamp}")
				out, err := utils.Run(cmd)
				return err != nil || strings.TrimSpace(out) != ""
			}).Should(BeTrue())
		})
	})

	Context("diamond multi-service", func() {
		It("all four components reach Ready", func() {
			applyFixture("diamond.yaml")

			By("waiting for EphemeralEnvironment to reach Ready")
			Eventually(envPhase).Should(Equal("Ready"))

			By("asserting all components are Ready")
			cmd := exec.Command("kubectl", "get", "ephemeralenvironment", envName,
				"-n", envNS,
				"-o", `jsonpath={range .status.components[*]}{.phase}{"\n"}{end}`)
			out, _ := utils.Run(cmd)
			for _, l := range utils.GetNonEmptyLines(out) {
				Expect(strings.TrimSpace(l)).To(Equal("Ready"))
			}
		})
	})

	Context("bad chart causes terminal Failed", func() {
		It("retries then reaches terminal Failed with reason", func() {
			applyFixture("invalid_chart.yaml")

			By("waiting for terminal Failed (retries exhausted)")
			Eventually(envPhase, 10*time.Minute, 15*time.Second).Should(Equal("Failed"))

			By("asserting the failure reason is surfaced")
			cmd := exec.Command("kubectl", "get", "ephemeralenvironment", envName,
				"-n", envNS,
				"-o", `jsonpath={.status.conditions[?(@.type=="Ready")].reason}`)
			out, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("DeployFailed"))
		})
	})
})

func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	By("creating temporary file to store the token request")
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		By("executing kubectl command to create the token")
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		By("parsing the JSON output to extract the token")
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

func getMetricsOutput() (string, error) {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	return utils.Run(cmd)
}

type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
