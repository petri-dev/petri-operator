package controller

import (
	"context"
	"fmt"
	"sync"

	"github.com/nuromirg/petri/api/v1alpha1"
	"github.com/nuromirg/petri/internal/deployer"
	"github.com/nuromirg/petri/internal/provisioner"
)

type CallEvent struct {
	Op          string
	ReleaseName string
}

type fakeDeployer struct {
	mu sync.Mutex

	outcome      map[string]deployer.JobState
	observeCount map[string]int
	submitted    map[string]bool

	calls []CallEvent

	undeployPending map[string]bool
	undeployFail    map[string]int // remaining failure count per release

	submitErr map[string]error
}

func newFakeDeployer() *fakeDeployer {
	return &fakeDeployer{
		outcome:         make(map[string]deployer.JobState),
		observeCount:    make(map[string]int),
		submitted:       make(map[string]bool),
		undeployPending: make(map[string]bool),
		undeployFail:    make(map[string]int),
		submitErr:       make(map[string]error),
	}
}

func (f *fakeDeployer) setOutcome(releaseName string, phase deployer.JobPhase, reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.outcome[releaseName] = deployer.JobState{Phase: phase, Reason: reason}
}

func (f *fakeDeployer) setSubmitError(releaseName string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.submitErr[releaseName] = err
	f.outcome[releaseName] = deployer.JobState{Phase: deployer.FailedJobPhase, Reason: err.Error()}
}

func (f *fakeDeployer) setUndeployFail(releaseName string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.undeployFail[releaseName] = n
}

func (f *fakeDeployer) SubmitCount(releaseName string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c.Op == "submit" && c.ReleaseName == releaseName {
			n++
		}
	}
	return n
}

func (f *fakeDeployer) submitOrder() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := make(map[string]bool)
	var out []string
	for _, c := range f.calls {
		if c.Op == "submit" && !seen[c.ReleaseName] {
			seen[c.ReleaseName] = true
			out = append(out, c.ReleaseName)
		}
	}
	return out
}

func (f *fakeDeployer) undeployOrder() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := make(map[string]bool)
	var out []string
	for _, c := range f.calls {
		if c.Op == "submit-undeploy" && !seen[c.ReleaseName] {
			seen[c.ReleaseName] = true
			out = append(out, c.ReleaseName)
		}
	}
	return out
}

func (f *fakeDeployer) Submit(_ context.Context, opts deployer.DeployOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, CallEvent{Op: "submit", ReleaseName: opts.ReleaseName})

	if err := f.submitErr[opts.ReleaseName]; err != nil {
		return err
	}

	if !f.submitted[opts.ReleaseName] {
		f.submitted[opts.ReleaseName] = true
		f.observeCount[opts.ReleaseName] = 0
	}
	return nil
}

func (f *fakeDeployer) Observe(_ context.Context, opts deployer.DeployOptions) (deployer.JobState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, CallEvent{Op: "observe", ReleaseName: opts.ReleaseName})

	count := f.observeCount[opts.ReleaseName]
	f.observeCount[opts.ReleaseName] = count + 1

	if count == 0 {
		return deployer.JobState{Phase: deployer.PendingJobPhase}, nil
	}

	if st, ok := f.outcome[opts.ReleaseName]; ok {
		return st, nil
	}
	return deployer.JobState{Phase: deployer.SucceededJobPhase}, nil
}

func (f *fakeDeployer) SubmitUndeploy(_ context.Context, opts deployer.DeployOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, CallEvent{Op: "submit-undeploy", ReleaseName: opts.ReleaseName})
	f.undeployPending[opts.ReleaseName] = true
	return nil
}

func (f *fakeDeployer) ObserveUndeploy(_ context.Context, opts deployer.DeployOptions) (deployer.JobState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, CallEvent{Op: "observe-undeploy", ReleaseName: opts.ReleaseName})

	if n := f.undeployFail[opts.ReleaseName]; n > 0 {
		f.undeployFail[opts.ReleaseName] = n - 1
		return deployer.JobState{Phase: deployer.FailedJobPhase, Reason: "injected undeploy failure"}, nil
	}

	if f.undeployPending[opts.ReleaseName] {
		return deployer.JobState{Phase: deployer.SucceededJobPhase}, nil
	}
	return deployer.JobState{Phase: deployer.PendingJobPhase}, nil
}

type fakeChecker struct {
	mu    sync.Mutex
	ready map[string]bool // release name and ready
	calls []string        // release names in call order
}

func newFakeChecker() *fakeChecker {
	return &fakeChecker{ready: make(map[string]bool)}
}

func (c *fakeChecker) setReady(releaseName string, ready bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ready[releaseName] = ready
}

func (c *fakeChecker) CheckOrder() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.calls))
	copy(out, c.calls)
	return out
}

func (c *fakeChecker) IsReady(_ context.Context, _ string, releaseName string, readiness *v1alpha1.ReadinessSpec) (bool, string, error) {
	if readiness == nil {
		return true, "", nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, releaseName)
	if c.ready[releaseName] {
		return true, "", nil
	}
	return false, "not ready", nil
}

type fakeProvisioner struct {
	mu sync.Mutex

	provisionCount    map[string]int
	deprovisionCount  map[string]int
	provisionObserved map[string]int
	deprovObserved    map[string]int

	provisionSubmitted   map[string]bool
	deprovisionSubmitted map[string]bool

	provisionFail   map[string]error
	deprovisionFail map[string]error
}

func newFakeProvisioner() *fakeProvisioner {
	return &fakeProvisioner{
		provisionCount:       make(map[string]int),
		deprovisionCount:     make(map[string]int),
		provisionObserved:    make(map[string]int),
		deprovObserved:       make(map[string]int),
		provisionSubmitted:   make(map[string]bool),
		deprovisionSubmitted: make(map[string]bool),
		provisionFail:        make(map[string]error),
		deprovisionFail:      make(map[string]error),
	}
}

func provKey(envName, componentName string) string {
	return fmt.Sprintf("%s/%s", envName, componentName)
}

func (f *fakeProvisioner) ProvisionCount(envName, componentName string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.provisionCount[provKey(envName, componentName)]
}

func (f *fakeProvisioner) DeprovisionCount(envName, componentName string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deprovisionCount[provKey(envName, componentName)]
}

func (f *fakeProvisioner) setProvisionFail(envName, componentName string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.provisionFail[provKey(envName, componentName)] = err
}

func (f *fakeProvisioner) SubmitProvision(_ context.Context, opts provisioner.ProvisionOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := provKey(opts.EnvName, opts.ComponentName)
	if err := f.provisionFail[k]; err != nil {
		return err
	}
	f.provisionCount[k]++
	if !f.provisionSubmitted[k] {
		f.provisionSubmitted[k] = true
		f.provisionObserved[k] = 0
	}
	return nil
}

func (f *fakeProvisioner) ObserveProvision(_ context.Context, opts provisioner.ProvisionOptions) (deployer.JobState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := provKey(opts.EnvName, opts.ComponentName)
	if err := f.provisionFail[k]; err != nil {
		return deployer.JobState{Phase: deployer.FailedJobPhase, Reason: err.Error()}, nil
	}
	if f.provisionCount[k] == 0 {
		return deployer.JobState{Phase: deployer.PendingJobPhase}, nil
	}
	n := f.provisionObserved[k]
	f.provisionObserved[k] = n + 1
	if n == 0 {
		return deployer.JobState{Phase: deployer.PendingJobPhase}, nil
	}
	return deployer.JobState{Phase: deployer.SucceededJobPhase}, nil
}

func (f *fakeProvisioner) SubmitDeprovision(_ context.Context, opts provisioner.ProvisionOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := provKey(opts.EnvName, opts.ComponentName)
	if err := f.deprovisionFail[k]; err != nil {
		return err
	}
	f.deprovisionCount[k]++
	if !f.deprovisionSubmitted[k] {
		f.deprovisionSubmitted[k] = true
		f.deprovObserved[k] = 0
	}
	return nil
}

func (f *fakeProvisioner) ObserveDeprovision(_ context.Context, opts provisioner.ProvisionOptions) (deployer.JobState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := provKey(opts.EnvName, opts.ComponentName)
	if f.deprovisionCount[k] == 0 {
		return deployer.JobState{Phase: deployer.PendingJobPhase}, nil
	}
	n := f.deprovObserved[k]
	f.deprovObserved[k] = n + 1
	if n == 0 {
		return deployer.JobState{Phase: deployer.PendingJobPhase}, nil
	}
	return deployer.JobState{Phase: deployer.SucceededJobPhase}, nil
}
