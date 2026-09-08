package deployer

import (
	"context"

	"github.com/petri-dev/petri-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

type JobPhase string

const (
	PendingJobPhase   JobPhase = "Pending"
	RunningJobPhase   JobPhase = "Running"
	SucceededJobPhase JobPhase = "Succeeded"
	FailedJobPhase    JobPhase = "Failed"
)

const (
	EnvOp   = "PETRI_OP"
	EnvSpec = "PETRI_SPEC"

	OpDeploy   = "deploy"
	OpUndeploy = "undeploy"
)

type Undeploy interface {
	SubmitUndeploy(ctx context.Context, opts DeployOptions) error
	ObserveUndeploy(ctx context.Context, opts DeployOptions) (JobState, error)
}

type Deploy interface {
	Submit(ctx context.Context, opts DeployOptions) error
	Observe(ctx context.Context, opts DeployOptions) (JobState, error)
}

type Deployer interface {
	Deploy
	Undeploy
}

type JobState struct {
	Phase  JobPhase
	Reason string
}

type DeployOptions struct {
	OwnerUID    types.UID
	Namespace   string
	ReleaseName string
	Component   v1alpha1.ComponentSpec
	Attempt     int32 `json:",omitempty"`
}
