package provisioner

import (
	"context"

	"github.com/petri-dev/petri-operator/api/v1alpha1"
	"github.com/petri-dev/petri-operator/internal/deployer"
)

type Provisioner interface {
	SubmitProvision(ctx context.Context, opts ProvisionOptions) error
	ObserveProvision(ctx context.Context, opts ProvisionOptions) (deployer.JobState, error)
	SubmitDeprovision(ctx context.Context, opts ProvisionOptions) error
	ObserveDeprovision(ctx context.Context, opts ProvisionOptions) (deployer.JobState, error)
}

type ProvisionOptions struct {
	EnvName              string
	ComponentName        string
	SharedName           string
	Script               v1alpha1.JobScript
	ProvisionerSecretRef string
}
