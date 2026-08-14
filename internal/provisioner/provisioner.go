package provisioner

import (
	"context"

	"github.com/nuromirg/petri/api/v1alpha1"
	"github.com/nuromirg/petri/internal/deployer"
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
