package deployer

import (
	"context"

	"github.com/nuromirg/petri/api/v1alpha1"
)

type Deployer interface {
	Deploy(ctx context.Context, opts DeployOptions) error
	Undeploy(ctx context.Context, opts DeployOptions) error
}

type DeployOptions struct {
	Namespace   string
	ReleaseName string
	Component   v1alpha1.ComponentSpec
}
