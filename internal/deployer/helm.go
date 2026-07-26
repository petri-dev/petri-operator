package deployer

import (
	"context"
	"errors"
	"fmt"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/registry"
	"helm.sh/helm/v3/pkg/storage/driver"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"
)

type HelmDeployer struct {
	restConfig *rest.Config
}

func NewHelmDeployer(restConfig *rest.Config) *HelmDeployer {
	return &HelmDeployer{restConfig: restConfig}
}

func (h *HelmDeployer) Deploy(ctx context.Context, opts DeployOptions) error {
	if opts.Component.Helm == nil {
		return fmt.Errorf("component %s has no helm spec", opts.Component.Name)
	}

	cfg, err := h.initActionConfig(opts.Namespace)
	if err != nil {
		return err
	}

	registryClient, err := registry.NewClient()
	if err != nil {
		return fmt.Errorf("failed to create registry client: %w", err)
	}

	cfg.RegistryClient = registryClient

	settings := cli.New()
	settings.SetNamespace(opts.Namespace)

	helmSpec := opts.Component.Helm

	locator := action.NewInstall(cfg)
	locator.ChartPathOptions.RepoURL = helmSpec.Repo
	locator.ChartPathOptions.Version = helmSpec.Version

	chartPath, err := locator.ChartPathOptions.LocateChart(helmSpec.Chart, settings)
	if err != nil {
		return fmt.Errorf("locate chart %q: %w", helmSpec.Chart, err)
	}

	ch, err := loader.Load(chartPath)
	if err != nil {
		return fmt.Errorf("load chart %q: %w", chartPath, err)
	}

	values := make(map[string]any)
	for k, v := range helmSpec.Values {
		values[k] = v
	}

	history := action.NewHistory(cfg)
	history.Max = 1
	_, err = history.Run(opts.ReleaseName)

	if errors.Is(err, driver.ErrReleaseNotFound) {
		install := action.NewInstall(cfg)
		install.ReleaseName = opts.ReleaseName
		install.Namespace = opts.Namespace
		install.Version = helmSpec.Version
		install.CreateNamespace = false
		if _, err := install.RunWithContext(ctx, ch, values); err != nil {
			return fmt.Errorf("helm install %s: %w", opts.ReleaseName, err)
		}
	} else if err != nil {
		return fmt.Errorf("helm history %s: %w", opts.ReleaseName, err)
	} else {
		upgrade := action.NewUpgrade(cfg)
		upgrade.Namespace = opts.Namespace
		upgrade.Version = helmSpec.Version
		if _, err := upgrade.RunWithContext(ctx, opts.ReleaseName, ch, values); err != nil {
			return fmt.Errorf("helm upgrade %s: %w", opts.ReleaseName, err)
		}
	}

	return nil
}

func (h *HelmDeployer) Undeploy(ctx context.Context, opts DeployOptions) error {
	cfg, err := h.initActionConfig(opts.Namespace)
	if err != nil {
		return err
	}

	uninstall := action.NewUninstall(cfg)
	if _, err := uninstall.Run(opts.ReleaseName); err != nil {
		if !errors.Is(err, driver.ErrReleaseNotFound) {
			return fmt.Errorf("helm uninstall %s: %w", opts.ReleaseName, err)
		}
	}

	return nil
}

func (h *HelmDeployer) initActionConfig(namespace string) (*action.Configuration, error) {
	cfg := &action.Configuration{}

	flags := &genericclioptions.ConfigFlags{
		Namespace: &namespace,
	}
	flags.WithWrapConfigFn(func(*rest.Config) *rest.Config {
		return h.restConfig
	})

	if err := cfg.Init(flags, namespace, "secret", func(format string, v ...interface{}) {}); err != nil {
		return nil, fmt.Errorf("failed to init action config: %w", err)
	}

	return cfg, nil
}
