package config

import (
	"fmt"
	"os"
	"time"

	"github.com/petri-dev/petri-operator/internal/controller"
	"sigs.k8s.io/yaml"
)

// Config is the operator's behavioral configuration, loaded from a YAML file via --config. Low-level serving settings (metrics/probe addresses, TLS certs,  http2) stay as command-line flags.
// This file, though, holds controller behavior only.
type Config struct {
	LeaderElection LeaderElection `json:"leaderElection"`
	Controllers    Controllers    `json:"controllers"`
	Deployer       Deployer       `json:"deployer"`
}

type LeaderElection struct {
	Enabled bool   `json:"enabled,omitempty"`
	ID      string `json:"id,omitempty"`
}

type Controllers struct {
	MaxConcurrentReconciles int     `json:"maxConcurrentReconciles,omitempty"`
	QPS                     float64 `json:"qps,omitempty"`
	Burst                   int     `json:"burst,omitempty"`

	// DefaultDeployTimeout is the fallback readiness timeout when a template
	// does not set spec.deployTimeout. Non-negative Go duration string (e.g. "15m"); zero uses the default.
	DefaultDeployTimeout string `json:"defaultDeployTimeout,omitempty"`

	// JobDeadline bounds deploy/provision Job runtime (ActiveDeadlineSeconds).
	// Non-negative Go duration string; zero disables the deadline.
	JobDeadline string `json:"jobDeadline,omitempty"`
}

type Deployer struct {
	Image          string `json:"image,omitempty"`
	ServiceAccount string `json:"serviceAccount,omitempty"`
}

const defaultServiceAccount = "petri-deployer"

func Load(path string) (Config, error) {
	var c Config
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config %q: %w", path, err)
		}

		if err := yaml.UnmarshalStrict(data, &c); err != nil {
			return Config{}, fmt.Errorf("parse config %q: %w", path, err)
		}
	}
	for _, field := range []struct{ name, value string }{
		{"defaultDeployTimeout", c.Controllers.DefaultDeployTimeout},
		{"jobDeadline", c.Controllers.JobDeadline},
	} {
		if field.value == "" {
			continue
		}
		d, err := time.ParseDuration(field.value)
		if err != nil {
			return Config{}, fmt.Errorf("controllers.%s: %w", field.name, err)
		}
		if d < 0 {
			return Config{}, fmt.Errorf("controllers.%s must be non-negative", field.name)
		}
	}
	c.applyDefaults()
	return c, nil
}

func (c *Config) applyDefaults() {
	rl := controller.DefaultRateLimitOptions()
	if c.Controllers.MaxConcurrentReconciles == 0 {
		c.Controllers.MaxConcurrentReconciles = rl.MaxConcurrentReconciles
	}
	if c.Controllers.QPS == 0 {
		c.Controllers.QPS = rl.QPS
	}
	if c.Controllers.Burst == 0 {
		c.Controllers.Burst = rl.Burst
	}
	if c.Deployer.ServiceAccount == "" {
		c.Deployer.ServiceAccount = defaultServiceAccount
	}

	// NOTE: DefaultDeployTimeout and JobDeadline are controller package responsibility.
}
