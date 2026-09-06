package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoad_EmptyPathUsesDefaults(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Controllers.MaxConcurrentReconciles != 4 {
		t.Errorf("MaxConcurrentReconciles: got %d, want 4", c.Controllers.MaxConcurrentReconciles)
	}
	if c.Controllers.QPS != 10 {
		t.Errorf("QPS: got %v, want 10", c.Controllers.QPS)
	}
	if c.Controllers.Burst != 100 {
		t.Errorf("Burst: got %d, want 100", c.Controllers.Burst)
	}
	if c.Deployer.ServiceAccount != defaultServiceAccount {
		t.Errorf("ServiceAccount: got %q, want %q", c.Deployer.ServiceAccount, defaultServiceAccount)
	}
	if c.LeaderElection.Enabled {
		t.Error("LeaderElection.Enabled: got true, want false (zero value)")
	}
}

func TestLoad_PartialOverridesOnlyNamedFields(t *testing.T) {
	path := writeTemp(t, `
leaderElection:
  enabled: true
  id: my-lock
controllers:
  maxConcurrentReconciles: 8
  defaultDeployTimeout: 20m
`)
	c, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !c.LeaderElection.Enabled || c.LeaderElection.ID != "my-lock" {
		t.Errorf("LeaderElection: got %+v", c.LeaderElection)
	}
	if c.Controllers.MaxConcurrentReconciles != 8 {
		t.Errorf("MaxConcurrentReconciles: got %d, want 8", c.Controllers.MaxConcurrentReconciles)
	}
	if c.Controllers.DefaultDeployTimeout != "20m" {
		t.Errorf("DefaultDeployTimeout: got %q, want 20m", c.Controllers.DefaultDeployTimeout)
	}
	// unset fields still get defaults
	if c.Controllers.QPS != 10 || c.Controllers.Burst != 100 {
		t.Errorf("defaults not applied to unset fields: QPS=%v Burst=%d", c.Controllers.QPS, c.Controllers.Burst)
	}
	if c.Deployer.ServiceAccount != defaultServiceAccount {
		t.Errorf("ServiceAccount default not applied: %q", c.Deployer.ServiceAccount)
	}
}

func TestLoad_UnknownFieldRejected(t *testing.T) {
	path := writeTemp(t, "controllers:\n  bogusField: 1\n")
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for unknown field, got nil")
	}
}

func TestLoad_MissingFileErrors(t *testing.T) {
	if _, err := Load("/nonexistent/petri-config.yaml"); err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestLoad_ControllerDurations(t *testing.T) {
	for _, field := range []string{"defaultDeployTimeout", "jobDeadline"} {
		for _, tc := range []struct {
			value   string
			wantErr bool
		}{
			{"", false},
			{"0s", false},
			{"20m", false},
			{"+20m", false},
			{"-5m", true},
			{"-1ns", true},
			{"bad", true},
		} {
			t.Run(field+"/"+tc.value, func(t *testing.T) {
				path := writeTemp(t, fmt.Sprintf("controllers:\n  %s: %q\n", field, tc.value))
				c, err := Load(path)
				if tc.wantErr {
					if err == nil || !strings.Contains(err.Error(), "controllers."+field) {
						t.Fatalf("expected error naming controllers.%s, got %v", field, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				got := c.Controllers.DefaultDeployTimeout
				if field == "jobDeadline" {
					got = c.Controllers.JobDeadline
				}
				if got != tc.value {
					t.Errorf("got %q, want %q", got, tc.value)
				}
			})
		}
	}
}

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}
