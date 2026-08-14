package provisioner

import (
	"testing"

	"github.com/nuromirg/petri/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

func TestBuildJob(t *testing.T) {
	t.Parallel()
	p := &JobProvisioner{}
	opts := ProvisionOptions{
		EnvName:              "pr-1",
		ComponentName:        "postgres",
		SharedName:           "postgres-preview",
		ProvisionerSecretRef: "shared-postgres-preview-provision-pr-1",
		Script: v1alpha1.JobScript{
			Image:   "postgres:15",
			Script:  "psql -c 'CREATE DATABASE x'",
			EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "admin-creds"}}}},
		},
	}

	job := p.buildJob(opts, OpProvision, jobName(OpProvision, opts.EnvName, opts.ComponentName))

	if job.Namespace != sharedNamespace {
		t.Fatalf("namespace = %q, want %q", job.Namespace, sharedNamespace)
	}

	c := job.Spec.Template.Spec.Containers[0]

	// script form wraps in /bin/sh -c
	if len(c.Command) != 3 || c.Command[0] != "/bin/sh" || c.Command[2] != opts.Script.Script {
		t.Fatalf("script command wrong: %v", c.Command)
	}

	// provider envFrom + appended provision secret
	if len(c.EnvFrom) != 2 || c.EnvFrom[1].SecretRef.Name != opts.ProvisionerSecretRef {
		t.Fatalf("envFrom wiring wrong: %+v", c.EnvFrom)
	}
}

func TestBuildJobCommandForm(t *testing.T) {
	t.Parallel()
	p := &JobProvisioner{}
	opts := ProvisionOptions{
		EnvName:       "pr-1",
		ComponentName: "minio",
		Script: v1alpha1.JobScript{
			Image:   "minio/mc",
			Command: []string{"/mc", "mb", "local/pr-1-bucket"},
		},
	}

	c := p.buildJob(opts, OpProvision, "petri-provision-pr-1-minio").Spec.Template.Spec.Containers[0]

	// command form passes through verbatim, no /bin/sh wrapping
	if len(c.Command) != 3 || c.Command[0] != "/mc" {
		t.Fatalf("command form should pass through: %v", c.Command)
	}
}

func TestJobNameTruncation(t *testing.T) {
	t.Parallel()
	name := jobName(OpDeprovision, "pr-a-very-long-environment-name-that-goes-on", "postgres-component-x")
	if len(name) > 63 {
		t.Fatalf("name %q len %d exceeds 63", name, len(name))
	}
}
