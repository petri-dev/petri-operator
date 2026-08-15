package controller

import (
	"testing"

	"github.com/petri-dev/petri-operator/api/v1alpha1"
)

func env(name string) *v1alpha1.EphemeralEnvironment {
	e := &v1alpha1.EphemeralEnvironment{}
	e.Name = name
	return e
}

func helmComponent(name string, values map[string]string, env map[string]v1alpha1.EnvValue) v1alpha1.ComponentSpec {
	return v1alpha1.ComponentSpec{
		Name: name,
		Helm: &v1alpha1.HelmSpec{Values: values},
		Env:  env,
	}
}

func strVal(s string) v1alpha1.EnvValue { return v1alpha1.EnvValue{Value: s} }

func secretVal(component, key string) v1alpha1.EnvValue {
	return v1alpha1.EnvValue{SecretKeyRef: &v1alpha1.EnvSecretRef{Component: component, Key: key}}
}

func TestRenderConsumerValues_SpecValuesOverrideTemplate(t *testing.T) {
	e := &v1alpha1.EphemeralEnvironment{}
	e.Name = "pr-42"
	e.Spec.Values = map[string]string{"image.tag": "pr-42-abc", "replicaCount": "2"}

	c := helmComponent("api", map[string]string{"image.tag": "latest", "replicaCount": "1"}, nil)

	out, err := renderConsumerValues(e, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := out.Helm.Values["image.tag"]; got != "pr-42-abc" {
		t.Errorf("image.tag: got %q, want %q", got, "pr-42-abc")
	}
	if got := out.Helm.Values["replicaCount"]; got != "2" {
		t.Errorf("replicaCount: got %q, want %q", got, "2")
	}
}

func TestRenderConsumerValues_SpecEnvOverrideTemplate(t *testing.T) {
	e := &v1alpha1.EphemeralEnvironment{}
	e.Name = "pr-42"
	e.Spec.Env = map[string]v1alpha1.EnvValue{"FEATURE_X": strVal("true")}

	c := helmComponent("api", nil, map[string]v1alpha1.EnvValue{"FEATURE_X": strVal("false"), "OTHER": strVal("keep")})

	out, err := renderConsumerValues(e, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := out.Env["FEATURE_X"].Value; got != "true" {
		t.Errorf("FEATURE_X: got %q, want %q", got, "true")
	}
	if got := out.Env["OTHER"].Value; got != "keep" {
		t.Errorf("OTHER: got %q, want %q", got, "keep")
	}
}

func TestRenderConsumerValues_TemplateValuesKeptWhenNoOverride(t *testing.T) {
	e := &v1alpha1.EphemeralEnvironment{}
	e.Name = "pr-42"

	c := helmComponent("api", map[string]string{"image.tag": "latest"}, nil)

	out, err := renderConsumerValues(e, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := out.Helm.Values["image.tag"]; got != "latest" {
		t.Errorf("image.tag: got %q, want %q", got, "latest")
	}
}

func TestRenderConsumerValues_SpecEnvSecretKeyRefWaitsIfNotProvisioned(t *testing.T) {
	e := &v1alpha1.EphemeralEnvironment{}
	e.Name = "pr-42"
	e.Spec.Env = map[string]v1alpha1.EnvValue{"DB_PASS": secretVal("postgres", "PGPASSWORD")}

	c := helmComponent("api", nil, nil)

	_, err := renderConsumerValues(e, c, map[string]map[string]string{})
	if err != errSharedNotReady {
		t.Errorf("expected errSharedNotReady, got %v", err)
	}
}

func TestRenderConsumerValues_SpecEnvSecretKeyRefProceeds(t *testing.T) {
	e := &v1alpha1.EphemeralEnvironment{}
	e.Name = "pr-42"
	e.Spec.Env = map[string]v1alpha1.EnvValue{"DB_PASS": secretVal("postgres", "PGPASSWORD")}

	c := helmComponent("api", nil, nil)
	components := map[string]map[string]string{"postgres": {"PGPASSWORD": "secret"}}

	out, err := renderConsumerValues(e, c, components)
	if err != nil {
		t.Fatal(err)
	}
	if got := out.Helm.Values["extraEnvVarsSecret"]; got != "pr-42-postgres-binding" {
		t.Errorf("extraEnvVarsSecret: got %q, want %q", got, "pr-42-postgres-binding")
	}
}

func TestRenderConsumerValues_NonHelmComponentPassthrough(t *testing.T) {
	e := &v1alpha1.EphemeralEnvironment{}
	e.Name = "pr-42"
	e.Spec.Values = map[string]string{"image.tag": "pr-42-abc"}

	c := v1alpha1.ComponentSpec{Name: "postgres", SharedComponentRef: "pg-shared"}

	out, err := renderConsumerValues(e, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out.Helm != nil {
		t.Error("expected Helm to be nil for shared component")
	}
}
