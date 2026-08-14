package renderer

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
)

type Vars struct {
	Env        EnvVars
	Components map[string]map[string]string
	// Instance holds keys from the instance Secret (instanceSecret in the provider).
	// Available in binding.secretKeys so providers can reference shared credentials (e.g. redis has one password for all consumers: {{.Instance.REDIS_PASSWORD}}).
	Instance map[string]string
}

type EnvVars struct {
	Name string
	// Slug is Name with hyphens replaced by underscores, safe for use as a SQL identifier or filename without quoting (e.g. "pr-1" -> "pr_1").
	Slug            string
	GeneratedSecret string
}

// EnvVarsFor builds EnvVars for an environment name, auto-filling Slug.
func EnvVarsFor(name, generatedSecret string) EnvVars {
	return EnvVars{
		Name:            name,
		Slug:            strings.ReplaceAll(name, "-", "_"),
		GeneratedSecret: generatedSecret,
	}
}

// Render executes a single template string. missingkey=error so an unresolved key fails loudly instead of injecting an empty string.
func Render(s string, v Vars) (string, error) {
	t, err := template.New("").Option("missingkey=error").Parse(s)
	if err != nil {
		return "", fmt.Errorf("parse template %q: %w", s, err)
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, v); err != nil {
		return "", fmt.Errorf("execute template %q: %w", s, err)
	}

	return buf.String(), nil
}

// TODO write a description for the method
func RenderMap(m map[string]string, v Vars) (map[string]string, error) {
	out := make(map[string]string, len(m))
	for k, val := range m {
		r, err := Render(val, v)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", k, err)
		}
		out[k] = r
	}

	return out, nil
}
