package renderer

import "testing"

/*
 *
 * TODO
 * ADD MORE TESTS, ACHIEVE ACCEPTABLE COVERAGE
 *
 */

func TestRender(t *testing.T) {
	t.Parallel()
	v := Vars{Env: EnvVars{Name: "pr-1", GeneratedSecret: "abc123"}}

	got, err := Render("db-{{.Env.Name}}-{{.Env.GeneratedSecret}}", v)
	if err != nil || got != "db-pr-1-abc123" {
		t.Fatalf("got %q err %v", got, err)
	}

	if _, err := Render("{{.Env.Nope}}", v); err == nil {
		t.Fatal("expected error on unknown key")
	}
}
