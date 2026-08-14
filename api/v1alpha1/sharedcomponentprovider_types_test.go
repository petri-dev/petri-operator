package v1alpha1

import "testing"

func TestJobScriptValidate(t *testing.T) {
	cases := []struct {
		name    string
		js      JobScript
		wantErr bool
	}{
		{"script only", JobScript{Image: "postgres:15", Script: "psql"}, false},
		{"command only", JobScript{Image: "minio/mc", Command: []string{"/mc", "mb"}}, false},
		{"both set", JobScript{Image: "x", Script: "psql", Command: []string{"/mc"}}, true},
		{"neither set", JobScript{Image: "x"}, true},
		{"no image", JobScript{Script: "psql"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := c.js.Validate(); (err != nil) != c.wantErr {
				t.Fatalf("Validate() err = %v, wantErr = %v", err, c.wantErr)
			}
		})
	}
}
