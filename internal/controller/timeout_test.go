package controller

import (
	"testing"
	"time"
)

func TestResolveDeployTimeout(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"empty uses fallback", "", defaultDeployTimeout, false},
		{"valid duration", "20m", 20 * time.Minute, false},
		{"unparsable", "bad", 0, true},
		{"zero rejected", "0s", 0, true},
		{"negative rejected", "-5m", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := resolveDeployTimeout(tc.in, defaultDeployTimeout)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (value %v)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
