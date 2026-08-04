package graph_test

import (
	"slices"
	"testing"

	"github.com/nuromirg/petri/api/v1alpha1"
	"github.com/nuromirg/petri/internal/graph"
)

func comp(name string, deps ...string) v1alpha1.ComponentSpec {
	return v1alpha1.ComponentSpec{Name: name, DependsOn: deps}
}

func levelNames(levels [][]v1alpha1.ComponentSpec) [][]string {
	out := make([][]string, len(levels))
	for i, level := range levels {
		names := make([]string, len(level))
		for j, c := range level {
			names[j] = c.Name
		}
		slices.Sort(names)
		out[i] = names
	}
	return out
}

func equalLevels(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

func TestBuildLevels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		components []v1alpha1.ComponentSpec
		want       [][]string
		wantErr    bool
	}{
		{
			name:       "empty input",
			components: nil,
			want:       [][]string{},
		},
		{
			name:       "single component no deps",
			components: []v1alpha1.ComponentSpec{comp("a")},
			want:       [][]string{{"a"}},
		},
		{
			name:       "two independent components",
			components: []v1alpha1.ComponentSpec{comp("a"), comp("b")},
			want:       [][]string{{"a", "b"}},
		},
		{
			name:       "linear chain",
			components: []v1alpha1.ComponentSpec{comp("a"), comp("b", "a"), comp("c", "b")},
			want:       [][]string{{"a"}, {"b"}, {"c"}},
		},
		{
			name: "diamond",
			components: []v1alpha1.ComponentSpec{
				comp("a"),
				comp("b", "a"),
				comp("c", "a"),
				comp("d", "b", "c"),
			},
			want: [][]string{{"a"}, {"b", "c"}, {"d"}},
		},
		{
			name:       "join",
			components: []v1alpha1.ComponentSpec{comp("a"), comp("b"), comp("c", "a", "b")},
			want:       [][]string{{"a", "b"}, {"c"}},
		},
		{
			name:       "unknown dependency",
			components: []v1alpha1.ComponentSpec{comp("a", "ghost")},
			wantErr:    true,
		},
		{
			name:       "self cycle",
			components: []v1alpha1.ComponentSpec{comp("a", "a")},
			wantErr:    true,
		},
		{
			name:       "two node cycle",
			components: []v1alpha1.ComponentSpec{comp("a", "b"), comp("b", "a")},
			wantErr:    true,
		},
		{
			name:       "indirect cycle",
			components: []v1alpha1.ComponentSpec{comp("a", "b"), comp("b", "c"), comp("c", "a")},
			wantErr:    true,
		},
		{
			name:       "duplicate names",
			components: []v1alpha1.ComponentSpec{comp("a"), comp("a")},
			wantErr:    true,
		},
		{
			name:       "valid nodes mixed with cycle",
			components: []v1alpha1.ComponentSpec{comp("a"), comp("b", "c"), comp("c", "b")},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			levels, err := graph.BuildLevels(tt.components)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if levels != nil {
					t.Fatalf("expected nil levels on error, got %v", levelNames(levels))
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			got := levelNames(levels)
			if len(tt.want) == 0 {
				if len(got) != 0 {
					t.Fatalf("expected no levels, got %v", got)
				}
				return
			}

			if !equalLevels(got, tt.want) {
				t.Fatalf("levels mismatch:\n got  %v\n want %v", got, tt.want)
			}
		})
	}
}
