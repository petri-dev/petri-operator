package secretgen

import "testing"

func TestRandom(t *testing.T) {
	s, err := Random(32, "alphanumeric")
	if err != nil || len(s) != 32 {
		t.Fatalf("len = %d, err = %v", len(s), err)
	}

	for _, c := range s {
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
		if !ok {
			t.Fatalf("unexpected char %q in %q", c, s)
		}
	}

	other, _ := Random(32, "alphanumeric")
	if s == other {
		t.Fatal("two random values are identical")
	}

	d, _ := Random(0, "alphanumeric")
	if len(d) != 24 {
		t.Fatalf("default length = %d, want 24", len(d))
	}
}
