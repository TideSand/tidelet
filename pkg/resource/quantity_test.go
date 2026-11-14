package resource

import "testing"

func TestParseCPU(t *testing.T) {
	cases := map[string]Millicores{
		"":       0,
		"0":      0,
		"1":      1000,
		"2":      2000,
		"0.5":    500,
		"500m":   500,
		"1500m":  1500,
		"0.001":  1,
		"0.0001": 1, // rounds up
	}
	for in, want := range cases {
		got, err := ParseCPU(in)
		if err != nil {
			t.Fatalf("ParseCPU(%q): %v", in, err)
		}
		if got != want {
			t.Errorf("ParseCPU(%q) = %d, want %d", in, got, want)
		}
	}
	for _, bad := range []string{"-1", "abc", "1x", "m", "1.5m"} {
		if _, err := ParseCPU(bad); err == nil {
			t.Errorf("ParseCPU(%q) expected error", bad)
		}
	}
}

func TestParseMemory(t *testing.T) {
	cases := map[string]Bytes{
		"":      0,
		"1024":  1024,
		"1Ki":   1024,
		"512Mi": 512 << 20,
		"1Gi":   1 << 30,
		"1G":    1_000_000_000,
		"1.5Gi": 3 << 29,
		"100M":  100_000_000,
		"2k":    2000,
	}
	for in, want := range cases {
		got, err := ParseMemory(in)
		if err != nil {
			t.Fatalf("ParseMemory(%q): %v", in, err)
		}
		if got != want {
			t.Errorf("ParseMemory(%q) = %d, want %d", in, got, want)
		}
	}
	for _, bad := range []string{"-1", "abc", "1Xi", "Mi", "99999999999Ei"} {
		if _, err := ParseMemory(bad); err == nil {
			t.Errorf("ParseMemory(%q) expected error", bad)
		}
	}
}

func TestString(t *testing.T) {
	if s := Millicores(1500).String(); s != "1500m" {
		t.Errorf("got %q", s)
	}
	if s := Millicores(2000).String(); s != "2" {
		t.Errorf("got %q", s)
	}
	if s := Bytes(512 << 20).String(); s != "512Mi" {
		t.Errorf("got %q", s)
	}
	if s := Bytes(1000).String(); s != "1000" {
		t.Errorf("got %q", s)
	}
}

func TestSet(t *testing.T) {
	a := Set{CPU: 1000, Memory: 100}
	b := Set{CPU: 500, Memory: 50}
	if got := a.Add(b); got != (Set{CPU: 1500, Memory: 150}) {
		t.Errorf("Add = %v", got)
	}
	if got := b.Sub(a); !got.IsZero() {
		t.Errorf("Sub clamp = %v", got)
	}
	if !b.Fits(a) || a.Fits(b) {
		t.Errorf("Fits wrong")
	}
	if got := a.Scale(1.5); got != (Set{CPU: 1500, Memory: 150}) {
		t.Errorf("Scale = %v", got)
	}
}
