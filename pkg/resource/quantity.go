// Package resource provides normalised CPU and memory quantities with
// Kubernetes-compatible parsing.
//
// CPU is measured in millicores (1000 = one CPU). Memory is measured in
// bytes. A zero value means "unset" throughout Tidelet.
package resource

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Millicores is a CPU quantity. 1000 millicores equal one CPU.
type Millicores int64

// Bytes is a memory quantity in bytes.
type Bytes int64

// ErrInvalidQuantity is returned for unparsable quantity strings.
var ErrInvalidQuantity = errors.New("invalid quantity")

// ParseCPU parses a Kubernetes CPU quantity: "2", "0.5", "500m".
func ParseCPU(s string) (Millicores, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	if strings.HasSuffix(s, "m") {
		v, err := strconv.ParseInt(strings.TrimSuffix(s, "m"), 10, 64)
		if err != nil || v < 0 {
			return 0, fmt.Errorf("%w: cpu %q", ErrInvalidQuantity, s)
		}
		return Millicores(v), nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f < 0 || math.IsInf(f, 0) || math.IsNaN(f) {
		return 0, fmt.Errorf("%w: cpu %q", ErrInvalidQuantity, s)
	}
	// Round up to the next millicore like Kubernetes does.
	return Millicores(math.Ceil(f * 1000)), nil
}

// String renders the quantity in the shortest Kubernetes form.
func (m Millicores) String() string {
	if m == 0 {
		return "0"
	}
	if m%1000 == 0 {
		return strconv.FormatInt(int64(m)/1000, 10)
	}
	return strconv.FormatInt(int64(m), 10) + "m"
}

// Cores returns the quantity as a floating point number of CPUs.
func (m Millicores) Cores() float64 { return float64(m) / 1000 }

var memorySuffixes = []struct {
	suffix string
	mult   int64
}{
	{"Ei", 1 << 60}, {"Pi", 1 << 50}, {"Ti", 1 << 40}, {"Gi", 1 << 30}, {"Mi", 1 << 20}, {"Ki", 1 << 10},
	{"E", 1e18}, {"P", 1e15}, {"T", 1e12}, {"G", 1e9}, {"M", 1e6}, {"k", 1e3}, {"K", 1e3},
}

// ParseMemory parses a Kubernetes memory quantity: "128974848", "512Mi",
// "1G", "1.5Gi".
func ParseMemory(s string) (Bytes, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	mult := int64(1)
	num := s
	for _, sfx := range memorySuffixes {
		if strings.HasSuffix(s, sfx.suffix) {
			mult = sfx.mult
			num = strings.TrimSuffix(s, sfx.suffix)
			break
		}
	}
	if i, err := strconv.ParseInt(num, 10, 64); err == nil {
		if i < 0 || (mult > 1 && i > math.MaxInt64/mult) {
			return 0, fmt.Errorf("%w: memory %q", ErrInvalidQuantity, s)
		}
		return Bytes(i * mult), nil
	}
	f, err := strconv.ParseFloat(num, 64)
	if err != nil || f < 0 || math.IsInf(f, 0) || math.IsNaN(f) {
		return 0, fmt.Errorf("%w: memory %q", ErrInvalidQuantity, s)
	}
	v := math.Ceil(f * float64(mult))
	if v > math.MaxInt64 {
		return 0, fmt.Errorf("%w: memory %q", ErrInvalidQuantity, s)
	}
	return Bytes(v), nil
}

// String renders the quantity using the largest binary suffix that divides
// it exactly, falling back to plain bytes.
func (b Bytes) String() string {
	if b == 0 {
		return "0"
	}
	for _, sfx := range memorySuffixes[:6] {
		if int64(b)%sfx.mult == 0 {
			return strconv.FormatInt(int64(b)/sfx.mult, 10) + sfx.suffix
		}
	}
	return strconv.FormatInt(int64(b), 10)
}

// Set is a bundle of resource quantities. Zero means unset.
type Set struct {
	CPU    Millicores `json:"cpu"`
	Memory Bytes      `json:"memory"`
}

// Add returns s + o.
func (s Set) Add(o Set) Set { return Set{CPU: s.CPU + o.CPU, Memory: s.Memory + o.Memory} }

// Sub returns s - o, clamped at zero.
func (s Set) Sub(o Set) Set {
	return Set{CPU: max(s.CPU-o.CPU, 0), Memory: max(s.Memory-o.Memory, 0)}
}

// Fits reports whether s fits within capacity on every resource.
func (s Set) Fits(capacity Set) bool {
	return s.CPU <= capacity.CPU && s.Memory <= capacity.Memory
}

// IsZero reports whether nothing is set.
func (s Set) IsZero() bool { return s.CPU == 0 && s.Memory == 0 }

// Scale multiplies every resource by factor.
func (s Set) Scale(factor float64) Set {
	return Set{
		CPU:    Millicores(math.Round(float64(s.CPU) * factor)),
		Memory: Bytes(math.Round(float64(s.Memory) * factor)),
	}
}

func (s Set) String() string {
	return fmt.Sprintf("cpu=%s memory=%s", s.CPU, s.Memory)
}
