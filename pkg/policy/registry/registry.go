// Package registry maps policy names to constructors. It is the one place
// that imports every policy implementation, so that package policy and
// the implementations never depend on each other's registration.
package registry

import (
	"fmt"
	"sort"

	"github.com/TideSand/tidelet/pkg/policy"
	"github.com/TideSand/tidelet/pkg/policy/agentcgroup"
	"github.com/TideSand/tidelet/pkg/policy/overcommit"
	"github.com/TideSand/tidelet/pkg/policy/strict"
	"github.com/TideSand/tidelet/pkg/policy/tidesand"
)

// Constructor builds a policy from name=value options.
type Constructor func(opts map[string]string) (policy.Policy, error)

var registry = map[string]Constructor{
	// Strict reservation: a sandbox runs only when its limits fit the capacity.
	strict.Name: func(opts map[string]string) (policy.Policy, error) { return strict.New(opts) },
	// Threshold overcommit: admit while measured CPU and memory are under a threshold.
	overcommit.Name: func(opts map[string]string) (policy.Policy, error) { return overcommit.New(opts) },
	// The AgentCgroup baseline (arXiv 2602.09345).
	agentcgroup.Name: func(opts map[string]string) (policy.Policy, error) { return agentcgroup.New(opts) },
	// TideSand.
	tidesand.Name: func(opts map[string]string) (policy.Policy, error) { return tidesand.New(opts) },
}

// Names lists the registered policies.
func Names() []string {
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// New builds the named policy.
func New(name string, opts map[string]string) (policy.Policy, error) {
	ctor, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("unknown policy %q (available: %v)", name, Names())
	}
	return ctor(opts)
}
