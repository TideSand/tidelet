// Package version holds build-time version information.
package version

// Version is the release this source belongs to, bumped by release-please; a build may
// override it via -ldflags (the Makefile uses git describe).
var Version = "0.1.1" // x-release-please-version
