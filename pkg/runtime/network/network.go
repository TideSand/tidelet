// Package network attaches sandboxes to the node's CNI network. Each
// bridged sandbox gets its own network namespace, an IP from the CNI
// bridge plugin with outbound NAT, and generated /etc/hosts and
// /etc/resolv.conf files. Other sandboxes can join an existing namespace,
// which is how the containers of a Pod share one network identity.
package network

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/containerd/containerd/v2/pkg/netns"
	cni "github.com/containerd/go-cni"
)

// DefaultConfDir is where CNI network configurations live.
const DefaultConfDir = "/etc/cni/net.d"

// DefaultPluginDir is where CNI plugin binaries live.
const DefaultPluginDir = "/opt/cni/bin"

// defaultConfList is written when the configuration directory has no
// network. It mirrors containerd's CRI example: a bridge with host-local
// IPAM and masquerading.
const defaultConfList = `{
  "cniVersion": "1.0.0",
  "name": "tidelet",
  "plugins": [
    {
      "type": "bridge",
      "bridge": "tidelet0",
      "isGateway": true,
      "ipMasq": true,
      "promiscMode": true,
      "ipam": {
        "type": "host-local",
        "ranges": [[{"subnet": "10.88.0.0/16"}]],
        "routes": [{"dst": "0.0.0.0/0"}]
      }
    },
    {
      "type": "portmap",
      "capabilities": {"portMappings": true}
    }
  ]
}
`

// Config configures the network manager.
type Config struct {
	ConfDir    string
	PluginDirs []string
	// NetNSDir is where namespaces are bind-mounted. It must survive
	// daemon restarts.
	NetNSDir string
	// FilesDir holds the generated /etc/hosts and /etc/resolv.conf per
	// namespace.
	FilesDir string
	// WriteDefaultConf writes defaultConfList into ConfDir when it holds
	// no configuration.
	WriteDefaultConf bool
	Logger           *slog.Logger
}

// Attachment is the result of Setup.
type Attachment struct {
	// NamespacePath is the bind-mounted network namespace.
	NamespacePath string
	// IPs are the assigned addresses.
	IPs []string
	// HostsFile and ResolvConf are generated files to bind-mount into the
	// sandbox.
	HostsFile  string
	ResolvConf string
}

// Manager sets up and tears down sandbox networks.
type Manager struct {
	cfg Config
	log *slog.Logger
	mu  sync.Mutex
	cni cni.CNI
}

// New loads the CNI configuration.
func New(cfg Config) (*Manager, error) {
	if cfg.ConfDir == "" {
		cfg.ConfDir = DefaultConfDir
	}
	if len(cfg.PluginDirs) == 0 {
		cfg.PluginDirs = []string{DefaultPluginDir}
	}
	if cfg.NetNSDir == "" || cfg.FilesDir == "" {
		return nil, errors.New("network: NetNSDir and FilesDir are required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	for _, d := range []string{cfg.NetNSDir, cfg.FilesDir, cfg.ConfDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	if cfg.WriteDefaultConf {
		if err := ensureDefaultConf(cfg.ConfDir, cfg.Logger); err != nil {
			return nil, err
		}
	}
	c, err := cni.New(
		cni.WithMinNetworkCount(2), // loopback + one real network
		cni.WithPluginConfDir(cfg.ConfDir),
		cni.WithPluginDir(cfg.PluginDirs),
		cni.WithInterfacePrefix("eth"),
	)
	if err != nil {
		return nil, fmt.Errorf("init cni: %w", err)
	}
	if err := c.Load(cni.WithLoNetwork, cni.WithDefaultConf); err != nil {
		return nil, fmt.Errorf("load cni config from %s: %w", cfg.ConfDir, err)
	}
	m := &Manager{cfg: cfg, log: cfg.Logger.With("component", "network"), cni: c}
	m.log.Info("cni loaded", "confDir", cfg.ConfDir, "networks", len(c.GetConfig().Networks))
	return m, nil
}

func ensureDefaultConf(dir string, log *slog.Logger) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".conflist") || strings.HasSuffix(e.Name(), ".conf") || strings.HasSuffix(e.Name(), ".json") {
			return nil
		}
	}
	path := filepath.Join(dir, "10-tidelet.conflist")
	log.Info("writing default CNI configuration", "path", path)
	return os.WriteFile(path, []byte(defaultConfList), 0o644)
}

// Setup creates a network namespace for id, attaches it to the CNI
// network, and writes the sandbox's hosts and resolv.conf files.
func (m *Manager) Setup(ctx context.Context, id, hostname string) (att Attachment, retErr error) {
	ns, err := netns.NewNetNS(m.cfg.NetNSDir)
	if err != nil {
		return Attachment{}, fmt.Errorf("create network namespace: %w", err)
	}
	att.NamespacePath = ns.GetPath()
	defer func() {
		if retErr != nil {
			_ = m.cni.Remove(ctx, id, att.NamespacePath)
			_ = ns.Remove()
		}
	}()
	m.mu.Lock()
	res, err := m.cni.Setup(ctx, id, att.NamespacePath, cni.WithArgs("IgnoreUnknown", "1"))
	m.mu.Unlock()
	if err != nil {
		return Attachment{}, fmt.Errorf("cni setup: %w", err)
	}
	for name, iface := range res.Interfaces {
		if !strings.HasPrefix(name, "eth") {
			continue
		}
		for _, ip := range iface.IPConfigs {
			att.IPs = append(att.IPs, ip.IP.String())
		}
	}
	var nameservers, search []string
	for _, d := range res.DNS {
		nameservers = append(nameservers, d.Nameservers...)
		search = append(search, d.Search...)
	}
	att.HostsFile, att.ResolvConf, err = m.writeFiles(att.NamespacePath, hostname, att.IPs, nameservers, search)
	if err != nil {
		return Attachment{}, err
	}
	m.log.Info("network attached", "id", id, "ips", att.IPs, "netns", att.NamespacePath)
	return att, nil
}

// Files returns the hosts and resolv.conf paths generated for an existing
// namespace, for sandboxes that join it.
func (m *Manager) Files(namespacePath string) (hostsFile, resolvConf string, err error) {
	dir := m.filesDir(namespacePath)
	hostsFile, resolvConf = filepath.Join(dir, "hosts"), filepath.Join(dir, "resolv.conf")
	for _, p := range []string{hostsFile, resolvConf} {
		if _, err := os.Stat(p); err != nil {
			return "", "", fmt.Errorf("network namespace %s has no generated files: %w", namespacePath, err)
		}
	}
	return hostsFile, resolvConf, nil
}

func (m *Manager) filesDir(namespacePath string) string {
	return filepath.Join(m.cfg.FilesDir, filepath.Base(namespacePath))
}

func (m *Manager) writeFiles(namespacePath, hostname string, ips, nameservers, search []string) (string, string, error) {
	dir := m.filesDir(namespacePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", err
	}
	var hosts strings.Builder
	hosts.WriteString("127.0.0.1\tlocalhost\n::1\tlocalhost ip6-localhost ip6-loopback\n")
	for _, ip := range ips {
		addr, _, _ := strings.Cut(ip, "/")
		fmt.Fprintf(&hosts, "%s\t%s\n", addr, hostname)
	}
	hostsPath := filepath.Join(dir, "hosts")
	if err := os.WriteFile(hostsPath, []byte(hosts.String()), 0o644); err != nil {
		return "", "", err
	}
	resolv := buildResolvConf(nameservers, search)
	resolvPath := filepath.Join(dir, "resolv.conf")
	if err := os.WriteFile(resolvPath, []byte(resolv), 0o644); err != nil {
		return "", "", err
	}
	return hostsPath, resolvPath, nil
}

// buildResolvConf prefers CNI-provided DNS, then the host's upstream
// resolvers. On systemd-resolved hosts /etc/resolv.conf points at a stub
// listener that is unreachable from another network namespace, so the
// upstream list is read instead.
func buildResolvConf(nameservers, search []string) string {
	if len(nameservers) == 0 {
		for _, p := range []string{"/run/systemd/resolve/resolv.conf", "/etc/resolv.conf"} {
			data, err := os.ReadFile(p)
			if err != nil {
				continue
			}
			ns, srch := parseResolvConf(string(data))
			if len(ns) > 0 {
				nameservers, search = ns, append(search, srch...)
				break
			}
		}
	}
	if len(nameservers) == 0 {
		nameservers = []string{"1.1.1.1", "8.8.8.8"}
	}
	var b strings.Builder
	b.WriteString("# Generated by tidelet\n")
	for _, ns := range nameservers {
		fmt.Fprintf(&b, "nameserver %s\n", ns)
	}
	if len(search) > 0 {
		fmt.Fprintf(&b, "search %s\n", strings.Join(search, " "))
	}
	return b.String()
}

func parseResolvConf(data string) (nameservers, search []string) {
	for _, line := range strings.Split(data, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "nameserver":
			if !strings.HasPrefix(fields[1], "127.") {
				nameservers = append(nameservers, fields[1])
			}
		case "search":
			search = append(search, fields[1:]...)
		}
	}
	return nameservers, search
}

// Teardown detaches and removes the namespace created by Setup. It is
// safe to call for a namespace that is already gone.
func (m *Manager) Teardown(ctx context.Context, id, namespacePath string) error {
	if namespacePath == "" {
		return nil
	}
	m.mu.Lock()
	err := m.cni.Remove(ctx, id, namespacePath)
	m.mu.Unlock()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		m.log.Warn("cni remove", "id", id, "err", err)
	}
	if err := netns.LoadNetNS(namespacePath).Remove(); err != nil {
		return fmt.Errorf("remove network namespace: %w", err)
	}
	if err := os.RemoveAll(m.filesDir(namespacePath)); err != nil {
		return err
	}
	return nil
}
