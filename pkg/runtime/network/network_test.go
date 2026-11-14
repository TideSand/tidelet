package network

import (
	"strings"
	"testing"
)

func TestParseResolvConf(t *testing.T) {
	ns, search := parseResolvConf("nameserver 127.0.0.53\nnameserver 10.0.0.2\nsearch ec2.internal corp\noptions edns0\n")
	if len(ns) != 1 || ns[0] != "10.0.0.2" {
		t.Fatalf("nameservers = %v", ns)
	}
	if len(search) != 2 || search[1] != "corp" {
		t.Fatalf("search = %v", search)
	}
}

func TestBuildResolvConfPrefersCNI(t *testing.T) {
	out := buildResolvConf([]string{"9.9.9.9"}, []string{"example"})
	if !strings.Contains(out, "nameserver 9.9.9.9\n") || !strings.Contains(out, "search example\n") {
		t.Fatalf("resolv.conf = %q", out)
	}
}

func TestDefaultConfListIsValidJSON(t *testing.T) {
	if !strings.Contains(defaultConfList, `"type": "bridge"`) {
		t.Fatal("default conf missing bridge")
	}
}
