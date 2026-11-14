package containerd

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestDumpRunningSeesTheProcessNamingTheImageDirectory(t *testing.T) {
	imgDir := filepath.Join(t.TempDir(), "criu", "abc", "7")
	if dumpRunning(imgDir) {
		t.Fatal("nothing names the directory yet")
	}
	cmd := exec.Command("sh", "-c", "sleep 30", "sh", imgDir) // the OCI runtime's checkpoint names the image directory
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start sh: %v", err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	if !dumpRunning(imgDir) {
		t.Fatal("the process naming the image directory is not seen")
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	if dumpRunning(imgDir) {
		t.Fatal("still seen after it exited")
	}
}
