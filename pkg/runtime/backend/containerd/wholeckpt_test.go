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
	cmd := exec.Command("sh", "-c", "echo started; read line", "sh", imgDir) // the OCI runtime's checkpoint names the image directory
	// The pipe stays open until Wait: sh blocks reading it.
	if _, err := cmd.StdinPipe(); err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start sh: %v", err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	// /proc shows no arguments for sh until its exec has completed, which its output proves.
	if _, err := stdout.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if !dumpRunning(imgDir) {
		t.Fatal("the process naming the image directory is not seen")
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	if dumpRunning(imgDir) {
		t.Fatal("still seen after it exited")
	}
}
