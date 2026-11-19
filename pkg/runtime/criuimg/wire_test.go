package criuimg

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestFieldsRoundTrip(t *testing.T) {
	in := []field{vint(1, 0x7f0000001000), vint(2, 300), {Num: 3, Wire: wireFixed8, Val: 1 << 40},
		vbytes(4, []byte("/.tidelet/mem/x.mem")), {Num: 5, Wire: wireFixed4, Val: 7}, vint(9, 1<<18), vint(9, 1<<10)}
	buf := encode(in)
	out, err := parse(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encode(out), buf) || len(out) != len(in) {
		t.Fatalf("round trip changed the message: %v", out)
	}
	if _, err := parse(buf[:len(buf)-1]); err == nil {
		t.Fatal("a truncated message parsed")
	}
}

func TestImageRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mm-1.img")
	entries := [][]byte{encode([]field{vint(1, 1)}), {}, encode([]field{vbytes(2, []byte("abc"))})}
	if err := writeImage(path, mmMagic, entries); err != nil {
		t.Fatal(err)
	}
	got, err := readImage(path, mmMagic, imgCommonMagic)
	if err != nil || len(got) != 3 || !bytes.Equal(got[2], entries[2]) {
		t.Fatalf("read back %v, %v", got, err)
	}
	if _, err := readImage(path, filesMagic, imgCommonMagic); err == nil {
		t.Fatal("wrong magic accepted")
	}
}

func TestVMAConversion(t *testing.T) {
	anon := func(flags, status, madv uint64) *vma {
		raw := encode([]field{vint(vmaStart, 0x1000), vint(vmaEnd, 0x3000), vint(vmaPgoff, 0), vint(vmaShmid, 0),
			vint(5, 3), vint(vmaFlags, flags), vint(vmaStatus, status), vint(8, ^uint64(0)), vint(vmaMadv, madv)})
		v, err := parseVMA(raw)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	plain := anon(mapPrivate|mapAnonymous, vmaAreaRegular|vmaAnonPrivate, 0)
	if !plain.convertible() {
		t.Fatal("a private anonymous area must be convertible")
	}
	for name, v := range map[string]*vma{
		"stack":      anon(mapPrivate|mapAnonymous|mapGrowsdown, vmaAreaRegular|vmaAnonPrivate, 0),
		"locked":     anon(mapPrivate|mapAnonymous|mapLocked, vmaAreaRegular|vmaAnonPrivate, 0),
		"wipeonfork": anon(mapPrivate|mapAnonymous, vmaAreaRegular|vmaAnonPrivate, 1<<madvWipeonfork),
		"shared":     anon(0x01|mapAnonymous, vmaAreaRegular, 0),
		"vdso":       anon(mapPrivate|mapAnonymous, vmaAreaRegular|vmaAnonPrivate|1<<3, 0),
	} {
		if v.convertible() {
			t.Errorf("%s area must stay anonymous", name)
		}
	}
	m, err := decode(plain.asFileMapping(42))
	if err != nil {
		t.Fatal(err)
	}
	if m.num(vmaShmid) != 42 || m.num(vmaFlags)&mapAnonymous != 0 || m.num(vmaStatus)&vmaFilePrivate == 0 ||
		m.num(vmaStatus)&vmaAnonPrivate != 0 || !m.has(vmaFdflags) || m.num(vmaStart) != 0x1000 {
		t.Fatalf("file mapping entry = %v", m)
	}
}

func TestCopyExtentsSkipsHoles(t *testing.T) {
	dir := t.TempDir()
	src, err := os.Create(filepath.Join(dir, "src"))
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	dst, err := os.Create(filepath.Join(dir, "dst"))
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	const hole = 1 << 20
	if _, err := src.WriteAt([]byte("head"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := src.WriteAt([]byte("tail"), hole); err != nil {
		t.Fatal(err)
	}
	if err := copyExtents(int(src.Fd()), 0, int(dst.Fd()), pageSize, hole+4); err != nil {
		t.Skipf("copy_file_range unavailable here: %v", err)
	}
	got := make([]byte, 4)
	if _, err := dst.ReadAt(got, pageSize+hole); err != nil || string(got) != "tail" {
		t.Fatalf("tail = %q, %v", got, err)
	}
	if _, err := dst.ReadAt(got, pageSize); err != nil || string(got) != "head" {
		t.Fatalf("head = %q, %v", got, err)
	}
}
