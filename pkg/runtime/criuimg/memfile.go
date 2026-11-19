// Package criuimg rewrites a CRIU image so that the memory of the processes in it is
// shared between everything restored from it.
//
// CRIU dumps a process's private anonymous memory as pages in the image, and a restore
// copies them back: every restore of a 1 GiB process costs 1 GiB. Linearize moves those
// pages into sparse files (on a tmpfs, so they are page-cache pages already in RAM) and
// rewrites each memory area in the image from an anonymous mapping into a private mapping
// of its file. A restore then maps the files: nothing is copied, a page a restored process
// reads is the page-cache page every other restore reads too, and the kernel's ordinary
// copy-on-write gives a process its own copy of a page only when it writes it. No daemon
// sits on the page-fault path, and neither CRIU nor the kernel is patched: private file
// mappings are something CRIU restores for any program.
//
// A process restored this way, dumped again, yields only the pages it wrote since (the
// rest still belong to the files), so a snapshot of a restored process is small; Survey
// reports how much of each kind an image holds, for deciding when to linearize again.
//
// Where a file mapping behaves unlike anonymous memory: growing an area with mremap reads
// the file past its end, so every file is 2^47 bytes long and growth lands on a hole that
// reads as zero; MADV_DONTNEED brings the file's bytes back where anonymous memory would
// read zero, which the containers work around for glibc (see MemMount's users); and areas
// marked MADV_WIPEONFORK or MADV_DONTFORK, growing stacks, locked and huge-page areas stay
// anonymous.
package criuimg

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// MemMount is where the directory of memory files must be bind-mounted, read-only, inside
// every container that is dumped or restored: the image names the files by this path.
const MemMount = "/.tidelet/mem"

const (
	pageSize    = 4096
	memFileSize = 1 << 47
	pePresent   = 1 << 2

	mapPrivate   = 0x02
	mapAnonymous = 0x20
	mapGrowsdown = 0x100
	mapLocked    = 0x2000
	mapHugetlb   = 0x40000

	// madvise advice numbers; vma_entry.madv is a bitmap of them.
	madvDontfork   = 10
	madvWipeonfork = 18
	madvAnonOnly   = 1<<madvDontfork | 1<<madvWipeonfork

	vmaAreaRegular = 1 << 0
	vmaAreaHeap    = 1 << 5
	vmaFilePrivate = 1 << 6
	vmaAnonPrivate = 1 << 9

	fdReg = 1 // fd_types.REG

	// Field numbers: vma_entry, mm_entry, pagemap_entry, file_entry, reg_file_entry, mnt_entry.
	vmaStart, vmaEnd, vmaPgoff, vmaShmid, vmaFlags, vmaStatus, vmaMadv, vmaFdflags = 1, 2, 3, 4, 6, 7, 9, 10
	mmVmas                                                                         = 14
	pmVaddr, pmNrPages, pmFlags, pmNrPages64                                       = 1, 2, 4, 5
	feType, feID, feReg                                                            = 1, 2, 3
	regID, regFlags, regPos, regFown, regName, regMntID                            = 1, 2, 3, 5, 6, 7
	mntID, mntMountpoint                                                           = 2, 7
)

// run is a stretch of dumped pages: its address, length in pages, offset in the pages
// file, and pagemap flags.
type run struct {
	vaddr, pages, offset, flags uint64
}

type vma struct {
	raw                []field
	start, end, pgoff  uint64
	shmid              uint64
	flags, status, adv uint64
	runs               []run
	memFile            string // the memory file it already maps, if any
}

func parseVMA(raw []byte) (*vma, error) {
	fs, err := parse(raw)
	if err != nil {
		return nil, err
	}
	v := &vma{raw: fs}
	for _, f := range fs {
		switch f.Num {
		case vmaStart:
			v.start = f.Val
		case vmaEnd:
			v.end = f.Val
		case vmaPgoff:
			v.pgoff = f.Val
		case vmaShmid:
			v.shmid = f.Val
		case vmaFlags:
			v.flags = f.Val
		case vmaStatus:
			v.status = f.Val
		case vmaMadv:
			v.adv |= f.Val
		}
	}
	return v, nil
}

// convertible: a plain private anonymous area (the heap included) that behaves the same as
// a private file mapping.
func (v *vma) convertible() bool {
	return v.flags&mapAnonymous != 0 && v.flags&mapPrivate != 0 &&
		v.flags&(mapGrowsdown|mapLocked|mapHugetlb) == 0 &&
		v.adv&madvAnonOnly == 0 &&
		v.status&^(vmaAreaRegular|vmaAreaHeap|vmaAnonPrivate) == 0
}

func (v *vma) nrPages() (n uint64) {
	for _, r := range v.runs {
		n += r.pages
	}
	return n
}

// asFileMapping is the area's entry as a private mapping of file fileID from offset 0.
func (v *vma) asFileMapping(fileID uint64) []byte {
	set := map[int]uint64{
		vmaPgoff: 0, vmaShmid: fileID, vmaFlags: v.flags &^ mapAnonymous,
		vmaStatus: v.status&^vmaAnonPrivate | vmaFilePrivate, vmaFdflags: uint64(os.O_RDONLY),
	}
	out := make([]field, 0, len(v.raw)+1)
	for _, f := range v.raw {
		if nv, ok := set[f.Num]; ok {
			f.Val = nv
			delete(set, f.Num)
		}
		out = append(out, f)
	}
	for _, n := range []int{vmaPgoff, vmaShmid, vmaFlags, vmaStatus, vmaFdflags} {
		if nv, ok := set[n]; ok {
			out = append(out, vint(n, nv))
		}
	}
	return encode(out)
}

// Survey says how an image's memory is held.
type Survey struct {
	// EagerPages are in the image: a restore copies them.
	EagerPages uint64
	// SharedPages are in the memory files the image maps.
	SharedPages uint64
	// MemFiles are the memory files the image maps (names in the memory directory).
	MemFiles []string
}

// ImageSet is the image directory of one CRIU dump and the directory its memory files live in.
type ImageSet struct {
	dir, memDir string
	files       [][]byte
	regNames    map[uint64]string
}

// Open reads the image in dir; memDir is the host directory mounted at MemMount in the
// dumped container.
func Open(dir, memDir string) (*ImageSet, error) {
	files, err := readImage(filepath.Join(dir, "files.img"), filesMagic, imgCommonMagic)
	if err != nil {
		return nil, err
	}
	s := &ImageSet{dir: dir, memDir: memDir, files: files, regNames: map[uint64]string{}}
	for _, raw := range files {
		d, err := decode(raw)
		if err != nil {
			return nil, err
		}
		if d.num(feType) != fdReg {
			continue
		}
		reg, err := decode(d[feReg].Bytes)
		if err != nil {
			return nil, err
		}
		s.regNames[d.num(feID)] = string(reg[regName].Bytes)
	}
	return s, nil
}

func (s *ImageSet) pids() ([]int, error) {
	paths, err := filepath.Glob(filepath.Join(s.dir, "mm-*.img"))
	if err != nil {
		return nil, err
	}
	var pids []int
	for _, p := range paths {
		n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(filepath.Base(p), "mm-"), ".img"))
		if err == nil {
			pids = append(pids, n)
		}
	}
	sort.Ints(pids)
	return pids, nil
}

// memMountID is the mount ID of MemMount in the dumped container.
func (s *ImageSet) memMountID() (int64, error) {
	paths, _ := filepath.Glob(filepath.Join(s.dir, "mountpoints-*.img"))
	for _, p := range paths {
		entries, err := readImage(p, mntsMagic, imgCommonMagic)
		if err != nil {
			return 0, err
		}
		for _, raw := range entries {
			d, err := decode(raw)
			if err != nil {
				return 0, err
			}
			if mp := string(d[mntMountpoint].Bytes); mp == MemMount || mp == "."+MemMount {
				return int64(d.num(mntID)), nil
			}
		}
	}
	return 0, fmt.Errorf("criuimg: %s: the image has no %s mount", s.dir, MemMount)
}

type pagemap struct {
	head    []byte
	pagesID uint64
	runs    []run
	wide    bool // the entries carry their page count in the 64-bit field
}

func (s *ImageSet) pagemap(pid int) (*pagemap, error) {
	entries, err := readImage(filepath.Join(s.dir, fmt.Sprintf("pagemap-%d.img", pid)), pagemapMagic, imgCommonMagic)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("criuimg: pagemap of %d is empty", pid)
	}
	head, err := decode(entries[0])
	if err != nil {
		return nil, err
	}
	pm := &pagemap{head: entries[0], pagesID: head.num(1)}
	var offset uint64
	for _, raw := range entries[1:] {
		d, err := decode(raw)
		if err != nil {
			return nil, err
		}
		nr := d.num(pmNrPages)
		if d.has(pmNrPages64) {
			nr, pm.wide = d.num(pmNrPages64), true
		}
		flags := d.num(pmFlags)
		if flags&pePresent != 0 {
			pm.runs = append(pm.runs, run{d.num(pmVaddr), nr, offset, flags})
			offset += nr * pageSize
		}
	}
	return pm, nil
}

func (pm *pagemap) entry(r run) []byte {
	if pm.wide {
		return encode([]field{vint(pmVaddr, r.vaddr), vint(pmNrPages, 0), vint(pmFlags, r.flags), vint(pmNrPages64, r.pages)})
	}
	return encode([]field{vint(pmVaddr, r.vaddr), vint(pmNrPages, r.pages), vint(pmFlags, r.flags)})
}

// vmas returns the process's mm entry and its areas, each with the page runs it holds.
func (s *ImageSet) vmas(pid int, pm *pagemap) ([]field, []*vma, error) {
	entries, err := readImage(filepath.Join(s.dir, fmt.Sprintf("mm-%d.img", pid)), mmMagic, imgCommonMagic)
	if err != nil {
		return nil, nil, err
	}
	if len(entries) != 1 {
		return nil, nil, fmt.Errorf("criuimg: mm of %d has %d entries", pid, len(entries))
	}
	mm, err := parse(entries[0])
	if err != nil {
		return nil, nil, err
	}
	var vmas []*vma
	for _, f := range mm {
		if f.Num != mmVmas {
			continue
		}
		v, err := parseVMA(f.Bytes)
		if err != nil {
			return nil, nil, err
		}
		if v.status&vmaFilePrivate != 0 {
			if name := s.regNames[v.shmid]; strings.HasPrefix(name, MemMount+"/") {
				v.memFile = filepath.Base(name)
			}
		}
		vmas = append(vmas, v)
	}
	for _, r := range pm.runs {
		for r.pages > 0 {
			i := sort.Search(len(vmas), func(i int) bool { return vmas[i].start > r.vaddr }) - 1
			if i < 0 || r.vaddr >= vmas[i].end {
				return nil, nil, fmt.Errorf("criuimg: pid %d: page %#x is outside every memory area", pid, r.vaddr)
			}
			v := vmas[i]
			take := min(r.pages, (v.end-r.vaddr)/pageSize)
			v.runs = append(v.runs, run{r.vaddr, take, r.offset, r.flags})
			r.vaddr += take * pageSize
			r.offset += take * pageSize
			r.pages -= take
		}
	}
	return mm, vmas, nil
}

func dataBytes(path string) uint64 {
	var st unix.Stat_t
	if unix.Stat(path, &st) != nil {
		return 0
	}
	return uint64(st.Blocks) * 512
}

// Survey reports how many pages the image holds itself and how many its memory files hold.
func (s *ImageSet) Survey() (Survey, error) {
	var out Survey
	seen := map[string]bool{}
	pids, err := s.pids()
	if err != nil {
		return out, err
	}
	for _, pid := range pids {
		pm, err := s.pagemap(pid)
		if err != nil {
			return out, err
		}
		_, vmas, err := s.vmas(pid, pm)
		if err != nil {
			return out, err
		}
		for _, v := range vmas {
			out.EagerPages += v.nrPages()
			if v.memFile != "" && !seen[v.memFile] {
				seen[v.memFile] = true
				out.MemFiles = append(out.MemFiles, v.memFile)
			}
		}
	}
	sort.Strings(out.MemFiles)
	for _, f := range out.MemFiles {
		out.SharedPages += dataBytes(filepath.Join(s.memDir, f)) / pageSize
	}
	return out, nil
}

func copyRange(src int, srcOff int64, dst int, dstOff int64, length int64) error {
	for length > 0 {
		n, err := unix.CopyFileRange(src, &srcOff, dst, &dstOff, int(min(length, 1<<30)), 0)
		if err != nil {
			return fmt.Errorf("copy_file_range: %w", err)
		}
		if n == 0 {
			return errors.New("copy_file_range: short copy")
		}
		length -= int64(n)
	}
	return nil
}

// copyExtents copies the data (not the holes) of src[srcOff, srcOff+length) to dst at dstOff.
func copyExtents(src int, srcOff int64, dst int, dstOff int64, length int64) error {
	end := srcOff + length
	for pos := srcOff; pos < end; {
		data, err := unix.Seek(src, pos, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) || (err == nil && data >= end) {
			return nil
		}
		if err != nil {
			return err
		}
		hole, err := unix.Seek(src, data, unix.SEEK_HOLE)
		if err != nil {
			return err
		}
		hole = min(hole, end)
		if err := copyRange(src, data, dst, dstOff+(data-srcOff), hole-data); err != nil {
			return err
		}
		pos = hole
	}
	return nil
}

// Linearize moves the memory of every convertible area into a memory file of its own,
// named prefix-<pid>-<start>.mem: the pages in the image, on top of what the area's
// earlier memory file held if it was a mapping of one already. The image keeps only the
// pages of the areas that must stay anonymous. It returns what the rewritten image holds.
func (s *ImageSet) Linearize(prefix string) (Survey, error) {
	var out Survey
	mnt, err := s.memMountID()
	if err != nil {
		return out, err
	}
	var nextID uint64
	for _, raw := range s.files {
		d, err := decode(raw)
		if err != nil {
			return out, err
		}
		nextID = max(nextID, d.num(feID)+1)
	}
	pids, err := s.pids()
	if err != nil {
		return out, err
	}
	var newFiles [][]byte
	for _, pid := range pids {
		pm, err := s.pagemap(pid)
		if err != nil {
			return out, err
		}
		mm, vmas, err := s.vmas(pid, pm)
		if err != nil {
			return out, err
		}
		rewritten := map[*vma][]byte{}
		var names []string
		nextID, names, err = s.linearizeProcess(prefix, pid, pm, vmas, rewritten, nextID, mnt, &newFiles, &out)
		if err != nil {
			for _, n := range names {
				_ = os.Remove(filepath.Join(s.memDir, n))
			}
			return out, err
		}
		if len(rewritten) == 0 {
			continue
		}
		entry := make([]field, 0, len(mm))
		i := 0
		for _, f := range mm {
			if f.Num == mmVmas {
				if b, ok := rewritten[vmas[i]]; ok {
					f.Bytes = b
				}
				i++
			}
			entry = append(entry, f)
		}
		if err := writeImage(filepath.Join(s.dir, fmt.Sprintf("mm-%d.img", pid)), mmMagic, [][]byte{encode(entry)}); err != nil {
			return out, err
		}
	}
	s.files = append(s.files, newFiles...)
	if err := writeImage(filepath.Join(s.dir, "files.img"), filesMagic, s.files); err != nil {
		return out, err
	}
	sort.Strings(out.MemFiles)
	return out, nil
}

func (s *ImageSet) linearizeProcess(prefix string, pid int, pm *pagemap, vmas []*vma, rewritten map[*vma][]byte,
	nextID uint64, mnt int64, newFiles *[][]byte, out *Survey) (uint64, []string, error) {
	var targets []*vma
	for _, v := range vmas {
		if v.memFile != "" || (v.convertible() && len(v.runs) > 0) {
			targets = append(targets, v)
		}
	}
	if len(targets) == 0 {
		for _, v := range vmas {
			out.EagerPages += v.nrPages()
		}
		return nextID, nil, nil
	}
	pagesPath := filepath.Join(s.dir, fmt.Sprintf("pages-%d.img", pm.pagesID))
	pages, err := unix.Open(pagesPath, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nextID, nil, fmt.Errorf("open %s: %w", pagesPath, err)
	}
	defer unix.Close(pages)
	bases := map[string]int{}
	defer func() {
		for _, fd := range bases {
			unix.Close(fd)
		}
	}()
	var names []string
	for _, v := range targets {
		name := fmt.Sprintf("%s-%d-%x.mem", prefix, pid, v.start)
		fd, err := unix.Open(filepath.Join(s.memDir, name), unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC, 0o444)
		if err != nil {
			return nextID, names, fmt.Errorf("create memory file: %w", err)
		}
		names = append(names, name)
		err = unix.Ftruncate(fd, memFileSize)
		if err == nil && v.memFile != "" {
			base, ok := bases[v.memFile]
			if !ok {
				if base, err = unix.Open(filepath.Join(s.memDir, v.memFile), unix.O_RDONLY|unix.O_CLOEXEC, 0); err == nil {
					bases[v.memFile] = base
				}
			}
			if err == nil {
				err = copyExtents(base, int64(v.pgoff), fd, 0, int64(v.end-v.start))
			}
		}
		for _, r := range v.runs {
			if err != nil {
				break
			}
			err = copyRange(pages, int64(r.offset), fd, int64(r.vaddr-v.start), int64(r.pages*pageSize))
		}
		var st unix.Stat_t
		if err == nil {
			err = unix.Fstat(fd, &st)
		}
		unix.Close(fd)
		if err != nil {
			return nextID, names, fmt.Errorf("fill memory file %s: %w", name, err)
		}
		out.SharedPages += uint64(st.Blocks) * 512 / pageSize
		rewritten[v] = v.asFileMapping(nextID)
		*newFiles = append(*newFiles, fileEntry(nextID, MemMount+"/"+name, mnt))
		nextID++
	}
	out.MemFiles = append(out.MemFiles, names...)
	// The pages of the areas left anonymous, packed into a new pages file.
	tmp := pagesPath + ".tmp"
	kept, err := unix.Open(tmp, unix.O_WRONLY|unix.O_CREAT|unix.O_TRUNC|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return nextID, names, err
	}
	entries := [][]byte{pm.head}
	var off int64
	for _, v := range vmas {
		if _, ok := rewritten[v]; ok {
			continue
		}
		for _, r := range v.runs {
			if err = copyRange(pages, int64(r.offset), kept, off, int64(r.pages*pageSize)); err != nil {
				unix.Close(kept)
				return nextID, names, err
			}
			off += int64(r.pages * pageSize)
			entries = append(entries, pm.entry(r))
		}
	}
	unix.Close(kept)
	out.EagerPages += uint64(off) / pageSize
	if err := os.Rename(tmp, pagesPath); err != nil {
		return nextID, names, err
	}
	return nextID, names, writeImage(filepath.Join(s.dir, fmt.Sprintf("pagemap-%d.img", pid)), pagemapMagic, entries)
}

func fileEntry(id uint64, name string, mnt int64) []byte {
	fown := encode([]field{vint(1, 0), vint(2, 0), vint(3, 0), vint(4, 0), vint(5, 0)})
	reg := encode([]field{vint(regID, id), vint(regFlags, uint64(os.O_RDONLY)), vint(regPos, 0), vbytes(regFown, fown),
		vbytes(regName, []byte(name)), vint(regMntID, zigzag(mnt))})
	return encode([]field{vint(feType, fdReg), vint(feID, id), vbytes(feReg, reg)})
}

// PagesBytes is the size of the pages the image itself holds.
func (s *ImageSet) PagesBytes() (n uint64) {
	paths, _ := filepath.Glob(filepath.Join(s.dir, "pages-*.img"))
	for _, p := range paths {
		n += dataBytes(p)
	}
	return n
}

// MemBytes is the size of the named memory files.
func (s *ImageSet) MemBytes(names []string) (n uint64) {
	for _, f := range names {
		n += dataBytes(filepath.Join(s.memDir, f))
	}
	return n
}
