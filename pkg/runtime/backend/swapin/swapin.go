// Package swapin brings a process's swapped-out anonymous memory back into
// RAM from outside the process. It reads /proc/<pid>/maps for the
// writable private mappings (heap, stacks, anonymous allocations: the
// pages that go to swap) and /proc/<pid>/pagemap for which of their pages
// are in swap, then asks the kernel to read those back with
// process_madvise(MADV_WILLNEED), which starts swap-in without touching
// the process. Linux 5.10 or later, and either the same user as the
// target or CAP_SYS_NICE; tideletd runs as root.
package swapin

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	pagemapSwapped = 1 << 62
	pagemapPresent = 1 << 63
	maxIOV         = 1024 // iovecs per process_madvise call (IOV_MAX)
)

// Process swaps in the swapped pages of one process. It returns the bytes
// it asked the kernel for. A process that exited meanwhile is not an
// error: zero bytes come back.
func Process(pid int) (int64, error) {
	ranges, err := swappedRanges(pid)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ESRCH) {
			return 0, nil
		}
		return 0, err
	}
	if len(ranges) == 0 {
		return 0, nil
	}
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		if errors.Is(err, unix.ESRCH) {
			return 0, nil
		}
		return 0, fmt.Errorf("pidfd_open %d: %w", pid, err)
	}
	defer unix.Close(fd)
	var total int64
	for start := 0; start < len(ranges); start += maxIOV {
		end := min(start+maxIOV, len(ranges))
		// The iovec bases are addresses in the target process, not pointers
		// of ours, so the ABI struct is spelled out with integers.
		iov := make([]remoteIovec, 0, end-start)
		var n int64
		for _, r := range ranges[start:end] {
			iov = append(iov, remoteIovec{base: r.start, length: r.end - r.start})
			n += int64(r.end - r.start)
		}
		// process_madvise(pidfd, iovec, vlen, advice, flags)
		_, _, errno := unix.Syscall6(unix.SYS_PROCESS_MADVISE, uintptr(fd), uintptr(unsafe.Pointer(&iov[0])), uintptr(len(iov)), unix.MADV_WILLNEED, 0, 0)
		if errno != 0 {
			if errno == unix.ESRCH {
				return total, nil
			}
			return total, fmt.Errorf("process_madvise %d: %w", pid, errno)
		}
		total += n
	}
	return total, nil
}

type vmRange struct{ start, end uint64 }

// remoteIovec is struct iovec (void *iov_base; size_t iov_len) on a 64-bit
// kernel, with the base as the integer it is in the other process.
type remoteIovec struct {
	base   uint64
	length uint64
}

// swappedRanges lists the page-aligned ranges of pid's private writable
// mappings whose pages are in swap, adjacent swapped pages merged.
func swappedRanges(pid int) ([]vmRange, error) {
	maps, err := os.Open(fmt.Sprintf("/proc/%d/maps", pid))
	if err != nil {
		return nil, err
	}
	defer maps.Close()
	pagemap, err := os.Open(fmt.Sprintf("/proc/%d/pagemap", pid))
	if err != nil {
		return nil, err
	}
	defer pagemap.Close()
	pageSize := uint64(os.Getpagesize())
	var out []vmRange
	buf := make([]byte, 8*4096) // 4096 pagemap entries per read
	sc := bufio.NewScanner(maps)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 5 || len(fields[1]) < 4 || fields[1][1] != 'w' || fields[1][3] != 'p' {
			continue // not a private writable mapping: nothing of it goes to swap
		}
		if len(fields) >= 6 && strings.HasPrefix(fields[5], "[v") {
			continue // [vvar], [vdso], [vsyscall]
		}
		lo, hi, ok := strings.Cut(fields[0], "-")
		if !ok {
			continue
		}
		start, err1 := strconv.ParseUint(lo, 16, 64)
		end, err2 := strconv.ParseUint(hi, 16, 64)
		if err1 != nil || err2 != nil || end <= start {
			continue
		}
		for page := start; page < end; {
			n := min((end-page)/pageSize, uint64(len(buf)/8))
			read, err := pagemap.ReadAt(buf[:n*8], int64(page/pageSize*8))
			if err != nil && err != io.EOF {
				if errors.Is(err, unix.EIO) {
					break // mapping gone
				}
				return nil, fmt.Errorf("pagemap: %w", err)
			}
			for i := uint64(0); i < uint64(read)/8; i++ {
				entry := binary.LittleEndian.Uint64(buf[i*8:])
				if entry&pagemapSwapped != 0 && entry&pagemapPresent == 0 {
					addr := page + i*pageSize
					if len(out) > 0 && out[len(out)-1].end == addr {
						out[len(out)-1].end = addr + pageSize
					} else {
						out = append(out, vmRange{addr, addr + pageSize})
					}
				}
			}
			if read == 0 {
				break
			}
			page += uint64(read) / 8 * pageSize
		}
	}
	return out, sc.Err()
}

// CgroupReclaim asks the kernel to reclaim up to `bytes` from the cgroup
// at cgroupPath (a path under /sys/fs/cgroup), via memory.reclaim (cgroup
// v2, Linux 5.19 or later). Reclaiming less than asked is not an error.
func CgroupReclaim(cgroupPath string, bytes int64) error {
	if bytes <= 0 {
		return nil
	}
	// Raw syscalls, not os.File: Go would register the kernfs descriptor with its
	// netpoller, and when the kernel answers EAGAIN (it could not reclaim that much)
	// the write would park for a writability edge that never comes, for good.
	fd, err := unix.Open(cgroupPath+"/memory.reclaim", unix.O_WRONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return &os.PathError{Op: "open", Path: cgroupPath + "/memory.reclaim", Err: err}
	}
	defer unix.Close(fd)
	// A long reclaim can be interrupted by a signal (Go's preemption SIGURG among
	// them): try again; a partial reclaim (EAGAIN) is fine.
	for try := 0; ; try++ {
		_, err := unix.Write(fd, []byte(strconv.FormatInt(bytes, 10)))
		if err == nil || errors.Is(err, unix.EAGAIN) {
			return nil
		}
		if errors.Is(err, unix.EINTR) && try < 10 {
			continue
		}
		return fmt.Errorf("memory.reclaim: %w", err)
	}
}

// CgroupOf returns the cgroup v2 path of a process, as a path under
// /sys/fs/cgroup.
func CgroupOf(pid int) (string, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if strings.HasPrefix(line, "0::") {
			return "/sys/fs/cgroup" + strings.TrimPrefix(line, "0::"), nil
		}
	}
	return "", fmt.Errorf("pid %d: no cgroup v2 entry", pid)
}
