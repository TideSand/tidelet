package criuimg

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

// CRIU image files: two magic words, then entries, each a 32-bit length and a protobuf
// message. The messages are read and written field by field, so this package needs none
// of CRIU's generated code and leaves the fields it does not know untouched.
const (
	imgCommonMagic  = 0x54564319
	imgServiceMagic = 0x55105940
	pagemapMagic    = 0x56084025
	statsMagic      = 0x57093306
	mmMagic         = 0x57492820
	filesMagic      = 0x56303138
	mntsMagic       = 0x55563928
)

// Protobuf wire types.
const (
	wireVarint = 0
	wireFixed8 = 1
	wireBytes  = 2
	wireFixed4 = 5
)

// field is one protobuf field: Val for the numeric wire types, Bytes for length-delimited ones.
type field struct {
	Num   int
	Wire  int
	Val   uint64
	Bytes []byte
}

func uvarint(buf []byte, pos int) (uint64, int, error) {
	v, n := binary.Uvarint(buf[pos:])
	if n <= 0 {
		return 0, 0, errors.New("criuimg: bad varint")
	}
	return v, pos + n, nil
}

func parse(buf []byte) ([]field, error) {
	var out []field
	for pos := 0; pos < len(buf); {
		tag, p, err := uvarint(buf, pos)
		if err != nil {
			return nil, err
		}
		pos = p
		f := field{Num: int(tag >> 3), Wire: int(tag & 7)}
		switch f.Wire {
		case wireVarint:
			if f.Val, pos, err = uvarint(buf, pos); err != nil {
				return nil, err
			}
		case wireFixed8:
			if pos+8 > len(buf) {
				return nil, errors.New("criuimg: short fixed64")
			}
			f.Val = binary.LittleEndian.Uint64(buf[pos:])
			pos += 8
		case wireBytes:
			n, p, err := uvarint(buf, pos)
			if err != nil || p+int(n) > len(buf) {
				return nil, errors.New("criuimg: short bytes field")
			}
			f.Bytes = buf[p : p+int(n)]
			pos = p + int(n)
		case wireFixed4:
			if pos+4 > len(buf) {
				return nil, errors.New("criuimg: short fixed32")
			}
			f.Val = uint64(binary.LittleEndian.Uint32(buf[pos:]))
			pos += 4
		default:
			return nil, fmt.Errorf("criuimg: unsupported wire type %d", f.Wire)
		}
		out = append(out, f)
	}
	return out, nil
}

func encode(fields []field) []byte {
	var out []byte
	for _, f := range fields {
		out = binary.AppendUvarint(out, uint64(f.Num)<<3|uint64(f.Wire))
		switch f.Wire {
		case wireVarint:
			out = binary.AppendUvarint(out, f.Val)
		case wireFixed8:
			out = binary.LittleEndian.AppendUint64(out, f.Val)
		case wireBytes:
			out = binary.AppendUvarint(out, uint64(len(f.Bytes)))
			out = append(out, f.Bytes...)
		case wireFixed4:
			out = binary.LittleEndian.AppendUint32(out, uint32(f.Val))
		}
	}
	return out
}

// msg is a parsed message with last-one-wins lookup by field number.
type msg map[int]field

func decode(buf []byte) (msg, error) {
	fs, err := parse(buf)
	if err != nil {
		return nil, err
	}
	m := make(msg, len(fs))
	for _, f := range fs {
		m[f.Num] = f
	}
	return m, nil
}

func (m msg) num(n int) uint64 { return m[n].Val }

func (m msg) has(n int) bool { _, ok := m[n]; return ok }

func vint(num int, v uint64) field   { return field{Num: num, Wire: wireVarint, Val: v} }
func vbytes(num int, b []byte) field { return field{Num: num, Wire: wireBytes, Bytes: b} }

func zigzag(v int64) uint64 { return uint64(v<<1) ^ uint64(v>>63) }

func readImage(path string, magic uint32, head uint32) ([][]byte, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(buf) < 8 || binary.LittleEndian.Uint32(buf) != head || binary.LittleEndian.Uint32(buf[4:]) != magic {
		return nil, fmt.Errorf("criuimg: %s: bad magic", path)
	}
	var entries [][]byte
	for pos := 8; pos < len(buf); {
		if pos+4 > len(buf) {
			return nil, fmt.Errorf("criuimg: %s: truncated", path)
		}
		n := int(binary.LittleEndian.Uint32(buf[pos:]))
		pos += 4
		if pos+n > len(buf) {
			return nil, fmt.Errorf("criuimg: %s: truncated entry", path)
		}
		entries = append(entries, buf[pos:pos+n])
		pos += n
	}
	return entries, nil
}

func writeImage(path string, magic uint32, entries [][]byte) error {
	out := binary.LittleEndian.AppendUint32(nil, imgCommonMagic)
	out = binary.LittleEndian.AppendUint32(out, magic)
	for _, e := range entries {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(e)))
		out = append(out, e...)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
