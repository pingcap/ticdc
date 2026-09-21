// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
)

// DecodeChunk first verifies the entire immutable object before emitting rows.
// The local artifact backend doubles as a bounded spool (64 MiB per object).
func DecodeChunk(ref Chunk, expected Header, span Span, emit func([]byte, []byte) error) (Footer, error) {
	var zero Footer
	if ref.Size < 0 || ref.Size > 64<<20 {
		return zero, Invalid("chunk exceeds demo size limit")
	}
	info, err := os.Stat(ref.URI)
	if err != nil {
		return zero, Wrap(err)
	}
	if info.Size() != ref.Size {
		return zero, Invalid("chunk size mismatch")
	}
	actual, err := Ref(ref.URI)
	if err != nil {
		return zero, err
	}
	if actual != ref.ObjectRef {
		return zero, Invalid("chunk checksum/size mismatch")
	}
	f, err := os.Open(ref.URI)
	if err != nil {
		return zero, Wrap(err)
	}
	defer f.Close()
	return decode(f, expected, span, ref, emit)
}

func readN(r io.Reader, n uint32, max uint32) ([]byte, error) {
	if n > max {
		return nil, Invalid("frame exceeds limit")
	}
	b := make([]byte, n)
	_, e := io.ReadFull(r, b)
	return b, Wrap(e)
}

func readU32(r io.Reader) (uint32, error) {
	var b [4]byte
	_, e := io.ReadFull(r, b[:])
	return binary.LittleEndian.Uint32(b[:]), Wrap(e)
}

func decode(r io.Reader, expected Header, span Span, ref Chunk, emit func([]byte, []byte) error) (Footer, error) {
	var seen Footer
	magic, e := readN(r, 8, 8)
	if e != nil {
		return seen, e
	}
	if string(magic) != "SNAPKV01" {
		return seen, Invalid("invalid chunk magic")
	}
	n, e := readU32(r)
	if e != nil {
		return seen, e
	}
	b, e := readN(r, n, 64<<10)
	if e != nil {
		return seen, e
	}
	var h Header
	if e = json.Unmarshal(b, &h); e != nil {
		return seen, Wrap(e)
	}
	if h != expected || h.Version != 1 || h.Compression != "none" {
		return seen, Invalid("chunk header mismatch or unsupported version")
	}
	for {
		n, e = readU32(r)
		if e != nil {
			return seen, e
		}
		count, e := readU32(r)
		if e != nil {
			return seen, e
		}
		if n == 0 {
			b, e = readN(r, count, 64<<10)
			if e != nil {
				return seen, e
			}
			var footer Footer
			if e = json.Unmarshal(b, &footer); e != nil {
				return seen, Wrap(e)
			}
			end, e := readN(r, 8, 8)
			if e != nil {
				return seen, e
			}
			if string(end) != "SNAPEND1" {
				return seen, Invalid("missing footer magic")
			}
			var tail [1]byte
			if n, err := r.Read(tail[:]); n != 0 || err != io.EOF {
				return seen, Invalid("trailing chunk bytes")
			}
			if footer.Blocks != seen.Blocks || footer.KVCount != seen.KVCount || footer.LogicalBytes != seen.LogicalBytes || !bytes.Equal(footer.First, seen.First) || !bytes.Equal(footer.Last, seen.Last) || footer.KVCount != ref.KVCount || footer.LogicalBytes != ref.LogicalBytes || !bytes.Equal(footer.First, ref.First) || !bytes.Equal(footer.Last, ref.Last) {
				return seen, Invalid("footer/statistics mismatch")
			}
			return footer, nil
		}
		if count == 0 || count > n/8 {
			return seen, Invalid("invalid block count")
		}
		b, e = readN(r, n, (16<<20)+8)
		if e != nil {
			return seen, e
		}
		block := bytes.NewReader(b)
		for i := uint32(0); i < count; i++ {
			kl, e := readU32(block)
			if e != nil {
				return seen, e
			}
			vl, e := readU32(block)
			if e != nil {
				return seen, e
			}
			if kl == 0 || uint64(kl)+uint64(vl) > 16<<20 {
				return seen, Invalid("record exceeds limit")
			}
			k, e := readN(block, kl, 16<<20)
			if e != nil {
				return seen, e
			}
			v, e := readN(block, vl, 16<<20)
			if e != nil {
				return seen, e
			}
			if bytes.Compare(k, span.Start) < 0 || bytes.Compare(k, span.End) >= 0 || seen.KVCount > 0 && bytes.Compare(seen.Last, k) >= 0 {
				return seen, Invalid("key bounds/order violation")
			}
			if seen.KVCount == 0 {
				seen.First = k
			}
			seen.Last = k
			seen.KVCount++
			seen.LogicalBytes += uint64(kl) + uint64(vl)
			if e = emit(k, v); e != nil {
				return seen, e
			}
		}
		if block.Len() != 0 {
			return seen, Invalid("block trailing bytes")
		}
		seen.Blocks++
	}
}
