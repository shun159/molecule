package kvs

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

// A record not whole at the end of the last segment is a write cut short,
// by a crash, or damage to a write durable before: told apart by the
// records after it, whole, if any. A write is durable once synced, and
// the records written after claim it, see durableClaim; one a record
// after claims is damage, one none claims is cut short. The writes of a
// batch synced, with none after them yet, or of a table with SyncNone,
// are claimed by none: damage to them is taken for a write cut short.

const (
	// minRecordLen is the length of the least record: an op, a key of no
	// bytes, no value, and its seq, after its header.
	minRecordLen = headerLen + 2 + seqLen
	// minClaimLen is that of the least claiming: with its claim.
	minClaimLen = minRecordLen + claimLen
)

// claimant returns the seq of a record whole of b, the bytes after the
// record of seq torn, that claims torn durable; false if none.
//
// It looks for one at each offset of b, where the record cut short or
// damaged may have left any bytes: zeros, the start of a record whose end
// is lost, or records after it. A crc is checked only of a record that
// would claim torn: its op claiming, its length in b, and its seq after
// torn, no more after it than b, beginning by the record of torn, holds
// records.
func claimant(b []byte, torn uint64) (uint64, bool) {
	most := torn + uint64(len(b)/minRecordLen)
	for at := 0; at+minClaimLen <= len(b); at++ {
		if seq, ok := claimAt(b[at:], torn, most); ok {
			return seq, true
		}
	}
	return 0, false
}

// claimAt reports whether b begins with a record whole of a seq after
// torn, up to most, claiming torn durable, and returns its seq.
func claimAt(b []byte, torn, most uint64) (uint64, bool) {
	n := int(binary.LittleEndian.Uint32(b))
	if op := b[headerLen]; op != opPutClaim && op != opDelClaim {
		return 0, false
	}
	if n < minClaimLen-headerLen || n > len(b)-headerLen {
		return 0, false
	}
	rec := b[headerLen : headerLen+n]
	seq := binary.LittleEndian.Uint64(rec[n-seqLen:])
	claim := uint64(binary.LittleEndian.Uint16(rec[n-seqLen-claimLen:]))
	claims := seq > torn && seq <= most && claim > 0 && claim <= seq-torn // seq-claim >= torn
	if !claims || crc32.Checksum(rec, castagnoli) != binary.LittleEndian.Uint32(b[4:]) {
		return 0, false
	}
	return seq, true
}

// readAfter returns the bytes of the file at path after off.
func readAfter(path string, off int64) ([]byte, error) {
	f, err := openRead(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := io.CopyN(io.Discard, f, off); err != nil {
		return nil, err
	}
	return io.ReadAll(f)
}
