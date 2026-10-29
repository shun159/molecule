package kvs

import "math"

// durableClaim is the claim a record of seq makes, written: of the writes
// before it, those durable then, on disk, as their last seq is durable.
// Opening, a record damaged is told from one cut short by a power loss by
// the claims after it: one a record after it claims durable was on disk,
// whole, before; one none claims may have been cut short.
//
// The claim is seq less durable: a claim of more than a uint16 holds is
// none, 0. It is none as well with synced false, the log never synced, as
// with SyncNone, where a write is durable in the page cache only.
func durableClaim(seq, durable uint64, synced bool) uint16 {
	lag := seq - durable // durable is before seq, written now
	if !synced || lag > math.MaxUint16 {
		return 0
	}
	return uint16(lag)
}
