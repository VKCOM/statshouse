package api

import (
	"unsafe"

	"github.com/hrissan/tdigest"

	"github.com/VKCOM/statshouse/internal/format"
)

var sizeofCache2DataCol = int(unsafe.Sizeof([]tsSelectRow(nil)))
var sizeofCache2DataRow = int(unsafe.Sizeof(tsSelectRow{}))

func sizeofCache2Chunks(s []*cache2Chunk) int {
	var res int
	for i := 0; i < len(s); i++ {
		s[i].mu.Lock()
		res += s[i].size
		s[i].mu.Unlock()
	}
	return res
}

func sizeofCache2Data(s cache2Data) int {
	var res int
	if cap(s) > len(s) {
		res += (cap(s) - len(s)) * sizeofCache2DataCol
	}
	for i := 0; i < len(s); i++ {
		res += sizeofCache2DataCol
		col := s[i]
		if cap(col) > len(col) {
			res += (cap(col) - len(col)) * sizeofCache2DataRow
		}
		for j := 0; j < len(col); j++ {
			res += sizeofCache2Row(&col[j])
		}
	}
	return res
}

func sizeofCache2Row(r *tsSelectRow) int {
	n := sizeofCache2DataRow
	for i := 0; i < format.MaxTags; i++ {
		n += len(r.stag[i])
	}
	n += r.unique.HeapAllocBytes()
	if r.percentile != nil {
		n += int(unsafe.Sizeof(*r.percentile))
		n += tdigest.ByteSizeForCompression(r.percentile.Compression)
	}
	n += len(r.minHostStr.AsString) + len(r.maxHostStr.AsString)
	return n
}
