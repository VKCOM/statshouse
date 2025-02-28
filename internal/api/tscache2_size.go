package api

import "unsafe"

var sizeofCache2DataCol = int(unsafe.Sizeof([]tsSelectRow(nil)))
var sizeofCache2DataRow = int(unsafe.Sizeof(tsSelectRow{}))

func sizeofCache2Chunks(s []*cache2Chunk) int {
	var res int
	for i := 0; i < len(s); i++ {
		res += s[i].size
	}
	return res
}

func sizeofCache2Data(s cache2Data) int {
	res := len(s) * sizeofCache2DataCol
	for i := 0; i < len(s); i++ {
		res += len(s[i]) * sizeofCache2DataRow
	}
	return res
}
