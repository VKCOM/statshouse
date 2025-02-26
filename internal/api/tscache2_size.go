package api

import "unsafe"

var sizeofCache2Chunk = int(unsafe.Sizeof(int(0)) + unsafe.Sizeof(cache2Chunk{}))
var sizeofCache2Shard = int(unsafe.Sizeof(cache2Shard{}))
var sizeofCache2Bucket = int(unsafe.Sizeof(cache2Bucket{}))
var sizeofCache2DataCol = int(unsafe.Sizeof([]tsSelectRow(nil)))
var sizeofCache2DataRow = int(unsafe.Sizeof(tsSelectRow{}))

func (c *cache2Chunk) size() int {
	return sizeofCache2Chunk + c.dataSize
}

func sizeofCache2Chunks(s []*cache2Chunk) int {
	var res int
	for i := 0; i < len(s); i++ {
		res += s[i].size()
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
