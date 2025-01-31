// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const (
	magicGoodBucket    = 0x59b907EC // change if format of seconds is changed
	magicDeletedBucket = 0x000007EC
	headerSize         = 20 // magic, time, chunk_size, crc32
	fileRotateSize     = 50 << 20
	maxChunkSize       = fileRotateSize - headerSize // Protect against very large allocations
	fileRotateInterval = 3600 * time.Second
	dtFormat           = "20060102_150405.000000000" // string representations must be ordered
)

type DiskBucketStorage struct {
	lockFile *os.File

	shards []*diskCacheShard
}

type diskCacheFile struct {
	fp      *os.File
	name    string
	nextPos int64 // We read next tail bucket from this position
	size    int64

	refCount int // Once for readingFileTail, writingFile in diskCacheShard and once for each bucket in knownBuckets
}

type waitingFile struct {
	name string
	size int64
}

type diskCacheBucket struct {
	file *diskCacheFile
	pos  int64 // position of header
	time uint32
	size int // size of body
	crc  uint32
}

type diskCacheShard struct {
	Logf func(format string, args ...interface{})

	shardPath        string
	mu               sync.Mutex                 // shards are fully independent
	knownBuckets     map[int64]*diskCacheBucket // known are seconds written and not deleted after restart, and seconds popped from tail, This is arbitrary reference ID, not timestamp.
	knownBucketsSize int64
	lastBucketID     int64 // incremented after reading bucket from disk or writing a new bucket

	readingFileTail  *diskCacheFile // current tail file being processed
	waitingFilesTail []waitingFile  // when readingFileTail is finished, next file from here is taken.
	waitingFilesSize int64

	writingFile          *diskCacheFile // after restart, we begin a new file. Will be rotated periodically by size or time since created
	writingFileCreatedTs time.Time

	totalFileSize int64
}

// TODO - new format with xxhash of (body | timestamp) so body cannot be attributed to the wrong second

// After restart, we always start to write a new file, we never append
// buckets are written to the file as chunks (crc32 is of body only)
// [MAGIC] [timestamp] [body_len] [crc32] ...body

// Each file has name by creation time. During reading files are sorted by name.
// Files are rotated when they are around for a long time or their size grows too much

// When bucket is not needed any more, we write different magic to erase it (we only need body_len to skip erased bucket)
// [DELETE] [xxxxxxxxx] [body_len] [xxxxx] ...xxxx

// When all buckets in a file not needed, file is deleted

// We use hardware-optimized polynomial for max speed
var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

func MakeDiskBucketStorage(dirPath string, numShards int, logf func(format string, args ...interface{})) (*DiskBucketStorage, error) {
	var err error
	filePath := filepath.Join(dirPath, "run.lock")
	d := &DiskBucketStorage{}
	d.lockFile, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("could not create lock file %q: %v", filePath, err)
	}
	if err := syscall.Flock(int(d.lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = d.lockFile.Close()
		return nil, fmt.Errorf("another instance of statshouse is already running in %q (%v)", dirPath, err)
	}
	for i := 0; i < numShards; i++ {
		sh, err := makeDiscCacheShard(filepath.Join(dirPath, strconv.Itoa(i)), logf)
		if err != nil {
			_ = d.lockFile.Close()
			return nil, err
		}
		d.shards = append(d.shards, sh)
	}
	return d, nil
}

func (d *DiskBucketStorage) Close() error {
	for _, s := range d.shards {
		s.Close()
	}
	return d.lockFile.Close()
}

func (d *DiskBucketStorage) TotalFileSize(shardID int) (total int64, unsent int64) {
	return d.shards[shardID].TotalFileSize()
}

// id 0 is returned when there is no more unknown buckets on disk
func (d *DiskBucketStorage) ReadNextTailBucket(shardID int) (time uint32, id int64) {
	return d.shards[shardID].ReadNextTailSecond()
}

func (d *DiskBucketStorage) GetBucket(shardID int, id int64, time uint32, scratchPad *[]byte) ([]byte, error) {
	return d.shards[shardID].GetBucket(id, time, scratchPad)
}

func (d *DiskBucketStorage) PutBucket(shardID int, time uint32, data []byte) (id int64, _ error) {
	return d.shards[shardID].PutBucket(time, data)
}

func (d *DiskBucketStorage) EraseBucket(shardID int, id int64) error {
	return d.shards[shardID].EraseBucket(id)
}

func makeDiscCacheShard(shardPath string, logf func(format string, args ...interface{})) (*diskCacheShard, error) {
	if err := os.MkdirAll(shardPath, os.ModePerm); err != nil { // no error if exists
		return nil, fmt.Errorf("could not create folder for shard %q: %v", shardPath, err)
	}
	dis, err := os.ReadDir(shardPath)
	if err != nil {
		logf("warning - could not read folder %q: %v", shardPath, err)
		return nil, fmt.Errorf("could not create folder for shard %q: %v", shardPath, err)
	}
	d := &diskCacheShard{shardPath: shardPath, Logf: logf, knownBuckets: map[int64]*diskCacheBucket{}}

	// Scan all files, skipping folders (legacy code stored separate seconds in folders)
	for _, di := range dis {
		dn := filepath.Join(d.shardPath, di.Name())
		if di.IsDir() { // strange but ok
			logf("warning - dir %q found in bucket disc cache", dn)
			continue
		}
		st, err := os.Stat(dn) // We need size for size accounting
		if err != nil {
			logf("warning - could not stat %q: %v", dn, err)
			continue
		}
		d.waitingFilesTail = append(d.waitingFilesTail, waitingFile{name: dn, size: st.Size()})
		d.waitingFilesSize += st.Size()
		d.totalFileSize += st.Size()
	}

	sort.Slice(d.waitingFilesTail, func(i, j int) bool {
		return d.waitingFilesTail[i].name < d.waitingFilesTail[j].name
	})

	return d, nil
}

func (d *diskCacheShard) Close() {
	for k, sec := range d.knownBuckets {
		d.unrefFileWithRemove(&sec.file, false)
		d.knownBucketsSize -= int64(sec.size) + headerSize
		delete(d.knownBuckets, k)
	}
	if d.readingFileTail != nil {
		d.unrefFileWithRemove(&d.readingFileTail, false)
	}
	if d.writingFile != nil {
		d.unrefFileWithRemove(&d.writingFile, false)
	}
}

func (d *diskCacheShard) TotalFileSize() (total int64, unsent int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	unsent = d.knownBucketsSize + d.waitingFilesSize
	if d.readingFileTail != nil && d.readingFileTail.nextPos < d.readingFileTail.size { // second must be always true
		unsent += d.readingFileTail.size - d.readingFileTail.nextPos
	}
	if unsent > d.totalFileSize { // must be never
		unsent = d.totalFileSize
	}
	return d.totalFileSize, unsent
}

func (d *diskCacheShard) unrefFile(fp **diskCacheFile) { // sets to nil to prevent usage
	d.unrefFileWithRemove(fp, true)
}

func (d *diskCacheShard) unrefFileWithRemove(fp **diskCacheFile, remove bool) { // sets to nil to prevent usage
	f := *fp
	f.refCount--
	if f.refCount < 0 {
		d.Logf("warning - ref count invariant violated with value %d for file %q in folder %q", f.refCount, f.name, d.shardPath)
	}
	if f.refCount == 0 {
		if err := f.fp.Close(); err != nil {
			d.Logf("warning - error closing file %q in folder %q: %v", f.name, d.shardPath, err)
		}
		d.totalFileSize -= f.size
		if remove {
			if err := os.Remove(f.name); err != nil {
				d.Logf("warning - error removing tail file %q in folder %q: %v", f.name, d.shardPath, err)
			}
		}
	}
	*fp = nil
}

func (d *diskCacheShard) writeSecond(second uint32, crc uint32, data []byte) error {
	if len(data) > maxChunkSize {
		return fmt.Errorf("chunk too big (%d) must be <= %d", len(data), maxChunkSize)
	}
	now := time.Now()
	if d.writingFile != nil && (d.writingFile.size+headerSize+int64(len(data)) > fileRotateSize || now.Sub(d.writingFileCreatedTs) >= fileRotateInterval) {
		d.unrefFile(&d.writingFile)
	}
	if d.writingFile == nil {
		// we do not want to deal with collisions, so simply include nanoseconds (and pray).
		// we prefer local format because it is easy to admin
		dt := now.Format(dtFormat)

		filename := filepath.Join(d.shardPath, fmt.Sprintf("%s.seconds", dt))
		fp, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0666)
		if err != nil {
			d.Logf("warning - error creating tail file %q in folder %q: %v", filename, d.shardPath, err)
			return err
		}
		d.writingFile = &diskCacheFile{fp: fp, name: filename, refCount: 1}
		d.writingFileCreatedTs = now
	}
	var header [headerSize]byte
	binary.LittleEndian.PutUint32(header[0:4], magicGoodBucket)
	binary.LittleEndian.PutUint32(header[4:8], second)
	binary.LittleEndian.PutUint64(header[8:16], uint64(len(data)))
	binary.LittleEndian.PutUint32(header[16:20], crc)
	if _, err := d.writingFile.fp.WriteAt(header[:], d.writingFile.size); err != nil {
		d.Logf("warning - error writing tail file header %q in folder %q: %v", d.writingFile.name, d.shardPath, err)
		d.unrefFile(&d.writingFile)
		return err
	}

	if _, err := d.writingFile.fp.WriteAt(data, d.writingFile.size+headerSize); err != nil {
		d.Logf("warning - error writing tail file %q in folder %q: %v", d.writingFile.name, d.shardPath, err)
		d.unrefFile(&d.writingFile)
		return err
	}
	return nil
}

func (d *diskCacheShard) ReadNextTailSecond() (time uint32, id int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for {
		for d.readingFileTail == nil {
			if len(d.waitingFilesTail) == 0 {
				return 0, 0
			}
			tailFile := d.waitingFilesTail[0]
			d.waitingFilesSize -= tailFile.size
			d.waitingFilesTail = d.waitingFilesTail[1:]
			fp, err := os.OpenFile(tailFile.name, os.O_RDWR, os.ModePerm)
			if err != nil {
				d.totalFileSize -= tailFile.size
				d.Logf("warning - error opening tail file %q in folder %q: %v", tailFile.name, d.shardPath, err)
				continue
			}
			d.readingFileTail = &diskCacheFile{fp: fp, name: tailFile.name, size: tailFile.size, refCount: 1}
			break
		}
		if d.readingFileTail.nextPos >= d.readingFileTail.size {
			d.unrefFile(&d.readingFileTail)
			continue
		}
		_, err := d.readingFileTail.fp.Seek(d.readingFileTail.nextPos, io.SeekStart)
		if err != nil {
			d.Logf("warning - error seeking tail file %q in folder %q: %v", d.readingFileTail.name, d.shardPath, err)
			d.unrefFile(&d.readingFileTail)
			continue
		}
		var header [headerSize]byte
		_, err = io.ReadFull(d.readingFileTail.fp, header[:])
		if err != nil {
			d.Logf("warning - error reading tail file %q in folder %q: %v", d.readingFileTail.name, d.shardPath, err)
			d.unrefFile(&d.readingFileTail)
			continue
		}
		magic := binary.LittleEndian.Uint32(header[0:4])
		time = binary.LittleEndian.Uint32(header[4:8])
		chunkSize := int64(binary.LittleEndian.Uint64(header[8:16]))
		crc := binary.LittleEndian.Uint32(header[16:20])
		if chunkSize < 0 || chunkSize > maxChunkSize || d.readingFileTail.nextPos+headerSize+chunkSize > d.readingFileTail.size {
			d.Logf("warning - wrong chunk size %d in tail file %q in folder %q", chunkSize, d.readingFileTail.name, d.shardPath)
			d.unrefFile(&d.readingFileTail)
			continue
		}
		if magic == magicDeletedBucket {
			d.readingFileTail.nextPos += headerSize + chunkSize
			continue
		}
		if magic != magicGoodBucket {
			d.Logf("warning - unknown magic %x in tail file %q in folder %q", magic, d.readingFileTail.name, d.shardPath)
			d.unrefFile(&d.readingFileTail) // If magic is corrupted, everything after it should not be trusted
			continue
		}
		d.lastBucketID++

		if _, ok := d.knownBuckets[d.lastBucketID]; ok { // overwriting known bucket will destroy refCounts
			panic(fmt.Errorf("invariant violation, duplicate bucket id %d for time %d in tail file %q in folder %q", d.lastBucketID, time, d.readingFileTail.name, d.shardPath))
		}
		d.knownBuckets[d.lastBucketID] = &diskCacheBucket{
			file: d.readingFileTail,
			pos:  d.readingFileTail.nextPos,
			time: time,
			crc:  crc,
			size: int(chunkSize), // safe because check above
		}
		d.knownBucketsSize += chunkSize + headerSize
		d.readingFileTail.refCount++
		d.readingFileTail.nextPos += headerSize + chunkSize
		return time, d.lastBucketID
	}
}

func (d *diskCacheShard) PutBucket(time uint32, data []byte) (id int64, _ error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	// we allow putting seconds with the same time, we simply assign different ids to them
	crc := crc32.Checksum(data, castagnoliTable)

	if err := d.writeSecond(time, crc, data); err != nil {
		return 0, err
	}
	sec := &diskCacheBucket{
		file: d.writingFile,
		pos:  d.writingFile.size,
		time: time,
		size: len(data),
		crc:  crc,
	}
	d.writingFile.refCount++
	d.writingFile.size += headerSize + int64(len(data))
	d.totalFileSize += headerSize + int64(len(data))
	d.knownBucketsSize += int64(sec.size) + headerSize
	d.lastBucketID++
	d.knownBuckets[d.lastBucketID] = sec
	return d.lastBucketID, nil
}

// reads into scratchPad and returns slice of scratchPad
func (d *diskCacheShard) GetBucket(id int64, time uint32, scratchPad *[]byte) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	sec, ok := d.knownBuckets[id]
	if !ok {
		return nil, fmt.Errorf("GetBucket bucket with id %d not known in shard %q", id, d.shardPath)
	}
	if sec.time != time {
		return nil, fmt.Errorf("GetBucket bucket with id %d has wrong second (%d, requested %d) in shard %q", id, sec.time, time, d.shardPath)
	}
	if cap(*scratchPad) >= sec.size {
		*scratchPad = (*scratchPad)[0:sec.size]
	} else {
		*scratchPad = make([]byte, sec.size)
	}
	if _, err := sec.file.fp.ReadAt(*scratchPad, sec.pos+headerSize); err != nil {
		_ = d.eraseBucket(id)
		return nil, fmt.Errorf("GetBucket failed read at %d for id %d second %d in shard %q: %v", sec.pos, id, sec.time, d.shardPath, err)
	}
	crc := crc32.Checksum(*scratchPad, castagnoliTable)
	if crc != sec.crc {
		_ = d.eraseBucket(id)
		return nil, fmt.Errorf("GetBucket wrong crc for id %d second %d in shard %q", id, sec.time, d.shardPath)
	}
	return *scratchPad, nil
}

func (d *diskCacheShard) eraseBucket(id int64) error {
	sec, ok := d.knownBuckets[id]
	if !ok {
		return nil // erasing second not saved is NOP, not error
	}
	var header [4]byte
	binary.LittleEndian.PutUint32(header[0:4], magicDeletedBucket)
	_, err := sec.file.fp.WriteAt(header[:], sec.pos)
	d.unrefFile(&sec.file)
	d.knownBucketsSize -= int64(sec.size) + headerSize
	delete(d.knownBuckets, id)
	if err != nil {
		return fmt.Errorf("EraseBucket id %d second %d failed in shard %q: %v", id, sec.time, d.shardPath, err)
	}
	return nil
}

func (d *diskCacheShard) EraseBucket(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.eraseBucket(id)
}
