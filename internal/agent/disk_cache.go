// Copyright 2022 V Kontakte LLC
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
	magicGoodSecond    = 0x59b907EC // change if format of seconds is changed
	magicDeletedSecond = 0x000007EC
	headerSize         = 20 // magic, second, chunk_size, crc32
	fileRotateSize     = 50 << 20
	maxChunkSize       = fileRotateSize // Protect against very large allocations
	fileRotateInterval = 3600 * time.Second
	dtFormat           = "20060102_150405" // string representation must be ordered
)

type DiskBucketStorage struct {
	lockFile *os.File

	shards []*diskCacheShard
}

type diskCacheFile struct {
	fp      *os.File
	name    string
	nextPos int64 // We read next tail second from this position
	size    int64

	refCount int // Once for readingFileTail, writingFile in diskCacheShard and once for each second in knownSeconds
}

type waitingFile struct {
	name string
	size int64
}

type diskCacheBucket struct {
	file *diskCacheFile
	pos  int64 // position of header
	size int   // size of body
	crc  uint32
}

type diskCacheShard struct {
	Logf func(format string, args ...interface{})

	shardPath    string
	mu           sync.Mutex                  // shards are fully independent
	knownSeconds map[uint32]*diskCacheBucket // known are seconds written and not deleted after restart, and seconds popped from tail

	readingFileTail  *diskCacheFile // current tail file being processed
	waitingFilesTail []waitingFile  // when readingFileTail is finished, next file from here is taken.

	writingFile          *diskCacheFile // after restart we begin a new file. Will be rotated periodically by size or seconds diff
	writingFileCreatedTs time.Time

	totalFileSize int64
}

// After restart we always start to write a new file, we never append
// Seconds are written to the file as chunks
// [MAGIC] [timestamp] [body_len] [crc32] ...body

// Each file has name of the first second it stores. All seconds in the file must be >= that seconds
// Files are rotated when they contain too diverse seconds or their size grows too much

// When second is not needed any more, we write different magic to erase it

// When all seconds in a file not needed, file is deleted

// We use hardware-optimized polynomial for max speed
// workaround for https://github.com/golang/go/issues/41911, before we use Go 1.16 everywhere
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

func (d *DiskBucketStorage) TotalFileSize(shardID int) int64 {
	return d.shards[shardID].TotalFileSize()
}

func (d *DiskBucketStorage) ReadNextTailSecond(shardID int) (uint32, bool) {
	return d.shards[shardID].ReadNextTailSecond()
}

func (d *DiskBucketStorage) GetBucket(shardID int, time uint32, scratchPad *[]byte) ([]byte, error) {
	return d.shards[shardID].GetBucket(time, scratchPad)
}

func (d *DiskBucketStorage) PutBucket(shardID int, time uint32, data []byte) error {
	return d.shards[shardID].PutBucket(time, data)
}

func (d *DiskBucketStorage) EraseBucket(shardID int, time uint32) error {
	return d.shards[shardID].EraseBucket(time)
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
	d := &diskCacheShard{shardPath: shardPath, Logf: logf, knownSeconds: map[uint32]*diskCacheBucket{}}

	// Scan all files, deleting legacy separate seconds and their folders
	for _, di := range dis {
		dn := filepath.Join(d.shardPath, di.Name())
		if di.IsDir() { // strange but ok
			logf("warning - dir %q found in bucket disc cache", dn)
			continue
		}
		// New format - small number of large files
		st, err := os.Stat(dn) // We need size for size accounting
		if err != nil {
			logf("warning - could not stat %q: %v", dn, err)
			continue
		}
		d.waitingFilesTail = append(d.waitingFilesTail, waitingFile{name: dn, size: st.Size()})
		d.totalFileSize += st.Size()
	}

	sort.Slice(d.waitingFilesTail, func(i, j int) bool {
		return d.waitingFilesTail[i].name < d.waitingFilesTail[j].name
	})

	return d, nil
}

func (d *diskCacheShard) Close() {
	for k, sec := range d.knownSeconds {
		d.unrefFileWithRemove(&sec.file, false)
		delete(d.knownSeconds, k)
	}
	if d.readingFileTail != nil {
		d.unrefFileWithRemove(&d.readingFileTail, false)
	}
	if d.writingFile != nil {
		d.unrefFileWithRemove(&d.writingFile, false)
	}
}

func (d *diskCacheShard) TotalFileSize() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.totalFileSize
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
	now := time.Now()
	if d.writingFile != nil && (d.writingFile.size+headerSize+int64(len(data)) > fileRotateSize || now.Sub(d.writingFileCreatedTs) >= fileRotateInterval) {
		d.unrefFile(&d.writingFile)
	}
	if d.writingFile == nil {
		dt := time.Unix(int64(second), 0).Format(dtFormat)

		filename := filepath.Join(d.shardPath, fmt.Sprintf("%s.seconds", dt))
		fp, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0666)
		if err != nil {
			// we presume, seconds are unique and ordered
			d.Logf("warning - error creating tail file %q in folder %q: %v", filename, d.shardPath, err)
			return err
		}
		d.writingFile = &diskCacheFile{fp: fp, name: filename, refCount: 1}
		d.writingFileCreatedTs = now
	}
	var header [headerSize]byte
	binary.LittleEndian.PutUint32(header[0:4], magicGoodSecond)
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

func (d *diskCacheShard) ReadNextTailSecond() (uint32, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for {
		for d.readingFileTail == nil {
			if len(d.waitingFilesTail) == 0 {
				return 0, false
			}
			tailFile := d.waitingFilesTail[0]
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
		if d.readingFileTail.nextPos == d.readingFileTail.size {
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
		second := binary.LittleEndian.Uint32(header[4:8])
		chunkSize := int64(binary.LittleEndian.Uint64(header[8:16]))
		crc := binary.LittleEndian.Uint32(header[16:20])
		if chunkSize < 0 || chunkSize > maxChunkSize || d.readingFileTail.nextPos+headerSize+chunkSize > d.readingFileTail.size {
			d.Logf("warning - wrong chunk size %d in tail file %q in folder %q", chunkSize, d.readingFileTail.name, d.shardPath)
			d.unrefFile(&d.readingFileTail)
			continue
		}
		if magic == magicDeletedSecond {
			d.readingFileTail.nextPos += headerSize + chunkSize
			continue
		}
		if magic != magicGoodSecond {
			d.Logf("warning - unknown magic %x in tail file %q in folder %q", magic, d.readingFileTail.name, d.shardPath)
			d.unrefFile(&d.readingFileTail) // If magic is corrupted, everything after it should not be trusted
			continue
		}
		_, ok := d.knownSeconds[second]
		if ok { // overwriting will destroy refCounts
			d.Logf("warning - skipping duplicate second %d in tail file %q in folder %q", second, d.readingFileTail.name, d.shardPath)
			continue
		}
		d.knownSeconds[second] = &diskCacheBucket{
			file: d.readingFileTail,
			pos:  d.readingFileTail.nextPos,
			crc:  crc,
			size: int(chunkSize), // safe because check above
		}
		d.readingFileTail.refCount++
		d.readingFileTail.nextPos += headerSize + chunkSize
		return second, true
	}
}

func (d *diskCacheShard) PutBucket(time uint32, data []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.knownSeconds[time]; ok {
		return fmt.Errorf("PutBucket Second %d already exists in shard %q", time, d.shardPath)
	}
	crc := crc32.Checksum(data, castagnoliTable)

	if err := d.writeSecond(time, crc, data); err != nil {
		return err
	}
	sec := &diskCacheBucket{
		file: d.writingFile,
		pos:  d.writingFile.size,
		size: len(data),
		crc:  crc,
	}
	d.writingFile.refCount++
	d.writingFile.size += headerSize + int64(len(data))
	d.totalFileSize += headerSize + int64(len(data))
	d.knownSeconds[time] = sec
	return nil
}

// reads into scratchPad and returns slice of scratchPad
func (d *diskCacheShard) GetBucket(time uint32, scratchPad *[]byte) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	sec, ok := d.knownSeconds[time]
	if !ok {
		return nil, fmt.Errorf("GetBucket Second %d not known in shard %q", time, d.shardPath)
	}
	if cap(*scratchPad) >= sec.size {
		*scratchPad = (*scratchPad)[0:sec.size]
	} else {
		*scratchPad = make([]byte, sec.size)
	}
	if _, err := sec.file.fp.ReadAt(*scratchPad, sec.pos+headerSize); err != nil {
		_ = d.eraseBucket(time)
		return nil, fmt.Errorf("GetBucket failed read at %d for second %d in shard %q: %v", sec.pos, time, d.shardPath, err)
	}
	crc := crc32.Checksum(*scratchPad, castagnoliTable)
	if crc != sec.crc {
		_ = d.eraseBucket(time)
		return nil, fmt.Errorf("GetBucket wrong crc for second %d in shard %q", time, d.shardPath)
	}
	return *scratchPad, nil
}

func (d *diskCacheShard) eraseBucket(time uint32) error {
	sec, ok := d.knownSeconds[time]
	if !ok {
		return nil // erasing second not saved is NOP, not error
	}
	var header [4]byte
	binary.LittleEndian.PutUint32(header[0:4], magicDeletedSecond)
	_, err := sec.file.fp.WriteAt(header[:], sec.pos)
	d.unrefFile(&sec.file)
	delete(d.knownSeconds, time)
	if err != nil {
		return fmt.Errorf("EraseBucket second %d failed in shard %q: %v", time, d.shardPath, err)
	}
	return nil
}

func (d *diskCacheShard) EraseBucket(time uint32) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.eraseBucket(time)
}
