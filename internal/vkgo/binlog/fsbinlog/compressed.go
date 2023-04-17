// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fsbinlog

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/xi2/xz"

	"github.com/vkcom/statshouse/internal/vkgo/binlog"
)

type (
	compressAlgo byte
)

const (
	compressAlgoUnknown = compressAlgo(iota)
	compressAlgoZlib
	compressAlgoBz2
	compressAlgoXz
)

func (a compressAlgo) String() string {
	switch a {
	case compressAlgoZlib:
		return `zlib`
	case compressAlgoBz2:
		return `bz2`
	case compressAlgoXz:
		return `xz`
	default:
		return `?`
	}
}

type decompressor struct {
	src          io.ReadSeeker
	srcSize      uint64
	algo         compressAlgo
	chunkOffsets []uint64     // копия kfsBinlogZipHeader.ChunkOffset
	chunkIdx     int          // текущий элемент в chunkIdx
	chunkData    bytes.Buffer // один распакованный chunk

	xzReader *xz.Reader
}

var _ ReaderSync = &decompressor{}

func newDecompressor(src io.ReadSeeker, srcSize uint64, algo compressAlgo, chunkOffsets []uint64) *decompressor {
	return &decompressor{
		src:          src,
		srcSize:      srcSize,
		algo:         algo,
		chunkOffsets: chunkOffsets,
		chunkIdx:     -1,
	}
}

func (d *decompressor) Read(p []byte) (n int, err error) {
	if d.chunkData.Len() == 0 {
		if err := d.readNextChunk(); err != nil {
			return 0, err // произошла ошибка распаковки или EOF
		}
	}

	return d.chunkData.Read(p)
}

func (d *decompressor) readNextChunk() error {
	d.chunkIdx++
	if d.chunkIdx >= len(d.chunkOffsets) {
		return io.EOF
	}

	offset := d.chunkOffsets[d.chunkIdx]
	if _, err := d.src.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}

	nextChunkOffset := d.srcSize
	if d.chunkIdx < len(d.chunkOffsets)-1 {
		nextChunkOffset = d.chunkOffsets[d.chunkIdx+1]
	}
	size := nextChunkOffset - offset
	err := d.decompressChunk(io.LimitReader(d.src, int64(size)))
	if err != nil {
		return err
	}

	return nil
}

func (d *decompressor) decompressChunk(src io.Reader) error {
	switch d.algo {
	case compressAlgoZlib: // ~195MB/s
		return d.decompressChunkZlib(src)
	case compressAlgoXz: // ~76MB/s
		return d.decompressChunkXz(src)
	default:
		return fmt.Errorf(`unsupported algo: %d (%s)`, d.algo, d.algo)
	}
}

func (d *decompressor) decompressChunkXz(src io.Reader) error {
	if d.xzReader == nil {
		var err error
		d.xzReader, err = xz.NewReader(src, 0)
		if err != nil {
			return err
		}
	} else if err := d.xzReader.Reset(src); err != nil {
		return err
	}

	d.chunkData.Reset()

	if _, err := io.Copy(&d.chunkData, d.xzReader); err != nil {
		return err
	}

	return nil
}

func (d *decompressor) decompressChunkZlib(src io.Reader) error {
	zlibReader, err := zlib.NewReader(src)
	if err != nil {
		return err
	}

	d.chunkData.Reset()

	_, err = io.Copy(&d.chunkData, zlibReader)
	_ = zlibReader.Close()

	return err
}

func (d *decompressor) Sync() error {
	return nil
}

type kfsBinlogZipHeader struct {
	Magic        uint32   // 0x047a4c4b
	Format       int32    // 0 for gzip, 1 for bzip2, 2 for lzma (младшие 4 бита), level (4-7 биты), флаги (24-31 бит).
	OrigFileSize int64    // длина исходного (незапакованного) файла
	OrigFileMd5  [16]byte // md5 исходного файла
	First36Bytes [36]byte // первые 36 байтов исходного файла
	Last36Bytes  [36]byte // последние 36 байтов исходного файла
	First1mMd5   [16]byte // md5 от первого мегабайта исходного файла
	First128kMd5 [16]byte // md5 от первых 128k (нужно для репликации) или тег бинлога. Если тег присутствует установлен 24 бит поля format. Чтобы pack-binlog нашёл тег, нужно чтобы LEV_TAG был записан сразу после LEV_START.
	Last128kMd5  [16]byte // md5 от последних 128k (нужно для репликации)
	FileHash     int64    // md5 от первых 16k и последних 16k у которых последние 16 байт забито нулями (нужно если кусок начинается с LEV_START)
	ChunkOffset  []uint64 // смещение от начала архива i-ого куска бинлога
	Crc32        uint32   // crc32 всех предшествующих байтов заголовка (Magic .. ChunkOffset) (@see kfs_bz_compute_header_size + process_binlog_zip_header)
}

const (
	kfsBinlogZipChunkSizeExp = 24
	kfsBinlogZipChunkSize    = 1 << kfsBinlogZipChunkSizeExp
)

func readKfsBinlogZipHeader(lev *kfsBinlogZipHeader, data []byte) (int64, error) {
	if len(data) < 148 {
		return 0, binlog.ErrorNotEnoughData
	}
	lev.Magic = magicKfsBinlogZipMagic
	lev.Format = int32(binary.LittleEndian.Uint32(data[4:]))
	lev.OrigFileSize = int64(binary.LittleEndian.Uint64(data[8:]))

	from := 16
	to := from + len(lev.OrigFileMd5)
	copy(lev.OrigFileMd5[:], data[from:to])

	from = to
	to += len(lev.First36Bytes)
	copy(lev.First36Bytes[:], data[from:to])

	from = to
	to += len(lev.Last36Bytes)
	copy(lev.Last36Bytes[:], data[from:to])

	from = to
	to += len(lev.First1mMd5)
	copy(lev.First1mMd5[:], data[from:to])

	from = to
	to += len(lev.First128kMd5)
	copy(lev.First128kMd5[:], data[from:to])

	from = to
	to += len(lev.Last128kMd5)
	copy(lev.Last128kMd5[:], data[from:to])

	from = to
	lev.FileHash = int64(binary.LittleEndian.Uint64(data[from:]))
	from += 8

	if int64(len(data)-from) < lev.getChunksCount()*8+4 {
		return 0, binlog.ErrorNotEnoughData
	}

	lev.ChunkOffset = make([]uint64, lev.getChunksCount())
	for i := range lev.ChunkOffset {
		val := binary.LittleEndian.Uint64(data[from:])
		from += 8
		lev.ChunkOffset[i] = val
	}

	lev.Crc32 = binary.LittleEndian.Uint32(data[from:])

	return int64(from + 4), nil
}

// kfs_bz_get_chunks_no
func (h *kfsBinlogZipHeader) getChunksCount() int64 {
	return (h.OrigFileSize + (kfsBinlogZipChunkSize - 1)) >> kfsBinlogZipChunkSizeExp
}

// kfs_bz_format_t
func (h *kfsBinlogZipHeader) getCompressAlgo() compressAlgo {
	algo := compressAlgo(h.Format & 15)
	switch algo {
	case 0:
		return compressAlgoZlib
	case 1:
		return compressAlgoBz2
	case 2:
		return compressAlgoXz
	default:
		return compressAlgoUnknown
	}
}
