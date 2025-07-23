// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fsbinlog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/myxo/gofs"
	"pgregory.net/rand"

	"github.com/VKCOM/statshouse/internal/vkgo/binlog"
	"github.com/VKCOM/statshouse/internal/vkgo/binlog/fsbinlog/internal/gen/tlfsbinlog"
)

func chooseFilenameForChunk(fs gofs.FS, pos int64, prefixPath string) string {
	posStr := fmt.Sprintf(`%05d`, pos)
	posSize := len(posStr) - 4
	prefix := fmt.Sprintf(`%s.%02d`, prefixPath, posSize)

	for l := 4; l < len(posStr); l++ {
		filename := prefix + posStr[:l] + `.bin`
		if _, err := fs.Stat(filename); os.IsNotExist(err) {
			return filename
		}
	}

	panic(`[WTF] Unreachable code in chooseFilenameForChunk`)
}

type FileHeader struct {
	FileName      string
	Position      int64
	Timestamp     uint64
	LevRotateFrom levRotateFrom // если есть

	CompressInfo struct {
		Compressed   bool
		Algo         compressAlgo
		ChunkOffsets []uint64
		headerSize   int64
	}
}

func getBinlogIndexByPosition(position int64, fileHeaders []FileHeader) int {
	idx := 0
	for i, hdr := range fileHeaders {
		if hdr.Position > position {
			break
		}
		idx = i
	}
	return idx
}

func hasFile(name string, files []FileHeader) bool {
	for _, file := range files {
		if file.FileName == name {
			return true
		}
	}
	return false
}

func ScanForFilesFromPos(fs gofs.FS, afterThisPosition int64, prefixPath string, expectedMagic uint32, knownFiles []FileHeader) ([]FileHeader, error) {
	root := path.Dir(prefixPath)
	basename := path.Base(prefixPath)

	allFilenames, err := fs.ReadDir(root)
	if err != nil {
		return nil, err
	}

	var newFileHeaders []FileHeader

	for _, fi := range allFilenames {
		name := fi.Name()
		if !strings.HasPrefix(name, basename) {
			continue
		}

		if !strings.HasSuffix(name, ".bin") && !strings.HasSuffix(name, ".bin.bz") {
			continue
		}

		filepath := path.Join(root, name)
		if hasFile(filepath, knownFiles) {
			continue
		}

		var fh FileHeader
		fh.FileName = filepath
		if err := readBinlogHeaderFile(fs, &fh, expectedMagic); err != nil {
			if afterThisPosition != 0 {
				// Если мы ожидаем новые бинлоги в конце, то можем попсть в ситуацию,
				// когда старый бинлог удалят прямо между получением списка файлов и чтением его заголовка.
				// Так что просто игнорируем такие ошибки в данном случае.
				continue
			}

			return nil, fmt.Errorf("could not readBinlogHeaderFromFile %s: %w", filepath, err)
		}

		if (afterThisPosition == 0) || (fh.Position > afterThisPosition) {
			newFileHeaders = append(newFileHeaders, fh)
		}
	}

	sort.Slice(newFileHeaders, func(i, j int) bool {
		return newFileHeaders[i].Position < newFileHeaders[j].Position
	})

	return newFileHeaders, nil
}

// WriteEmptyBinlog записывает содержимое начального блока нового бинлога (имя файла "name.000000.bin")
func writeEmptyBinlog(options Options, w io.Writer) error {
	levStart := tlfsbinlog.LevStart{
		SchemaId:   int32(options.Magic),
		ExtraBytes: 0,
		SplitMod:   int32(options.ClusterSize),
		SplitMin:   int32(options.EngineIDInCluster),
		SplitMax:   int32(options.EngineIDInCluster + 1),
	}

	data := levStart.WriteBoxed(nil)
	if _, err := w.Write(data); err != nil {
		return err
	}

	// TODO:

	levTag := levTag{
		Type: magicLevTag,
		// Tag: nil, // заполняется ниже
	}

	// в tag недопустимы нулевые байты
	if _, err := rand.Read(levTag.Tag[:]); err != nil {
		return err
	}
	for i := range levTag.Tag {
		for levTag.Tag[i] == 0 {
			levTag.Tag[i] = uint8(rand.Uint32())
		}
	}

	if err := binary.Write(w, binary.LittleEndian, levTag); err != nil {
		return err
	}

	return nil
}

func prepareSnapMeta(pos int64, crc uint32, ts uint32) []byte {
	meta := tlfsbinlog.SnapshotMeta{
		CommitPosition: pos,
		CommitCrc:      crc,
		CommitTs:       ts,
	}
	buffTmp := [20]byte{}
	buff := meta.WriteBoxed(buffTmp[:0])
	return buff
}

func updateCrc(prevCrc uint32, p []byte) uint32 {
	return crc32.Update(prevCrc, crc32.IEEETable, p)
}

func getAlignedBuffer(buff []byte) []byte {
	return buff[:len(buff)-(len(buff)%4)]
}

type readBuffer struct {
	buff []byte
	size int
}

func newReadBuffer(size int) *readBuffer {
	return &readBuffer{
		buff: make([]byte, size),
	}
}

func (b *readBuffer) TryReadFrom(r io.Reader) (int, error) {
	if b.size == cap(b.buff) {
		tmp := make([]byte, cap(b.buff)*2)
		copy(tmp, b.buff)
		b.buff = tmp
	}
	n, err := r.Read(b.buff[b.size:])
	b.size += n
	return n, err
}

func (b *readBuffer) RemoveProcessed(readBytes int) {
	if readBytes != 0 {
		copy(b.buff, b.buff[readBytes:b.size])
		b.size -= readBytes
	}
}

func (b *readBuffer) Bytes() []byte {
	return b.buff[:b.size]
}

func (b *readBuffer) IsLowFilled() bool {
	return b.size < int(float64(cap(b.buff))*0.1)
}

type EmptyReindexOperator struct {
}

var _ binlog.ReindexOperator = &EmptyReindexOperator{}

func (f *EmptyReindexOperator) FinishedError() {
}

func (f *EmptyReindexOperator) FinishedOk(bool) {
}
