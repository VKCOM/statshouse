// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package binlog

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/zeebo/xxh3"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/tlbarsic"
)

type SnapshotHeader = tlbarsic.SnapshotHeader
type SnapshotDependency = tlbarsic.SnapshotDependency
type SnapshotExternalFile = tlbarsic.SnapshotExternalFile

var ErrHeaderCorrupted = fmt.Errorf("snapshot header corrupted")
var ErrInvalidName = fmt.Errorf("invalid name format")

const (
	SnapshotExt     = ".snap"
	DiffSnapshotExt = ".diff"
	TimeLayout      = "2006-01-02T15-04-05"
)

const (
	snapMagicSize      = 4
	snapHeaderSizeSize = 8
	snapHeaderHashSize = 16
)

// ReadSnapshotHeader read common snapshot format. All snapshots need to have same header,
// so replicator or barsic can read it and can replicate, backup, delete etc. Header spec
// in TL format, and wrapped in frame with size and checksum. In case that buffer not have enough
// data to parse header, it will return io.ErrUnexpectedEOF and sizeHint with needed number of bytes
//
// Header has checksum to check if it corrupted. All other snapshot data should be protected
// by checksum by engine itself.
//
// Header format: magic (4b) + tl size (8b) + tlbarsic.SnapshotHeader + checksum (16b, xxh, be)
func ReadSnapshotHeader(buffer []byte) (_ []byte, hdr SnapshotHeader, sizeHint int64, err error) {
	r, err := basictl.NatReadExactTag(buffer, hdr.TLTag())
	if err != nil {
		return r, hdr, 12, err
	}

	var tlSize int64
	r, err = basictl.LongRead(r, &tlSize)
	if err != nil {
		return r, hdr, 12, err
	}
	tlFinish := snapMagicSize + snapHeaderSizeSize + tlSize
	hdrSize := tlFinish + snapHeaderHashSize

	if int(hdrSize) > len(buffer) {
		return r, hdr, hdrSize, io.ErrUnexpectedEOF
	}

	calcSum := xxh3.Hash128(buffer[:tlFinish])
	readSum := hashFromBytes(buffer[tlFinish:])
	if calcSum != readSum {
		return r, hdr, hdrSize, fmt.Errorf("expect checksum 0x%x, got 0x%x, %w", calcSum, readSum, ErrHeaderCorrupted)
	}

	r, err = hdr.Read(r)
	if err != nil {
		return r, hdr, hdrSize, err
	}
	r = r[snapHeaderHashSize:]
	return r, hdr, hdrSize, nil
}

// WriteSnapshotHeader write snapshot common header, see ReadSnapshotHeader doc
func WriteSnapshotHeader(w []byte, hdr *SnapshotHeader) []byte {
	w = basictl.NatWrite(w, hdr.TLTag())
	w = basictl.LongWrite(w, 0) // will write tl size here
	w = hdr.Write(w)            // this Write() never return error
	tlSize := uint64(len(w) - snapHeaderSizeSize - snapMagicSize)
	binary.LittleEndian.PutUint64(w[snapMagicSize:], tlSize)

	// canonical way to store xxh hash is in BigEndian
	hashSum := xxh3.Hash128(w)
	w = binary.BigEndian.AppendUint64(w, hashSum.Hi)
	w = binary.BigEndian.AppendUint64(w, hashSum.Lo)
	return w
}

func hashFromBytes(r []byte) xxh3.Uint128 {
	var result xxh3.Uint128
	result.Hi = binary.BigEndian.Uint64(r)
	result.Lo = binary.BigEndian.Uint64(r[8:])
	return result
}

func CanonicalDiffSnapshotName(clusterId string, shardId string, payloadOffset int64, t time.Time) string {
	return CanonicalSnapshotNameNoExt(clusterId, shardId, payloadOffset, t) + DiffSnapshotExt
}

func CanonicalSnapshotName(clusterId string, shardId string, payloadOffset int64, t time.Time) string {
	return CanonicalSnapshotNameNoExt(clusterId, shardId, payloadOffset, t) + SnapshotExt
}

func CanonicalSnapshotNameNoExt(clusterId string, shardId string, payloadOffset int64, t time.Time) string {
	return fmt.Sprintf("%s_%s.%015d.%s", clusterId, shardId, payloadOffset, t.Format(TimeLayout))
}

func ExtractDataFromName(name string) (prefix string, payloadOffset int64, t time.Time, ext string, err error) {
	var pos int

	if pos = strings.Index(name, "."); pos == -1 {
		return prefix, payloadOffset, t, ext, ErrInvalidName
	}

	prefix = name[:pos]
	name = name[pos+1:]

	if pos = strings.Index(name, "."); pos == -1 {
		return prefix, payloadOffset, t, ext, err
	}

	if payloadOffset, err = strconv.ParseInt(name[:pos], 10, 64); err != nil {
		return prefix, payloadOffset, t, ext, err
	}

	name = name[pos+1:]
	pos = strings.Index(name, ".")

	if pos == -1 {
		if t, err = time.Parse(TimeLayout, name); err != nil {
			return prefix, payloadOffset, t, ext, err
		}
	} else {
		if t, err = time.Parse(TimeLayout, name[:pos]); err != nil {
			return prefix, payloadOffset, t, ext, err
		}

		ext = name[pos+1:]
	}

	return prefix, payloadOffset, t, ext, nil
}

func GetLastSnapshot(dir string) (string, error) {
	di, err := os.ReadDir(dir)
	if err != nil {
		return "", fmt.Errorf("cannot read dir: %w", err)
	}

	var maxPO int64
	var maxPath string
	for i := range di {
		_, po, _, ext, err := ExtractDataFromName(di[i].Name())
		if err != nil {
			continue
		}
		if ext != "snap" {
			continue
		}
		if po > maxPO {
			maxPO = po
			maxPath = di[i].Name()
		}
	}
	return maxPath, nil
}
