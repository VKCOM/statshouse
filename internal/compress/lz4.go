// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package compress

import (
	"encoding/binary"
	"fmt"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/pierrec/lz4"
)

// We concatenate original size and compressed data in this function to efficiently store on disk
// we return this frame
func CompressAndFrame(originaldata []byte) ([]byte, error) {
	compressed := make([]byte, 4+lz4.CompressBlockBound(len(originaldata))) // Framing - first 4 bytes is original size
	compressedSize, err := lz4.CompressBlockHC(originaldata, compressed[4:], 0)
	if err != nil {
		return nil, fmt.Errorf("CompressBlockHC failed for sbV2: %w", err)
	}
	binary.LittleEndian.PutUint32(compressed, uint32(len(originaldata)))
	if compressedSize >= len(originaldata) { // does not compress (rare for large buckets, so copy is not a problem)
		compressed = append(compressed[:4], originaldata...)
	} else {
		compressed = compressed[:4+compressedSize]
	}
	return compressed, nil
}

func DeFrame(frame []byte) (originalSize uint32, compressedData []byte, err error) {
	if len(frame) < 4 {
		return 0, nil, fmt.Errorf("compression frame is too short")
	}
	return binary.LittleEndian.Uint32(frame), frame[4:], nil
}

func Decompress(originalSize uint32, compressedData []byte) ([]byte, error) {
	if int(originalSize) == len(compressedData) {
		return compressedData, nil
	}
	if originalSize > data_model.MaxUncompressedBucketSize {
		return nil, fmt.Errorf("failed to deserialize compressed statshouse.sourceBucket - uncompressed size %d too big", originalSize)
	}
	bucketBytes := make([]byte, int(originalSize))
	s, err := lz4.UncompressBlock(compressedData, bucketBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize compressed statshouse.sourceBucket: %w", err)
	}
	if s != int(originalSize) {
		return nil, fmt.Errorf("failed to deserialize compressed statshouse.sourceBucket request: expected size %d actual %d", originalSize, s)
	}
	return bucketBytes, nil
}
