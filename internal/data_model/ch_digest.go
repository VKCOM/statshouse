// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/VKCOM/statshouse/internal/vkgo/rowbinary"
	"github.com/hrissan/tdigest"
)

// ChDigest wraps TDigest for ClickHouse serialization
type ChDigest struct {
	Digest *tdigest.TDigest
}

func (cd *ChDigest) ReadFrom(r io.Reader) error {
	br := bufio.NewReaderSize(r, 16)

	// Read number of centroids as varint
	centroidCount, err := binary.ReadUvarint(br)
	if err != nil {
		return fmt.Errorf("failed to read centroid count: %w", err)
	}

	if centroidCount == 0 {
		// Empty digest
		cd.Digest = nil
		return nil
	}

	// Create new digest
	cd.Digest = tdigest.New()

	// Use sampleFactor = 1.0 as default, similar to how it's used in aggregator
	sampleFactor := 1.0

	var tmp [4]byte
	for i := 0; i < int(centroidCount); i++ {
		// Read mean as float32
		if _, err := io.ReadFull(br, tmp[:]); err != nil {
			return fmt.Errorf("failed to read centroid %d mean: %w", i, err)
		}
		mean := float64(math.Float32frombits(binary.LittleEndian.Uint32(tmp[:])))

		// Read weight as float32
		if _, err := io.ReadFull(br, tmp[:]); err != nil {
			return fmt.Errorf("failed to read centroid %d weight: %w", i, err)
		}
		weight := float64(math.Float32frombits(binary.LittleEndian.Uint32(tmp[:]))) / sampleFactor

		// Add centroid to digest
		cd.Digest.AddCentroid(tdigest.Centroid{Mean: mean, Weight: weight})
	}
	cd.Digest.Normalize()

	return nil
}

func (cd *ChDigest) MarshallAppend(buf []byte, sampleFactor float64) []byte {
	return rowbinary.AppendCentroids(buf, cd.Digest, sampleFactor)
}
