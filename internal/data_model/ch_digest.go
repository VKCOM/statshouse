// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
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

func (cd *ChDigest) ReadFrom(r io.ByteReader) error {
	// Read number of centroids as varint
	centroidCount, err := binary.ReadUvarint(r)
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
	sampleFactor := float32(1.0)

	for i := 0; i < int(centroidCount); i++ {
		// Read mean as float32
		mean, err := readFloat32LE(r)
		if err != nil {
			return fmt.Errorf("failed to read centroid %d mean: %w", i, err)
		}

		// Read weight as float32
		weight, err := readFloat32LE(r)
		if err != nil {
			return fmt.Errorf("failed to read centroid %d weight: %w", i, err)
		}
		weight = weight / sampleFactor

		// Add centroid to digest
		cd.Digest.AddCentroid(tdigest.Centroid{Mean: float64(mean), Weight: float64(weight)})
	}
	cd.Digest.Normalize()

	return nil
}

func (cd *ChDigest) MarshallAppend(buf []byte, sampleFactor float64) []byte {
	return rowbinary.AppendCentroids(buf, cd.Digest, sampleFactor)
}

func readFloat32LE(r io.ByteReader) (float32, error) {
	bits, err := readUint32LE(r)
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(bits), nil
}
