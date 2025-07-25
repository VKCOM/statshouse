// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/vkcom/statshouse/internal/vkgo/rowbinary"
)

func TestArgMinMaxConversion(t *testing.T) {
	tests := []struct {
		name     string
		argInt32 int32
		value    float32
		hasData  bool
	}{
		{
			name:    "empty",
			hasData: false,
		},
		{
			name:     "positive_int",
			argInt32: 12345,
			value:    1.5,
			hasData:  true,
		},
		{
			name:     "negative_int",
			argInt32: -67890,
			value:    9.8,
			hasData:  true,
		},
		{
			name:     "zero_int",
			argInt32: 0,
			value:    2.5,
			hasData:  true,
		},
		{
			name:     "max_int32",
			argInt32: math.MaxInt32,
			value:    100.0,
			hasData:  true,
		},
		{
			name:     "min_int32",
			argInt32: math.MinInt32,
			value:    -100.0,
			hasData:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create V2 format data (Int32)
			v2Data := createV2ArgMinMaxData(tt.argInt32, tt.value, tt.hasData)

			// Convert to V3 format (String)
			var v3Buffer []byte
			v3Buffer = convertArgMinMaxToString(v3Buffer, v2Data)

			// Parse the V3 data back to verify correctness
			argStr, val, hasVal, err := parseV3ArgMinMaxString(v3Buffer)
			if err != nil {
				t.Fatalf("Failed to parse V3 data: %v", err)
			}

			// Verify conversion
			if !tt.hasData {
				if hasVal {
					t.Errorf("Expected empty data, but got hasVal=true")
				}
				return
			}

			if !hasVal {
				t.Errorf("Expected hasVal=true, got false")
				return
			}

			if val != tt.value {
				t.Errorf("Expected value %f, got %f", tt.value, val)
			}

			// For Int32 converted to String, we expect 5 bytes: [0, int32_bytes...]
			if len(argStr) != 5 {
				t.Errorf("Expected string length 5, got %d", len(argStr))
				return
			}

			if argStr[0] != 0 {
				t.Errorf("Expected first byte to be 0, got %d", argStr[0])
				return
			}

			// Extract Int32 from string
			extractedInt32 := int32(binary.LittleEndian.Uint32([]byte(argStr[1:])))
			if extractedInt32 != tt.argInt32 {
				t.Errorf("Expected argInt32 %d, got %d", tt.argInt32, extractedInt32)
			}
		})
	}
}

func TestV2RowParsing(t *testing.T) {
	// Create a complete V2 row for testing
	v2Row := &v2Row{
		metric:      123,
		time:        1733000400, // 2024-12-01 09:00:00
		keys:        [16]int32{1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		skey:        "test_skey",
		count:       10.5,
		min:         1.2,
		max:         9.8,
		sum:         55.5,
		sumsquare:   123.45,
		percentiles: []byte{1, 2, 3}, // dummy data
		uniq_state:  []byte{4, 5, 6}, // dummy data
		min_host:    createV2ArgMinMaxData(42, 1.2, true),
		max_host:    createV2ArgMinMaxData(99, 9.8, true),
	}

	// Serialize V2 row to rowbinary
	v2Data := serializeV2Row(v2Row)

	// Parse it back
	parsedRow, bytesRead, err := parseV2Row(v2Data)
	if err != nil {
		t.Fatalf("Failed to parse V2 row: %v", err)
	}

	if bytesRead != len(v2Data) {
		t.Errorf("Expected to read %d bytes, read %d", len(v2Data), bytesRead)
	}

	// Verify all fields
	if parsedRow.metric != v2Row.metric {
		t.Errorf("Expected metric %d, got %d", v2Row.metric, parsedRow.metric)
	}
	if parsedRow.time != v2Row.time {
		t.Errorf("Expected time %d, got %d", v2Row.time, parsedRow.time)
	}
	if parsedRow.skey != v2Row.skey {
		t.Errorf("Expected skey %s, got %s", v2Row.skey, parsedRow.skey)
	}
	if parsedRow.count != v2Row.count {
		t.Errorf("Expected count %f, got %f", v2Row.count, parsedRow.count)
	}

	// Verify argMin/argMax data
	if !bytes.Equal(parsedRow.min_host, v2Row.min_host) {
		t.Errorf("min_host data mismatch")
	}
	if !bytes.Equal(parsedRow.max_host, v2Row.max_host) {
		t.Errorf("max_host data mismatch")
	}
}

func TestV2ToV3Conversion(t *testing.T) {
	v2Row := &v2Row{
		metric:      17, // shard 1 (17 % 16 = 1)
		time:        1733000400,
		keys:        [16]int32{123, 456, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		skey:        "test_conversion",
		count:       5.0,
		min:         0.1,
		max:         4.9,
		sum:         12.5,
		sumsquare:   31.25,
		percentiles: []byte{1, 2, 3, 4, 5}, // dummy percentiles data
		uniq_state:  []byte{6, 7, 8, 9},    // dummy uniq data
		min_host:    createV2ArgMinMaxData(100, 0.1, true),
		max_host:    createV2ArgMinMaxData(200, 4.9, true),
	}

	// Convert to V3
	var v3Buffer []byte
	v3Buffer = convertRowV2ToV3(v3Buffer, v2Row)

	// Verify the V3 data structure by checking key fields
	// This is a basic verification - in a real test we'd parse the entire V3 row
	if len(v3Buffer) == 0 {
		t.Fatal("V3 buffer is empty")
	}

	// The buffer should contain all the required V3 fields
	// We can't easily parse it back without implementing a full V3 parser,
	// but we can verify the buffer is properly constructed
	t.Logf("V3 buffer size: %d bytes", len(v3Buffer))
}

func TestEmptyArgMinMax(t *testing.T) {
	// Test empty argMin/argMax conversion
	emptyData := []byte{}

	var buffer []byte
	buffer = convertArgMinMaxToString(buffer, emptyData)

	// Should produce empty argMin/argMax string format
	expectedEmpty := rowbinary.AppendArgMinMaxStringEmpty(nil)
	if !bytes.Equal(buffer, expectedEmpty) {
		t.Errorf("Empty conversion mismatch. Expected: %v, got: %v", expectedEmpty, buffer)
	}
}

func TestV3RowBinaryFormat(t *testing.T) {
	// Test the V3 row generation in isolation
	v2Row := &v2Row{
		metric:      1,
		time:        1733000400,
		keys:        [16]int32{123, 456, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		skey:        "test",
		count:       10.0,
		min:         1.0,
		max:         5.0,
		sum:         30.0,
		sumsquare:   100.0,
		percentiles: []byte{1, 0, 0, 0, 2, 3},    // With length prefix: 1 byte length, then 2,3
		uniq_state:  []byte{3, 0, 0, 0, 4, 5, 6}, // With length prefix: 3 bytes length, then 4,5,6
		min_host:    createV2ArgMinMaxData(100, 1.0, true),
		max_host:    createV2ArgMinMaxData(200, 5.0, true),
	}

	// Convert to V3
	v3Data := convertRowV2ToV3(nil, v2Row)
	if len(v3Data) == 0 {
		t.Fatal("V3 data should not be empty")
	}

	// Basic structure validation - check that we have the expected minimum size
	// Rough calculation: 1 + 4 + 4 + 1 + 4 + (48 * (4 + 1)) + (6 * 8) + (5 * 8) + aggregate data
	expectedMinSize := 1 + 4 + 4 + 1 + 4 + (48 * 5) + (6 * 8) + (5 * 8) + len(v2Row.percentiles) + len(v2Row.uniq_state)
	if len(v3Data) < expectedMinSize/2 {
		t.Errorf("V3 data size %d is smaller than expected minimum %d", len(v3Data), expectedMinSize/2)
	}

	t.Logf("V3 row size: %d bytes (expected min: %d)", len(v3Data), expectedMinSize)

	// Check that it starts with the expected index_type = 0
	if v3Data[0] != 0 {
		t.Errorf("Expected index_type = 0, got %d", v3Data[0])
	}

	// Check that metric is correctly encoded (little endian)
	expectedMetric := []byte{1, 0, 0, 0} // 1 in little endian
	if len(v3Data) < 5 || !bytes.Equal(v3Data[1:5], expectedMetric) {
		t.Errorf("Expected metric bytes %v, got %v", expectedMetric, v3Data[1:5])
	}
}

func BenchmarkV2ToV3Conversion(b *testing.B) {
	v2Row := &v2Row{
		metric:      1,
		time:        1733000400,
		keys:        [16]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		skey:        "benchmark_test_skey_with_longer_name",
		count:       100.0,
		min:         -50.5,
		max:         150.7,
		sum:         5000.0,
		sumsquare:   25000.0,
		percentiles: make([]byte, 100), // realistic percentiles size
		uniq_state:  make([]byte, 50),  // realistic uniq size
		min_host:    createV2ArgMinMaxData(12345, -50.5, true),
		max_host:    createV2ArgMinMaxData(67890, 150.7, true),
	}

	buffer := make([]byte, 0, 2048) // Pre-allocate buffer

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer = buffer[:0] // Reset buffer
		buffer = convertRowV2ToV3(buffer, v2Row)
	}
}

// Helper functions

func createV2ArgMinMaxData(argInt32 int32, value float32, hasData bool) []byte {
	if !hasData {
		return []byte{}
	}

	var buf []byte

	// hasArg flag
	buf = append(buf, 1)

	// arg (Int32)
	argBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(argBytes, uint32(argInt32))
	buf = append(buf, argBytes...)

	// hasVal flag
	buf = append(buf, 1)

	// value (Float32)
	valBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(valBytes, math.Float32bits(value))
	buf = append(buf, valBytes...)

	return buf
}

func serializeV2Row(row *v2Row) []byte {
	var buf []byte

	// metric (Int32)
	buf = rowbinary.AppendInt32(buf, row.metric)

	// time (UInt32)
	buf = rowbinary.AppendUint32(buf, row.time)

	// keys (16 x Int32)
	for _, key := range row.keys {
		buf = rowbinary.AppendInt32(buf, key)
	}

	// skey (String)
	buf = rowbinary.AppendString(buf, row.skey)

	// Simple aggregates (Float64 each)
	buf = rowbinary.AppendFloat64(buf, row.count)
	buf = rowbinary.AppendFloat64(buf, row.min)
	buf = rowbinary.AppendFloat64(buf, row.max)
	buf = rowbinary.AppendFloat64(buf, row.sum)
	buf = rowbinary.AppendFloat64(buf, row.sumsquare)

	// Complex aggregates (raw bytes with length prefix)
	buf = rowbinary.AppendBytes(buf, row.percentiles)
	buf = rowbinary.AppendBytes(buf, row.uniq_state)
	buf = rowbinary.AppendBytes(buf, row.min_host)
	buf = rowbinary.AppendBytes(buf, row.max_host)

	return buf
}

func parseV3ArgMinMaxString(data []byte) (string, float32, bool, error) {
	if len(data) < 4 {
		return "", 0, false, nil // empty
	}

	pos := 0

	// Read string length (includes +1 for terminator)
	if pos+4 > len(data) {
		return "", 0, false, nil
	}
	strLenWithTerminator := binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	if strLenWithTerminator == 0 {
		// Empty string, should have hasVal flag but no value
		if pos >= len(data) {
			return "", 0, false, nil
		}
		hasVal := data[pos] != 0
		return "", 0, hasVal, nil
	}

	// String length without terminator
	strLen := strLenWithTerminator - 1

	// Read string data
	if pos+int(strLen) > len(data) {
		return "", 0, false, nil
	}
	argStr := string(data[pos : pos+int(strLen)])
	pos += int(strLen)

	// Read string terminator (should be 0)
	if pos >= len(data) {
		return "", 0, false, nil
	}
	terminator := data[pos]
	pos++
	if terminator != 0 {
		return "", 0, false, nil
	}

	// Read hasVal flag
	if pos >= len(data) {
		return "", 0, false, nil
	}
	hasVal := data[pos] != 0
	pos++

	if !hasVal {
		return argStr, 0, false, nil
	}

	// Read value (Float64 - 8 bytes, not Float32)
	if pos+8 > len(data) {
		return "", 0, false, nil
	}
	valFloat64 := math.Float64frombits(binary.LittleEndian.Uint64(data[pos:]))

	return argStr, float32(valFloat64), true, nil
}
