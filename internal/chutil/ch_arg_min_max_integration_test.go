// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package chutil

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

// Package-level variables to share ClickHouse across tests
var (
	clickHouseContainer testcontainers.Container
	clickHouseAddr      string
	httpClient          *http.Client
)

// TestMain runs once per test file and sets up shared resources
func TestMain(m *testing.M) {
	ctx := context.Background()

	// Start ClickHouse container once for all tests
	var err error
	clickHouseContainer, err = clickhouse.Run(ctx,
		"clickhouse/clickhouse-server:24.3-alpine",
		clickhouse.WithDatabase("default"),
		clickhouse.WithUsername("default"),
		clickhouse.WithPassword("secret"),
	)
	if err != nil {
		fmt.Printf("Failed to start ClickHouse container: %v\n", err)
		os.Exit(1)
	}

	// Get connection details
	host, err := clickHouseContainer.Host(ctx)
	if err != nil {
		fmt.Printf("Failed to get ClickHouse host: %v\n", err)
		os.Exit(1)
	}
	port, err := clickHouseContainer.MappedPort(ctx, "8123/tcp")
	if err != nil {
		fmt.Printf("Failed to get ClickHouse port: %v\n", err)
		os.Exit(1)
	}
	clickHouseAddr = fmt.Sprintf("%s:%s", host, port.Port())

	// Create shared HTTP client
	httpClient = &http.Client{Timeout: 30 * time.Second}

	// Create tables once for all tests
	err = execQuery(httpClient, clickHouseAddr, "default", "secret", `
		CREATE TABLE v2 (
			id UInt32,
			agg AggregateFunction(argMin, Int32, Float32)
		) ENGINE = AggregatingMergeTree()
		ORDER BY id
	`)
	if err != nil {
		fmt.Printf("Failed to create v2 table: %v\n", err)
		os.Exit(1)
	}

	err = execQuery(httpClient, clickHouseAddr, "default", "secret", `
		CREATE TABLE v3 (
			id UInt32,
			agg AggregateFunction(argMin, String, Float32)
		) ENGINE = AggregatingMergeTree()
		ORDER BY id
	`)
	if err != nil {
		fmt.Printf("Failed to create v3 table: %v\n", err)
		os.Exit(1)
	}

	// Run all tests
	exitCode := m.Run()

	// Clean up ClickHouse container
	if clickHouseContainer != nil {
		_ = testcontainers.TerminateContainer(clickHouseContainer)
	}

	os.Exit(exitCode)
}

func TestArgMinInt32ToStringMigrationIntegration(t *testing.T) {
	// Insert test data using argMinState in SELECT query (not VALUES)
	// err := execQuery(httpClient, clickHouseAddr, "default", "secret", `INSERT INTO v2
	// 	SELECT 1 as id, argMinState(toInt32(42), toFloat32(1.5)) as agg`)
	agg := &data_model.ArgMinMaxInt32Float32{
		Arg: 42,
		Val: 1.5,
	}
	aggBin := agg.MarshalAppend(nil)
	err := insertRawBinary(httpClient, clickHouseAddr, "default", "secret", "v2(id, agg)", [][]byte{
		encodeUInt32(1), // id
		aggBin,          // agg
	})
	require.NoError(t, err)

	// Read the state from v2 using HTTP
	aggRaw, err := selectRawBinary(httpClient, clickHouseAddr, "default", "secret", `SELECT agg FROM v2 WHERE id=1`)
	require.NoError(t, err)

	// Unmarshal as ArgMinMaxInt32Float32
	var v2 data_model.ArgMinMaxInt32Float32
	protoR := newProtoReader(aggRaw)
	require.NoError(t, v2.ReadFromProto(protoR))
	require.Equal(t, int32(42), v2.Arg)
	require.Equal(t, float32(1.5), v2.Val)

	// Convert to V3 format
	v3 := v2.ToStringFormat()
	v3bin := v3.MarshallAppend(nil)

	// Insert into v3 using binary data
	err = insertRawBinary(httpClient, clickHouseAddr, "default", "secret", "v3(id, agg)", [][]byte{
		encodeUInt32(1), // id
		v3bin,           // agg
	})
	require.NoError(t, err)

	// Read back from v3
	aggRaw2, err := selectRawBinary(httpClient, clickHouseAddr, "default", "secret", `SELECT agg FROM v3 WHERE id=1`)
	require.NoError(t, err)

	var v3b data_model.ArgMinMaxStringFloat32
	protoR2 := newProtoReader(aggRaw2)
	_, err = v3b.ReadFromProto(protoR2, make([]byte, 6))
	require.NoError(t, err)

	// Check values
	require.Equal(t, float32(1.5), v3b.Val)
	require.Equal(t, int32(42), v3b.AsInt32)
}

func TestArgMinInt32ToStringMigrationIntegration_Empty(t *testing.T) {
	// Insert empty aggregate by creating it manually and inserting binary data
	// Create empty V2 argMin data (no hasArg, no hasVal)
	emptyV2Data := []byte{0, 0} // hasArg=0, hasVal=0

	// Convert empty V2 to V3 format to verify it works
	var emptyV2 data_model.ArgMinMaxInt32Float32
	emptyProtoR := newProtoReader(emptyV2Data)
	err := emptyV2.ReadFromProto(emptyProtoR)
	require.NoError(t, err)

	err = insertRawBinary(httpClient, clickHouseAddr, "default", "secret", "v2", [][]byte{
		encodeUInt32(2), // id
		emptyV2Data,     // empty agg
	})
	require.NoError(t, err)

	// Read the state from v2
	aggRaw, err := selectRawBinary(httpClient, clickHouseAddr, "default", "secret", `SELECT agg FROM v2 WHERE id=2`)
	require.NoError(t, err)

	// Unmarshal as ArgMinMaxInt32Float32 (should be empty)
	var v2 data_model.ArgMinMaxInt32Float32
	protoR := newProtoReader(aggRaw)
	err = v2.ReadFromProto(protoR)
	require.NoError(t, err)

	// Convert to V3
	v3 := v2.ToStringFormat()
	v3bin := v3.MarshallAppend(nil)

	// Insert into v3
	err = insertRawBinary(httpClient, clickHouseAddr, "default", "secret", "v3", [][]byte{
		encodeUInt32(2), // id
		v3bin,           // agg
	})
	require.NoError(t, err)

	// Read back from v3
	aggRaw2, err := selectRawBinary(httpClient, clickHouseAddr, "default", "secret", `SELECT agg FROM v3 WHERE id=2`)
	require.NoError(t, err)

	var v3b data_model.ArgMinMaxStringFloat32
	protoR2 := newProtoReader(aggRaw2)
	_, err = v3b.ReadFromProto(protoR2, make([]byte, 6))
	require.NoError(t, err)

	// Check values are zero/empty as expected for empty aggregates
	require.Equal(t, float32(0), v3b.Val)
	require.Equal(t, int32(0), v3b.AsInt32)
}

func execQuery(httpClient *http.Client, addr, user, password, query string) error {
	req := &ClickHouseHttpRequest{
		httpClient: httpClient,
		addr:       addr,
		user:       user,
		password:   password,
		query:      query,
	}
	_, err := req.Execute()
	return err
}

func selectRawBinary(httpClient *http.Client, addr, user, password, query string) ([]byte, error) {
	req := &ClickHouseHttpRequest{
		httpClient: httpClient,
		addr:       addr,
		user:       user,
		password:   password,
		query:      query,
		format:     "RowBinary",
	}
	return req.Execute()
}

func insertRawBinary(httpClient *http.Client, addr, user, password, tableDesc string, rows [][]byte) error {
	var body []byte
	for _, row := range rows {
		body = append(body, row...)
	}

	req := &ClickHouseHttpRequest{
		httpClient: httpClient,
		addr:       addr,
		user:       user,
		password:   password,
		query:      fmt.Sprintf("INSERT INTO %s FORMAT RowBinary", tableDesc),
		body:       body,
		urlParams:  map[string]string{"input_format_values_interpret_expressions": "0"},
	}
	_, err := req.Execute()
	return err
}

func encodeUInt32(v uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, v)
	return buf
}

// Helper to create a ProtoReader from []byte
// testProtoReader implements data_model.ProtoReader for test purposes

type testProtoReader struct {
	buf *bytes.Reader
}

func (r *testProtoReader) ReadByte() (byte, error) {
	return r.buf.ReadByte()
}

func (r *testProtoReader) ReadFull(buf []byte) error {
	_, err := r.buf.Read(buf)
	return err
}

func newProtoReader(b []byte) *testProtoReader {
	return &testProtoReader{buf: bytes.NewReader(b)}
}
