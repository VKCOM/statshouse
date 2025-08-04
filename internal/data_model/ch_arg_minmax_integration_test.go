// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/VKCOM/statshouse/internal/chutil"
	"github.com/VKCOM/statshouse/internal/data_model"
	"net/http"
	"os"
	"testing"
	"time"

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

func TestArgInt32(t *testing.T) {
	aggIn := data_model.ArgMinMaxInt32Float32{
		Arg: 42,
		Val: 1.5,
	}
	aggInBytes := aggIn.MarshalAppend(nil)

	err := insertRawBinary(httpClient, clickHouseAddr, "default", "secret", "v2(id, agg)", [][]byte{
		encodeUInt32(1), // id
		aggInBytes,      // aggIn
	})
	require.NoError(t, err)

	aggOutBytes, err := selectRawBinary(httpClient, clickHouseAddr, "default", "secret", `SELECT agg FROM v2 WHERE id=1`)
	require.NoError(t, err)

	var aggOut data_model.ArgMinMaxInt32Float32
	require.NoError(t, aggOut.ReadFrom(bytes.NewReader(aggOutBytes)))
	require.Equal(t, aggIn, aggOut)
}

func TestArgStringAsString(t *testing.T) {
	aggIn := data_model.ArgMinMaxStringFloat32{
		AsString: "unmapped",
		Val:      1.5,
	}
	aggInBytes := aggIn.MarshallAppend(nil)

	err := insertRawBinary(httpClient, clickHouseAddr, "default", "secret", "v3(id, agg)", [][]byte{
		encodeUInt32(2), // id
		aggInBytes,      // agg
	})
	require.NoError(t, err)

	aggOutBytes, err := selectRawBinary(httpClient, clickHouseAddr, "default", "secret", `SELECT agg FROM v3 WHERE id=2`)
	require.NoError(t, err)

	var aggOut data_model.ArgMinMaxStringFloat32
	_, err = aggOut.ReadFrom(bytes.NewReader(aggOutBytes), make([]byte, 6))
	require.NoError(t, err)
	require.Equal(t, aggIn, aggOut)
}

func TestArgStringAsInt(t *testing.T) {
	aggIn := data_model.ArgMinMaxStringFloat32{
		AsInt32: 42,
		Val:     1.5,
	}
	aggInBytes := aggIn.MarshallAppend(nil)

	err := insertRawBinary(httpClient, clickHouseAddr, "default", "secret", "v3(id, agg)", [][]byte{
		encodeUInt32(3), // id
		aggInBytes,      // agg
	})
	require.NoError(t, err)

	aggOutBytes, err := selectRawBinary(httpClient, clickHouseAddr, "default", "secret", `SELECT agg FROM v3 WHERE id=3`)
	require.NoError(t, err)

	var aggOut data_model.ArgMinMaxStringFloat32
	_, err = aggOut.ReadFrom(bytes.NewReader(aggOutBytes), make([]byte, 6))
	require.NoError(t, err)
	require.Equal(t, aggIn, aggOut)
}

func TestArgStringEmpty(t *testing.T) {
	err := insertRawBinary(httpClient, clickHouseAddr, "default", "secret", "v3(id)", [][]byte{
		encodeUInt32(4), // id
	})
	require.NoError(t, err)

	aggOutBytes, err := selectRawBinary(httpClient, clickHouseAddr, "default", "secret", `SELECT agg FROM v3 WHERE id=4`)
	require.NoError(t, err)

	// Unmarshal as ArgMinMaxInt32Float32 (should be empty)
	var aggOut data_model.ArgMinMaxStringFloat32
	_, err = aggOut.ReadFrom(bytes.NewReader(aggOutBytes), nil)
	require.NoError(t, err)
	require.Equal(t, float32(0), aggOut.Val)
	require.Equal(t, int32(0), aggOut.AsInt32)
	require.Equal(t, "", aggOut.AsString)
}

func TestArgIntEmpty(t *testing.T) {
	err := insertRawBinary(httpClient, clickHouseAddr, "default", "secret", "v2(id)", [][]byte{
		encodeUInt32(5), // id
	})
	require.NoError(t, err)

	aggOutBytes, err := selectRawBinary(httpClient, clickHouseAddr, "default", "secret", `SELECT agg FROM v2 WHERE id=5`)
	require.NoError(t, err)

	var aggOut data_model.ArgMinMaxInt32Float32
	err = aggOut.ReadFrom(bytes.NewReader(aggOutBytes))
	require.NoError(t, err)
	require.Equal(t, float32(0), aggOut.Val)
	require.Equal(t, int32(0), aggOut.Arg)
}

func execQuery(httpClient *http.Client, addr, user, password, query string) error {
	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       addr,
		User:       user,
		Password:   password,
		Query:      query,
	}
	_, err := req.Execute()
	return err
}

func selectRawBinary(httpClient *http.Client, addr, user, password, query string) ([]byte, error) {
	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       addr,
		User:       user,
		Password:   password,
		Query:      query,
		Format:     "RowBinary",
	}
	return req.Execute()
}

func insertRawBinary(httpClient *http.Client, addr, user, password, tableDesc string, rows [][]byte) error {
	var body []byte
	for _, row := range rows {
		body = append(body, row...)
	}

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       addr,
		User:       user,
		Password:   password,
		Query:      fmt.Sprintf("INSERT INTO %s FORMAT RowBinary", tableDesc),
		Body:       body,
		UrlParams:  map[string]string{"input_format_values_interpret_expressions": "0"},
	}
	_, err := req.Execute()
	return err
}

func encodeUInt32(v uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, v)
	return buf
}
