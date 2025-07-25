//go:build integration
// +build integration

package chutil

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"bytes"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"google.golang.org/protobuf/proto"
)

func TestArgMinInt32ToStringMigrationIntegration(t *testing.T) {
	ctx := context.Background()

	// Start ClickHouse container
	clickHouseContainer, err := clickhouse.Run(ctx,
		"clickhouse/clickhouse-server:24.3-alpine",
		clickhouse.WithDatabase("default"),
		clickhouse.WithUsername("default"),
		clickhouse.WithPassword(""),
	)
	require.NoError(t, err)
	defer func() {
		_ = testcontainers.TerminateContainer(clickHouseContainer)
	}()

	// Get connection details
	host, err := clickHouseContainer.Host(ctx)
	require.NoError(t, err)
	port, err := clickHouseContainer.MappedPort(ctx, "9000/tcp")
	require.NoError(t, err)
	dsn := fmt.Sprintf("clickhouse://%s:%s", host, port.Port())
	db, err := sql.Open("clickhouse", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Create source and target tables
	_, err = db.Exec(`
		CREATE TABLE src (
			id UInt32,
			agg AggregateFunction(argMin, Int32, Float32)
		) ENGINE = Memory
	`)
	require.NoError(t, err)
	_, err = db.Exec(`
		CREATE TABLE dst (
			id UInt32,
			agg AggregateFunction(argMin, String, Float32)
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	// Insert a row into src
	_, err = db.Exec(`INSERT INTO src VALUES (1, argMinState(42, 1.5))`)
	require.NoError(t, err)

	// Read the state from src
	var aggRaw []byte
	row := db.QueryRow(`SELECT agg FROM src WHERE id=1`)
	require.NoError(t, row.Scan(&aggRaw))

	// Unmarshal as ArgMinMaxInt32Float32
	var v2 ArgMinMaxInt32Float32
	protoR := newProtoReader(aggRaw)
	require.NoError(t, v2.unmarshal(protoR, make([]byte, 4)))

	// Convert to V3
	v3 := v2.ToStringFormat()
	v3bin, err := v3.MarshalBinary()
	require.NoError(t, err)

	// Insert into dst
	_, err = db.Exec(`INSERT INTO dst VALUES (?, ?)`, 1, v3bin)
	require.NoError(t, err)

	// Read back from dst
	var aggRaw2 []byte
	row = db.QueryRow(`SELECT agg FROM dst WHERE id=1`)
	require.NoError(t, row.Scan(&aggRaw2))
	var v3b ArgMinMaxStringFloat32
	protoR2 := newProtoReader(aggRaw2)
	_, err = v3b.unmarshal(protoR2, make([]byte, 6))
	require.NoError(t, err)

	// Check values
	require.Equal(t, float32(1.5), v3b.val)
	require.Equal(t, int32(42), v3b.AsInt32)
}

func TestArgMinInt32ToStringMigrationIntegration_Empty(t *testing.T) {
	ctx := context.Background()

	// Start ClickHouse container
	clickHouseContainer, err := clickhouse.Run(ctx,
		"clickhouse/clickhouse-server:24.3-alpine",
		clickhouse.WithDatabase("default"),
		clickhouse.WithUsername("default"),
		clickhouse.WithPassword(""),
	)
	require.NoError(t, err)
	defer func() {
		_ = testcontainers.TerminateContainer(clickHouseContainer)
	}()

	// Get connection details
	host, err := clickHouseContainer.Host(ctx)
	require.NoError(t, err)
	port, err := clickHouseContainer.MappedPort(ctx, "9000/tcp")
	require.NoError(t, err)
	dsn := fmt.Sprintf("clickhouse://%s:%s", host, port.Port())
	db, err := sql.Open("clickhouse", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Create source and target tables
	_, err = db.Exec(`
		CREATE TABLE src (
			id UInt32,
			agg AggregateFunction(argMin, Int32, Float32)
		) ENGINE = Memory
	`)
	require.NoError(t, err)
	_, err = db.Exec(`
		CREATE TABLE dst (
			id UInt32,
			agg AggregateFunction(argMin, String, Float32)
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	// Insert a row with empty agg into src
	_, err = db.Exec(`INSERT INTO src (id) VALUES (2)`)
	require.NoError(t, err)

	// Read the state from src
	var aggRaw []byte
	row := db.QueryRow(`SELECT agg FROM src WHERE id=2`)
	require.NoError(t, row.Scan(&aggRaw))

	// Unmarshal as ArgMinMaxInt32Float32 (should be empty)
	var v2 ArgMinMaxInt32Float32
	protoR := newProtoReader(aggRaw)
	err = v2.unmarshal(protoR, make([]byte, 4))
	// Should not error, but v2 should be empty
	require.NoError(t, err)
	require.Equal(t, int32(0), v2.Arg)
	require.Equal(t, float32(0), v2.val)

	// Convert to V3
	v3 := v2.ToStringFormat()
	v3bin, err := v3.MarshalBinary()
	require.NoError(t, err)

	// Insert into dst
	_, err = db.Exec(`INSERT INTO dst VALUES (?, ?)`, 2, v3bin)
	require.NoError(t, err)

	// Read back from dst
	var aggRaw2 []byte
	row = db.QueryRow(`SELECT agg FROM dst WHERE id=2`)
	require.NoError(t, row.Scan(&aggRaw2))
	var v3b ArgMinMaxStringFloat32
	protoR2 := newProtoReader(aggRaw2)
	_, err = v3b.unmarshal(protoR2, make([]byte, 6))
	require.NoError(t, err)

	// Check values are empty
	require.Equal(t, float32(0), v3b.val)
	require.Equal(t, int32(0), v3b.AsInt32)
	require.True(t, v3b.Arg == "" || v3b.Arg == string([]byte{0, 0, 0, 0, 0}), "Arg should be empty or default encoding")
}

// Helper to create a proto.Reader from []byte
func newProtoReader(b []byte) *proto.Reader {
	return proto.NewReader(bytes.NewReader(b))
}
