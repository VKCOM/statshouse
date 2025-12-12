// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlitev2

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"unsafe"
)

var errColumnNotFound = errors.New("column not found")

type Entity struct {
	TableName         string
	columns           map[int32]*EntityColumn
	primaryKey        []int32
	uniqueConstraints [][]int32
}

func NewEntity(tableName string) *Entity {
	return &Entity{
		TableName:         tableName,
		columns:           map[int32]*EntityColumn{},
		primaryKey:        make([]int32, 0),
		uniqueConstraints: make([][]int32, 0),
	}
}

func (e *Entity) SetColumn(name string, columnType ColumnTypeInfo, columnID int32) *ColumnAttributes {
	column := &EntityColumn{
		Name:           name,
		ColumnTypeInfo: columnType,
		columnID:       columnID,
		Attributes:     newSimpleColumnAttributes(),
	}
	e.columns[columnID] = column
	return &column.Attributes
}

func (e *Entity) SetPrimaryColumn(name string, columnType ColumnTypeInfo, columnID int32) *ColumnAttributes {
	column := &EntityColumn{
		Name:           name,
		ColumnTypeInfo: columnType,
		columnID:       columnID,
		Attributes:     newPrimaryColumnAttributes(),
	}
	e.columns[columnID] = column
	return &column.Attributes
}

func (e *Entity) SetTechColumn(name string, columnType ColumnTypeInfo, columnID int32) *ColumnAttributes {
	column := &EntityColumn{
		Name:           name,
		ColumnTypeInfo: columnType,
		columnID:       columnID,
		Attributes:     newTechColumnAttributes(),
	}
	e.columns[columnID] = column
	return &column.Attributes
}

func (e *Entity) SetConstColumn(name string, columnType ColumnTypeInfo, columnID int32) *ColumnAttributes {
	column := &EntityColumn{
		Name:           name,
		ColumnTypeInfo: columnType,
		columnID:       columnID,
		Attributes:     newConstColumnAttributes(),
	}
	e.columns[columnID] = column
	return &column.Attributes
}

func (e *Entity) SetRawColumn(columnID int32, column *EntityColumn) {
	e.columns[columnID] = column
}

// PrimaryKey sets primary key expression for table using column ids.
func (e *Entity) PrimaryKey(columnIDs ...int32) {
	e.primaryKey = make([]int32, 0, len(columnIDs))
	e.primaryKey = append(e.primaryKey, columnIDs...)
}

// UniqueConstraint adds unique constraint to the table using column ids.
func (e *Entity) UniqueConstraint(columnIDs ...int32) {
	uniqueConstraint := make([]int32, 0, len(columnIDs))
	uniqueConstraint = append(uniqueConstraint, columnIDs...)

	e.uniqueConstraints = append(e.uniqueConstraints, uniqueConstraint)
}

// CreateTableQuery builds SQL query for table creation.
func (e *Entity) createTableQuery() string {
	result := "CREATE TABLE IF NOT EXISTS " + e.TableName + " ("

	columnIDs := slices.Sorted(maps.Keys(e.columns))
	result += buildStringFromSlice(columnIDs, func(cid int32) string {
		column := e.columns[cid]

		sql := column.Name + " " + column.ColumnTypeInfo.ToSql()

		if !column.IsNullable() {
			sql += " NOT NULL"
		}
		if column.HasDefault() {
			sql += fmt.Sprintf(" DEFAULT %s", column.GetDefault())
		}

		return sql
	})

	if len(e.primaryKey) > 0 {
		result += ", PRIMARY KEY ("

		result += buildStringFromSlice(e.primaryKey, func(cid int32) string {
			return e.columns[cid].Name
		})

		result += ")"
	}

	for _, uniqueConstraint := range e.uniqueConstraints {
		result += ", UNIQUE ("

		result += buildStringFromSlice(uniqueConstraint, func(cid int32) string {
			return e.columns[cid].Name
		})

		result += ")"
	}

	result += ") WITHOUT ROWID;"

	return result
}

func (e *Entity) FillSelectByMaskQuery(dst *[]byte, mask uint32) {
	e.FillSelectByMasksQuery(dst, []uint32{mask})
}

func (e *Entity) FillSelectByMasksQuery(dst *[]byte, masks []uint32) {
	if len(masks) == 0 {
		return
	}

	var hasColumn, ok bool
	var column *EntityColumn
	var mask uint32
	var maskIndex int

	for maskIndex, mask = range masks {
		if mask == 0 {
			continue
		}

		for i := int32(0); i < 32; i++ {
			if column, ok = e.columns[i+int32(32*maskIndex)]; !ok {
				continue
			}

			if (mask & (1 << (column.columnID % 32))) > 0 {
				if hasColumn {
					*dst = append(*dst, ',')
				} else {
					*dst = append(*dst, []byte("SELECT ")...)
				}

				hasColumn = true
				*dst = append(*dst, column.Name...)
			}
		}
	}

	if !hasColumn {
		return
	}

	*dst = append(*dst, []byte(" FROM ")...)
	*dst = append(*dst, e.TableName...)
}

func (e *Entity) IsIntColumnUnderCondition(name []byte) bool {
	for _, column := range e.columns {
		if column.IsUnderCondition() && column.Name == bytesAsString(name) && column.ColumnTypeInfo == ColumnInt {
			return true
		}
	}

	return false
}

func (e *Entity) IsColumnUnderCondition(name []byte) bool {
	for _, column := range e.columns {
		if column.IsUnderCondition() && column.Name == bytesAsString(name) {
			return true
		}
	}

	return false
}

func (e *Entity) IsColumnNullable(name []byte) bool {
	for _, column := range e.columns {
		if column.Name == bytesAsString(name) {
			return column.IsNullable()
		}
	}

	return false
}

func (e *Entity) GetColumnIDByName(name []byte) (columnID int32, err error) {
	for _, column := range e.columns {
		if column.Name == bytesAsString(name) {
			return column.columnID, nil
		}
	}

	return 0, errColumnNotFound
}

func (e *Entity) GetChangeableNameByID(id int32) []byte {
	if column, ok := e.columns[id]; ok {
		if column.isChangeable() && column.ColumnTypeInfo == ColumnInt {
			return []byte(column.Name)
		}
	}

	return nil
}

func (e *Entity) GetChangeableNameByIDBlob(id int32) []byte {
	if column, ok := e.columns[id]; ok {
		if column.columnID == id && column.isChangeable() && column.ColumnTypeInfo == ColumnBlob {
			return []byte(column.Name)
		}
	}

	return nil
}

func bytesAsString(b []byte) string {
	return unsafe.String(&b[0], len(b))
}

func buildStringFromSlice[T any](slice []T, builder func(T) string) (s string) {
	for i, el := range slice {
		if i > 0 {
			s += ","
		}
		s += builder(el)
	}
	return
}
