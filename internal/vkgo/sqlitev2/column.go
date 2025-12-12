// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlitev2

import (
	"fmt"
	"regexp"
)

type ColumnTypeInfo interface {
	ToSql() string
}

type SimpleColumnType int

const (
	ColumnInt = SimpleColumnType(iota)
	ColumnBlob
	ColumnReal
	ColumnText
	ColumnJson
)

func (t SimpleColumnType) ToSql() string {
	switch t {
	case ColumnInt:
		return "INT"
	case ColumnBlob:
		return "BLOB"
	case ColumnReal:
		return "REAL"
	case ColumnText:
		return "TEXT"
	case ColumnJson:
		return "JSON"
	default:
		return ""
	}
}

type VarcharColumnType struct {
	length int
}

func (t VarcharColumnType) ToSql() string {
	return fmt.Sprintf("VARCHAR(%d)", t.length)
}

func NewIntColumn() ColumnTypeInfo {
	return ColumnInt
}

func NewBlobColumn() ColumnTypeInfo {
	return ColumnBlob
}

func NewRealColumn() ColumnTypeInfo {
	return ColumnReal
}

func NewTextColumn() ColumnTypeInfo {
	return ColumnText
}

func NewJsonColumn() ColumnTypeInfo {
	return ColumnJson
}

func NewVarcharColumn(length int) ColumnTypeInfo {
	return VarcharColumnType{length: length}
}

var varcharRegex = regexp.MustCompile("VARCHAR(%d)")

func ColumnTypeInfoFromSql(sql string) (ColumnTypeInfo, error) {
	var defaultValue = NewIntColumn()
	var defaultError = fmt.Errorf("invalid ColumnTypeInfo: %s", sql)

	switch sql {
	case "INTEGER":
		fallthrough
	case "INT":
		return NewIntColumn(), nil
	case "BLOB":
		return NewBlobColumn(), nil
	case "REAL":
		return NewRealColumn(), nil
	case "TEXT":
		return NewTextColumn(), nil
	case "JSON":
		return NewJsonColumn(), nil
	default:
		if varcharRegex.Match([]byte(sql)) {
			length := 0
			if _, err := fmt.Sscanf(sql, "VARCHAR(%d)", &length); err != nil {
				return defaultValue, defaultError
			}
			return NewVarcharColumn(length), nil
		}

		return defaultValue, defaultError
	}
}

type DfltValueT struct {
	HasValue  bool
	DfltValue string
}

func NewDfltValue(value string) DfltValueT {
	return DfltValueT{
		HasValue:  true,
		DfltValue: value,
	}
}

type ColumnAttributes struct {
	Nullable bool
	// Indicates if column can be placed under filtering condition
	underCondition bool
	// Indicates whether column can be simple overwriten by query or mutated (e.g. array blob and appending query)
	changeable bool
	// Indicates whether column is tech and shouldn't be queried
	tech      bool
	DfltValue DfltValueT
}

func newSimpleColumnAttributes() ColumnAttributes {
	return ColumnAttributes{
		Nullable:       true,
		underCondition: true,
		changeable:     true,
		tech:           false,
		DfltValue:      DfltValueT{},
	}
}

func newPrimaryColumnAttributes() ColumnAttributes {
	return ColumnAttributes{
		Nullable:       false,
		underCondition: false,
		changeable:     false,
		tech:           false,
		DfltValue:      DfltValueT{},
	}
}

func newTechColumnAttributes() ColumnAttributes {
	return ColumnAttributes{
		Nullable:       true,
		underCondition: false,
		changeable:     false,
		tech:           true,
		DfltValue:      DfltValueT{},
	}
}

func newConstColumnAttributes() ColumnAttributes {
	return ColumnAttributes{
		Nullable:       true,
		underCondition: false,
		changeable:     false,
		tech:           false,
		DfltValue:      DfltValueT{},
	}
}

func (c *ColumnAttributes) NotNull() *ColumnAttributes {
	c.Nullable = false
	return c
}

func (c *ColumnAttributes) UnderCondition() *ColumnAttributes {
	c.underCondition = true
	return c
}

func (c *ColumnAttributes) Default(dfltValue DfltValueT) *ColumnAttributes {
	c.DfltValue = dfltValue
	return c
}

type EntityColumn struct {
	Name           string
	ColumnTypeInfo ColumnTypeInfo
	columnID       int32
	Attributes     ColumnAttributes
}

func (e *EntityColumn) IsNullable() bool {
	return e.Attributes.Nullable
}

func (e *EntityColumn) HasDefault() bool {
	return e.Attributes.DfltValue.HasValue
}

func (e *EntityColumn) GetDefault() string {
	return e.Attributes.DfltValue.DfltValue
}

func (e *EntityColumn) IsUnderCondition() bool {
	return e.Attributes.underCondition
}

func (e *EntityColumn) isChangeable() bool {
	return e.Attributes.changeable
}
