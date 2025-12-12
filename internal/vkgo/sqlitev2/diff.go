// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlitev2

import (
	"fmt"
)

func newRetrievedColumnAttributes(nullable bool, dfltValue DfltValueT) ColumnAttributes {
	return ColumnAttributes{
		Nullable:       nullable,
		underCondition: false,
		changeable:     false,
		tech:           false,
		DfltValue:      dfltValue,
	}
}

func IsEqualWithRetrievedField(field *EntityColumn, dbField *EntityColumn) bool {
	dbField.Attributes.underCondition = field.IsUnderCondition()
	dbField.Attributes.changeable = field.isChangeable()
	dbField.Attributes.tech = field.Attributes.tech

	return field.Name == dbField.Name &&
		field.ColumnTypeInfo == dbField.ColumnTypeInfo &&
		field.Attributes == dbField.Attributes
}

func IsFieldValidToAdd(field *EntityColumn) bool {
	return field.IsNullable() // TODO(whatsername): check isn't primary & unique
}

func RetrieveFieldFromRows(rows *Rows) (*EntityColumn, error) {
	cid := int32(rows.ColumnInteger(0))
	name, err := rows.ColumnBlobString(1)
	if err != nil {
		return nil, fmt.Errorf("failed to extract column %v", err)
	}

	ctype, err := rows.ColumnBlobString(2)
	if err != nil {
		return nil, fmt.Errorf("failed to extract column %v: %v", name, err)
	}
	columnTypeInfo, err := ColumnTypeInfoFromSql(ctype)
	if err != nil {
		return nil, fmt.Errorf("failed to extract column %v: %v", name, err)
	}

	notNullable := rows.ColumnInteger(3)

	value, err := rows.ColumnBlobString(4)
	var DfltValue DfltValueT
	if err != nil || value == "" {
		DfltValue = DfltValueT{}
	} else {
		DfltValue = NewDfltValue(value)
	}

	// TODO(whatsername): check if primary by index traversal
	// primary := rows.ColumnInteger(5)

	return &EntityColumn{
		Name:           name,
		ColumnTypeInfo: columnTypeInfo,
		columnID:       cid,
		Attributes:     newRetrievedColumnAttributes(notNullable == 0, DfltValue),
	}, nil
}
