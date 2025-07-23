// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package internal

import (
	"fmt"
	"strings"

	"github.com/VKCOM/statshouse/internal/vkgo/commonmetrics"
)

func ParseStringAsMethod(magic uint32, fullName string) commonmetrics.Method {
	var method commonmetrics.Method
	if fullName == "" {
		// Allocates, but hopefully this is rare
		method = commonmetrics.Method{Name: fmt.Sprintf("#%08x", magic)}
	} else {
		i := strings.IndexByte(fullName, '.')
		if i == -1 {
			method = commonmetrics.Method{Name: fullName}
		} else {
			method = commonmetrics.Method{Group: fullName[:i], Name: fullName}
		}
	}

	return method
}
