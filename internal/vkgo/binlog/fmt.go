// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package binlog

import "fmt"

func (c ChangeRoleInfo) String() string {
	return fmt.Sprintf("{master=%t, ready=%t, vn=%d}", c.IsMaster, c.IsReady, c.ViewNumber)
}
