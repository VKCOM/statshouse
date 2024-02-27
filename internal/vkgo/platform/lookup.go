// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package platform

import (
	"fmt"
	"os/user"
	"strconv"
)

func LookupGroup(groupname string) (int, error) {
	group, err := user.LookupGroup(groupname)
	if err != nil {
		return 0, fmt.Errorf(`could not get group %s: %w`, groupname, err)
	} else if gid, err := strconv.ParseUint(group.Gid, 10, 32); err != nil {
		return 0, fmt.Errorf(`could not parse group id %s: %w`, group.Gid, err)
	} else {
		return int(gid), nil
	}
}

func LookupUser(username string) (int, error) {
	_user, err := user.Lookup(username)
	if err != nil {
		return 0, fmt.Errorf(`could not get user %s: %w`, username, err)
	} else if uid, err := strconv.ParseUint(_user.Uid, 10, 32); err != nil {
		return 0, fmt.Errorf(`could not parse user id %s: %w`, _user.Uid, err)
	} else {
		return int(uid), nil
	}
}
