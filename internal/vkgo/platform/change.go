// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package platform

import (
	"fmt"
	"os"
	"syscall"
)

func ChangeUserGroup(username, groupname string) error {
	if os.Getuid() != 0 {
		return nil
	}

	if groupname != "" {
		if gid, err := LookupGroup(groupname); err != nil {
			return fmt.Errorf("could not get group %s: %w", groupname, err)
		} else if err := syscall.Setgid(gid); err != nil {
			return fmt.Errorf("could not change group to %s: %w", groupname, err)
		}
	}

	if username != "" {
		if uid, err := LookupUser(username); err != nil {
			return fmt.Errorf("could not get user %s: %w", username, err)
		} else if err := syscall.Setuid(uid); err != nil {
			return fmt.Errorf("could not change user to %s: %w", username, err)
		}
	}

	return nil
}

func Chown(path string, user string, group string) error {
	uid, gid := -1, -1
	var err error

	if user != "" {
		uid, err = LookupUser(user)
		if err != nil {
			return fmt.Errorf("failed to lookup user %q: %w", user, err)
		}
	}

	if group != "" {
		gid, err = LookupGroup(group)
		if err != nil {
			return fmt.Errorf("failed to lookup group %q: %w", group, err)
		}
	}

	err = os.Chown(path, uid, gid)
	if err != nil {
		return fmt.Errorf("failed to chown %q to %q:%q: %w", path, user, group, err)
	}

	return nil
}
