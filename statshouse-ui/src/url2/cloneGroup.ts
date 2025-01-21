// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GroupInfo } from './queryParams';

export function cloneGroup(group: GroupInfo): GroupInfo;
export function cloneGroup(group: undefined): undefined;
export function cloneGroup(group?: GroupInfo): GroupInfo | undefined {
  if (group == null) {
    return group;
  }
  return {
    ...group,
  };
}
