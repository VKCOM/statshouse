// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GET_PARAMS } from '../api/enum';

export function dashboardURL(id?: number, v?: number | null): string {
  if (!id) {
    return `/api/dashboard`;
  }
  const p = [[GET_PARAMS.dashboardID, id.toString()]];
  if (v != null) {
    p.push([GET_PARAMS.dashboardApiVersion, v.toString()]);
  }
  const strParams = new URLSearchParams(p).toString();
  return `/api/dashboard?${strParams}`;
}
