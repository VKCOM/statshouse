// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { QueryParams } from '@/url2';

export function prepareItemsGroup({
  orderGroup,
  orderPlot,
  groups,
}: Pick<QueryParams, 'orderGroup' | 'orderPlot' | 'groups'>) {
  const orderP = [...orderPlot];
  return orderGroup.map((groupKey) => {
    const plots = orderP.splice(0, groups[groupKey]?.count ?? 0);
    return {
      groupKey,
      plots,
    };
  });
}
