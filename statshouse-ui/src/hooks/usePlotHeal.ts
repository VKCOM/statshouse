// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { PlotKey } from '@/url2';
import { useStatsHouse } from '@/store2';
import { useCallback, useMemo } from 'react';

export function usePlotHeal(plotKey: PlotKey) {
  const status = useStatsHouse(useCallback((s) => s.plotHeals[plotKey], [plotKey]));
  return useMemo(() => !status || status.status || status.lastTimestamp + status.timeout * 1000, [status]);
}
