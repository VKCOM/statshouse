// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useCallback, useMemo } from 'react';
import { getMetricWhats } from '@/store2/helpers';
import { usePlotsDataStore } from '@/store2/plotDataStore';
import type { QueryWhat } from '@/api/enum';

export function useMetricWhats() {
  const { plot } = useWidgetPlotContext();
  const whats = usePlotsDataStore(useCallback(({ plotsData }) => plotsData[plot.id]?.whats, [plot.id]));

  return useMemo<QueryWhat[]>(() => getMetricWhats(plot, whats), [plot, whats]);
}
