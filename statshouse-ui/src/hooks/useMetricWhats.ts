// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useCallback, useMemo } from 'react';
import { isPromQL } from '@/store2/helpers';
import { usePlotsDataStore } from '@/store2/plotDataStore';

export function useMetricWhats() {
  const { plot } = useWidgetPlotContext();
  const { what } = plot;
  const isProm = isPromQL(plot);
  const whats = usePlotsDataStore(useCallback(({ plotsData }) => plotsData[plot.id]?.whats, [plot.id]));

  return useMemo(() => {
    if (!isProm) {
      return [...what];
    }
    if (whats) {
      return whats;
    }
    return [];
  }, [isProm, what, whats]);
}
