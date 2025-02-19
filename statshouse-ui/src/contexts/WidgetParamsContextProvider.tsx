// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { ReactNode, useMemo } from 'react';
import { useStatsHouse } from '@/store2';
import { WidgetParamsContext } from '@/contexts/WidgetParamsContext';

export type WidgetParamsContextProviderProps = {
  children?: ReactNode;
};

const { setParams, setPlot, removePlot } = useStatsHouse.getState();

export function WidgetParamsContextProvider({ children }: WidgetParamsContextProviderProps) {
  const params = useStatsHouse(({ params }) => params);
  const value = useMemo(() => ({ params, setParams, setPlot, removePlot }), [params]);
  return <WidgetParamsContext value={value}>{children}</WidgetParamsContext>;
}
