// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { ReactComponent as SVGPcDisplay } from 'bootstrap-icons/icons/pc-display.svg';
import React, { memo, useCallback } from 'react';
import { SwitchBox } from 'components/UI';
import { useStatsHouseShallow } from 'store2';
import { getNewMetric, type PlotKey } from 'url2';

export type PlotControlMaxHostProps = {
  plotKey: PlotKey;
};

const defaultMaxHost = getNewMetric().maxHost;

export function _PlotControlMaxHost({ plotKey }: PlotControlMaxHostProps) {
  const { value, setPlot } = useStatsHouseShallow(({ params: { plots }, setPlot }) => ({
    value: plots[plotKey]?.maxHost ?? defaultMaxHost,
    setPlot,
  }));
  const onChange = useCallback(
    (status: boolean) => {
      setPlot(plotKey, (s) => {
        s.maxHost = status;
      });
    },
    [plotKey, setPlot]
  );
  return (
    <SwitchBox title="Host" checked={value} onChange={onChange}>
      <SVGPcDisplay />
    </SwitchBox>
  );
}

export const PlotControlMaxHost = memo(_PlotControlMaxHost);
