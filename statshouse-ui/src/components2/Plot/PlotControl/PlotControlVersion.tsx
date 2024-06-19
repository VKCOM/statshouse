// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { ReactComponent as SVGLightning } from 'bootstrap-icons/icons/lightning.svg';
import React, { memo, useCallback } from 'react';
import { SwitchBox } from 'components';
import { globalSettings } from 'common/settings';
import { getNewPlot, type PlotKey } from 'url2';
import { useStatsHouseShallow } from '../../../store2';

export type PlotControlVersionProps = {
  plotKey: PlotKey;
};

const defaultUseV2 = getNewPlot().useV2;

export function _PlotControlVersion({ plotKey }: PlotControlVersionProps) {
  const { value, setPlot } = useStatsHouseShallow(({ params: { plots }, setPlot }) => ({
    value: plots[plotKey]?.useV2 ?? defaultUseV2,
    setPlot,
  }));
  const onChange = useCallback(
    (status: boolean) => {
      setPlot(plotKey, (s) => {
        s.useV2 = status;
      });
    },
    [plotKey, setPlot]
  );
  return (
    <SwitchBox checked={value} disabled={globalSettings.disabled_v1} onChange={onChange}>
      <SVGLightning />
    </SwitchBox>
  );
}

export const PlotControlVersion = memo(_PlotControlVersion);
