// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { ReactComponent as SVGLightning } from 'bootstrap-icons/icons/lightning.svg';
import React, { memo, useCallback } from 'react';
import { SwitchBox } from '@/components/UI';
import { globalSettings } from '@/common/settings';
import { useStatsHouse } from '@/store2';
import { METRIC_VALUE_BACKEND_VERSION, toMetricValueBackendVersion } from '@/api/enum';
import { useStoreDev } from '@/store2/dev';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';

export const PlotControlVersion = memo(function PlotControlVersion() {
  const devMode = useStoreDev((s) => s.enabled);
  const {
    plot: { backendVersion },
    setPlot,
  } = useWidgetPlotContext();

  const isDeveloper = useStatsHouse(({ user: { developer } }) => developer);
  const onChange = useCallback(
    (status: boolean) => {
      setPlot((s) => {
        s.backendVersion = status ? METRIC_VALUE_BACKEND_VERSION.v2 : METRIC_VALUE_BACKEND_VERSION.v1;
      });
    },
    [setPlot]
  );
  const onChangeSelect = useCallback(
    (event: React.ChangeEvent<HTMLSelectElement>) => {
      const value = toMetricValueBackendVersion(event.currentTarget.value, METRIC_VALUE_BACKEND_VERSION.v2);
      setPlot((s) => {
        s.backendVersion = value;
      });
    },
    [setPlot]
  );
  if (devMode && isDeveloper) {
    return (
      <select className="form-select" style={{ width: 70 }} value={backendVersion} onChange={onChangeSelect}>
        <option value={METRIC_VALUE_BACKEND_VERSION.v1}>v1</option>
        <option value={METRIC_VALUE_BACKEND_VERSION.v2}>v2</option>
        <option value={METRIC_VALUE_BACKEND_VERSION.v3}>v3</option>
      </select>
    );
  }
  return (
    <SwitchBox
      checked={backendVersion === METRIC_VALUE_BACKEND_VERSION.v2}
      disabled={globalSettings.disabled_v1}
      onChange={onChange}
    >
      <SVGLightning />
    </SwitchBox>
  );
});
