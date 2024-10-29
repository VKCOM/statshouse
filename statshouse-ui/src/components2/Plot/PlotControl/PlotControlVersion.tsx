// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { ReactComponent as SVGLightning } from 'bootstrap-icons/icons/lightning.svg';
import React, { memo, useCallback } from 'react';
import { SwitchBox } from 'components/UI';
import { globalSettings } from 'common/settings';
import { getNewMetric, type PlotKey } from 'url2';
import { useStatsHouseShallow } from 'store2';
import { METRIC_VALUE_BACKEND_VERSION, toMetricValueBackendVersion } from 'api/enum';
import { useStoreDev } from 'store/dev';

export type PlotControlVersionProps = {
  plotKey: PlotKey;
};

const defaultBackendVersion = getNewMetric().backendVersion;

export function _PlotControlVersion({ plotKey }: PlotControlVersionProps) {
  const devMode = useStoreDev((s) => s.enabled);
  const { value, setPlot, isDeveloper } = useStatsHouseShallow(
    ({ params: { plots }, setPlot, user: { developer } }) => ({
      value: plots[plotKey]?.backendVersion ?? defaultBackendVersion,
      isDeveloper: developer,
      setPlot,
    })
  );
  const onChange = useCallback(
    (status: boolean) => {
      setPlot(plotKey, (s) => {
        s.backendVersion = status ? METRIC_VALUE_BACKEND_VERSION.v2 : METRIC_VALUE_BACKEND_VERSION.v1;
      });
    },
    [plotKey, setPlot]
  );
  const onChangeSelect = useCallback(
    (event: React.ChangeEvent<HTMLSelectElement>) => {
      const value = toMetricValueBackendVersion(event.currentTarget.value, METRIC_VALUE_BACKEND_VERSION.v2);
      setPlot(plotKey, (s) => {
        s.backendVersion = value;
      });
    },
    [plotKey, setPlot]
  );
  if (devMode && isDeveloper) {
    return (
      <select className="form-select" style={{ width: 70 }} value={value} onChange={onChangeSelect}>
        <option value={METRIC_VALUE_BACKEND_VERSION.v1}>v1</option>
        <option value={METRIC_VALUE_BACKEND_VERSION.v2}>v2</option>
        <option value={METRIC_VALUE_BACKEND_VERSION.v3}>v3</option>
      </select>
    );
  }
  return (
    <SwitchBox
      checked={value === METRIC_VALUE_BACKEND_VERSION.v2}
      disabled={globalSettings.disabled_v1}
      onChange={onChange}
    >
      <SVGLightning />
    </SwitchBox>
  );
}

export const PlotControlVersion = memo(_PlotControlVersion);
