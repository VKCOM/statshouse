// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect } from 'react';
import { TAG_KEY, TagKey, toTagKey } from 'api/enum';
import { PlotKey, promQLMetric } from 'url2';
import { useStatsHouseShallow } from 'store2';
import { getMetricFullName } from 'store2/helpers';
import { getTagDescription, isTagEnabled } from '../../../view/utils2';

export type VariablePlotLinkSelectProps = {
  plotKey: PlotKey;
  selectTag?: TagKey;
  onChange?: (plotKey: PlotKey, selectTag?: TagKey) => void;
};
export function VariablePlotLinkSelect({ plotKey, selectTag, onChange }: VariablePlotLinkSelectProps) {
  const { plot, plotData, metricName, metricMeta, loadMetricMeta } = useStatsHouseShallow(
    ({ params: { plots }, metricMeta, plotsData, loadMetricMeta }) => {
      const metricName =
        (plots[plotKey]?.metricName !== promQLMetric ? plots[plotKey]?.metricName : plotsData[plotKey]?.metricName) ??
        '';
      return {
        plot: plots[plotKey],
        plotData: plotsData[plotKey],
        metricName,
        metricMeta: metricMeta[metricName],
        loadMetricMeta,
      };
    }
  );
  const changeTag = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) => {
      const iTag = toTagKey(e.currentTarget.value) ?? undefined;
      onChange?.(plotKey, iTag);
    },
    [plotKey, onChange]
  );

  useEffect(() => {
    loadMetricMeta(metricName);
  }, [loadMetricMeta, metricName]);

  if (plot == null) {
    return null;
  }
  return (
    <tr>
      <td className="text-end pb-0 ps-0">{getMetricFullName(plot, plotData)}</td>
      <td className="pb-0 pe-0">
        {plot.metricName === promQLMetric ? (
          <div className="form-control form-control-sm text-secondary">promQL</div>
        ) : (
          <select className="form-select form-select-sm" value={selectTag?.toString() ?? 'null'} onChange={changeTag}>
            <option value="null">-</option>
            {metricMeta?.tags?.map((tag, indexTag) => {
              const keyTag = toTagKey(indexTag);
              return (
                keyTag != null &&
                isTagEnabled(metricMeta, keyTag) && (
                  <option key={indexTag} value={keyTag}>
                    {getTagDescription(metricMeta, keyTag)}
                  </option>
                )
              );
            })}
            {isTagEnabled(metricMeta, TAG_KEY._s) && (
              <option value={TAG_KEY._s}>{getTagDescription(metricMeta, TAG_KEY._s)}</option>
            )}
          </select>
        )}
      </td>
    </tr>
  );
}
