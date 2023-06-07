// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useMemo } from 'react';
import { TagControl } from '../../view/TagControl';
import { selectorMetricsMeta, selectorParamsPlots, selectorParamsTagSync, useStore } from '../../store';
import { MetricMetaTag } from '../../api/metric';

export type DashboardTagControlProps = { className?: string };
export const DashboardTagControl: React.FC<DashboardTagControlProps> = ({ className }) => {
  const tagsSync = useStore(selectorParamsTagSync);
  const metricsMeta = useStore(selectorMetricsMeta);
  const plots = useStore(selectorParamsPlots);

  const tagsList = useMemo<{ tag: MetricMetaTag; indexPlot: number; indexTag: number }[]>(
    () =>
      tagsSync
        .map((group) => {
          const indexPlot = group.findIndex((s) => s !== null);
          const indexTag = group[indexPlot];
          if (indexPlot < 0 || indexTag === null) {
            return null;
          }
          const tag = metricsMeta[plots[indexPlot]?.metricName]?.tags?.[indexTag];
          if (!tag) {
            return null;
          }
          return {
            tag,
            indexPlot,
            indexTag,
          };
        })
        .filter(Boolean) as { tag: MetricMetaTag; indexPlot: number; indexTag: number }[],
    [metricsMeta, plots, tagsSync]
  );

  return (
    <div className={className}>
      {tagsList.map((t, index) => (
        <div key={index}>
          <TagControl
            key={`key${index}`}
            tag={t.tag}
            indexTag={t.indexTag}
            indexPlot={t.indexPlot}
            tagID={`key${t.indexTag}`}
            sync={false}
            small={true}
            dashboard={true}
          />
        </div>
      ))}
    </div>
  );
};
