// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
import React, { useMemo } from 'react';

import cn from 'classnames';

import { Tooltip } from 'components/UI';
import { getNewPlot, type PlotKey, promQLMetric } from 'url2';
import { useStatsHouseShallow } from 'store2';
import { toTagKey } from 'api/enum';
import { formatTagValue } from 'view/api';

const emptyPlot = getNewPlot();

export type PlotHeaderBadgesProps = { plotKey: PlotKey; compact?: boolean; dashboard?: boolean; className?: string };

export function PlotHeaderBadges({ plotKey, compact, className }: PlotHeaderBadgesProps) {
  const { meta, plot } = useStatsHouseShallow(({ params: { plots }, plotsData, metricMeta }) => ({
    plot: plots[plotKey] ?? emptyPlot,
    meta: metricMeta[
      (plots[plotKey]?.metricName !== promQLMetric ? plots[plotKey]?.metricName : plotsData[plotKey]?.metricName) ?? ''
    ],
  }));
  const filters = useMemo(
    () =>
      (meta?.tags || [])
        .map((t, index) => {
          const tagKey = toTagKey(index);
          return {
            title: t.description,
            in: ((tagKey && plot.filterIn[tagKey]) || [])
              .map((value) => formatTagValue(value, t?.value_comments?.[value], t.raw, t.raw_kind))
              .join(', '),
            notIn: ((tagKey && plot.filterNotIn[tagKey]) || [])
              .map((value) => formatTagValue(value, t?.value_comments?.[value], t.raw, t.raw_kind))
              .join(', '),
          };
        })
        .filter((f, index) => f.in || f.notIn),
    [meta?.tags, plot.filterIn, plot.filterNotIn]
  );

  return (
    <>
      {meta?.resolution !== undefined && meta?.resolution !== 1 && (
        <Tooltip<'span'>
          as="span"
          className={cn(
            className,
            'badge',
            meta?.resolution && plot.customAgg > 0 && meta?.resolution > plot.customAgg
              ? 'bg-danger'
              : 'bg-warning text-black'
          )}
          title="Custom resolution"
        >
          {meta?.resolution}s
        </Tooltip>
      )}
      {!plot.useV2 && <span className={cn(className, 'badge bg-danger')}>legacy data, production only</span>}
      {compact && (
        <>
          {
            /*tag values selected*/
            filters.map((f, i) => (
              <React.Fragment key={i}>
                {f.in && (
                  <Tooltip<'span'>
                    as="span"
                    title={f.title}
                    className={cn(className, 'badge border border-success text-success font-normal fw-normal')}
                  >
                    {f.in}
                  </Tooltip>
                )}
                {f.notIn && (
                  <Tooltip<'span'>
                    as="span"
                    title={f.title}
                    className={cn(className, 'badge border border-danger text-danger font-normal fw-normal')}
                  >
                    {f.notIn}
                  </Tooltip>
                )}
              </React.Fragment>
            ))
          }
        </>
      )}
    </>
  );
}
