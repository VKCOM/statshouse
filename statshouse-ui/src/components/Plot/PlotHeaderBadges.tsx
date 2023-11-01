// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
import React, { useMemo } from 'react';
import { selectorMetricsMetaByName, selectorParamsPlotsByIndex, selectorParamsTagSync, useStore } from '../../store';
import { formatTagValue } from '../../view/api';
import cn from 'classnames';
import { toKeyTag } from '../../url/queryParams';
import { Tooltip } from '../UI';

export type PlotHeaderBadgesProps = { indexPlot: number; compact?: boolean; dashboard?: boolean; className?: string };

export function PlotHeaderBadges({ indexPlot, compact, className }: PlotHeaderBadgesProps) {
  const selectorParamsPlot = useMemo(() => selectorParamsPlotsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const params = useStore(selectorParamsPlot);
  const selectorActivePlotMetricsMeta = useMemo(
    () => selectorMetricsMetaByName.bind(undefined, params.metricName ?? ''),
    [params.metricName]
  );
  const meta = useStore(selectorActivePlotMetricsMeta);
  const syncTag = useStore(selectorParamsTagSync);
  const filters = useMemo(
    () =>
      (meta?.tags || [])
        .map((t, index) => {
          const tagKey = toKeyTag(index);
          return {
            title: t.description,
            in: ((tagKey && params.filterIn[tagKey]) || [])
              .map((value) => formatTagValue(value, t?.value_comments?.[value], t.raw, t.raw_kind))
              .join(', '),
            notIn: ((tagKey && params.filterNotIn[tagKey]) || [])
              .map((value) => formatTagValue(value, t?.value_comments?.[value], t.raw, t.raw_kind))
              .join(', '),
          };
        })
        .filter((f, index) => (f.in || f.notIn) && !syncTag.some((group) => group[indexPlot] === index)),
    [indexPlot, meta?.tags, params.filterIn, params.filterNotIn, syncTag]
  );

  return (
    <>
      {meta?.resolution !== undefined && meta?.resolution !== 1 && (
        <Tooltip<'span'>
          as="span"
          className={cn(
            className,
            'badge',
            meta?.resolution && params.customAgg > 0 && meta?.resolution > params.customAgg
              ? 'bg-danger'
              : 'bg-warning text-black'
          )}
          title="Custom resolution"
        >
          {meta?.resolution}s
        </Tooltip>
      )}
      {!params.useV2 && <span className={cn(className, 'badge bg-danger')}>legacy data, production only</span>}
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
