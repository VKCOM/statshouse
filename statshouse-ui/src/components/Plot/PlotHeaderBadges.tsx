// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
import React, { useMemo } from 'react';
import { selectorMetricsMetaByName, selectorParamsPlotsByIndex, selectorParamsTagSync, useStore } from '../../store';
import { formatTagValue } from '../../view/api';
import cn from 'classnames';

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
      (meta.tags || [])
        .map((t, index) => ({
          title: t.description,
          in: (params.filterIn[`key${index}`] || [])
            .map((value) => formatTagValue(value, t?.value_comments?.[value], t.raw, t.raw_kind))
            .join(', '),
          notIn: (params.filterNotIn[`key${index}`] || [])
            .map((value) => formatTagValue(value, t?.value_comments?.[value], t.raw, t.raw_kind))
            .join(', '),
        }))
        .filter((f, index) => (f.in || f.notIn) && !syncTag.some((group) => group[indexPlot] === index)),
    [indexPlot, meta.tags, params.filterIn, params.filterNotIn, syncTag]
  );

  const syncedTags = useMemo(() => {
    const sTags = (meta.tags || [])
      .map((t, index) => ({
        title: t.description,
        in: (params.filterIn[`key${index}`] || [])
          .map((value) => formatTagValue(value, t?.value_comments?.[value], t.raw, t.raw_kind))
          .join(', '),
        notIn: (params.filterNotIn[`key${index}`] || [])
          .map((value) => formatTagValue(value, t?.value_comments?.[value], t.raw, t.raw_kind))
          .join(', '),
      }))
      .filter((f, index) => (f.in || f.notIn) && syncTag.some((group) => group[indexPlot] === index));
    return {
      in: sTags
        .filter((t) => t.in)
        .map((t) => `${t.title}: ${t.in}`)
        .join('\n'),
      notIn: sTags
        .filter((t) => t.notIn)
        .map((t) => `${t.title}: ${t.notIn}`)
        .join('\n'),
    };
  }, [indexPlot, meta.tags, params.filterIn, params.filterNotIn, syncTag]);

  return (
    <>
      {meta.resolution !== undefined && meta.resolution !== 1 && (
        <span
          className={cn(
            className,
            'badge',
            meta.resolution && params.customAgg > 0 && meta.resolution > params.customAgg
              ? 'bg-danger'
              : 'bg-warning text-black'
          )}
          title="Custom resolution"
        >
          {meta.resolution}s
        </span>
      )}
      {!params.useV2 && <span className={cn(className, 'badge bg-danger')}>legacy data, production only</span>}
      {compact && (
        <>
          {
            /*tag values selected*/
            filters.map((f, i) => (
              <React.Fragment key={i}>
                {f.in && (
                  <span
                    title={f.title}
                    className={cn(className, 'badge border border-success text-success font-normal fw-normal')}
                  >
                    {f.in}
                  </span>
                )}
                {f.notIn && (
                  <span
                    title={f.title}
                    className={cn(className, 'badge border border-danger text-danger font-normal fw-normal')}
                  >
                    {f.notIn}
                  </span>
                )}
              </React.Fragment>
            ))
          }
          {syncedTags.in && (
            <span
              title={syncedTags.in}
              className={cn(className, 'badge border border-success text-success font-normal fw-normal')}
            >
              synced
            </span>
          )}
          {syncedTags.notIn && (
            <span
              title={syncedTags.notIn}
              className={cn(className, 'badge border border-danger text-danger font-normal fw-normal')}
            >
              synced
            </span>
          )}
        </>
      )}
    </>
  );
}
