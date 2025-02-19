// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
import React, { useMemo } from 'react';

import cn from 'classnames';

import { Tooltip } from '@/components/UI';
import { METRIC_VALUE_BACKEND_VERSION, toTagKey } from '@/api/enum';
import { formatTagValue } from '@/view/api';
import { PlotHeaderBadgeResolution } from './PlotHeaderBadgeResolution';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useMetricMeta } from '@/hooks/useMetricMeta';
import { useMetricName } from '@/hooks/useMetricName';

export type PlotHeaderBadgesProps = { compact?: boolean; dashboard?: boolean; className?: string };

export function PlotHeaderBadges({ compact, className }: PlotHeaderBadgesProps) {
  const { plot } = useWidgetPlotContext();
  const meta = useMetricMeta(useMetricName(true));

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
        .filter((f) => f.in || f.notIn),
    [meta?.tags, plot.filterIn, plot.filterNotIn]
  );

  return (
    <>
      <PlotHeaderBadgeResolution resolution={meta?.resolution} customAgg={plot.customAgg} className={className} />
      {plot.backendVersion === METRIC_VALUE_BACKEND_VERSION.v1 && (
        <span className={cn(className, 'badge bg-danger')}>legacy data, production only</span>
      )}
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
