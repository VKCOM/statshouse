// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useMemo } from 'react';
import { type PlotControlProps } from './PlotControl';
import { PLOT_TYPE, TAG_KEY, type TagKey } from 'api/enum';
import { PlotControlFrom } from './PlotControlFrom';
import { PlotControlTo } from './PlotControlTo';
import { PlotControlGlobalTimeShifts } from './PlotControlGlobalTimeShifts';
import { PlotControlAggregation } from './PlotControlAggregation';
import { PlotControlNumSeries } from './PlotControlNumSeries';
import { PlotControlVersion } from './PlotControlVersion';
import { PlotControlMaxHost } from './PlotControlMaxHost';
import { PlotControlWhats } from './PlotControlWhats';
import { PlotControlView } from './PlotControlView';
import { PlotControlPromQLSwitch } from './PlotControlPromQLSwitch';
import { PlotControlFilterTag } from './PlotControlFilterTag';
import { PlotControlMetricName } from './PlotControlMetricName';
import { PlotControlEventOverlay } from './PlotControlEventOverlay';
import { useStatsHouseShallow } from 'store2';
import { filterHasTagID } from 'store2/helpers';
import { useGlobalLoader } from 'store2/plotQueryStore';
import { isTagEnabled } from 'view/utils2';

const emptyFilter: Partial<Record<TagKey, string[]>> = {};
const emptyGroup: TagKey[] = [];

export function PlotControlFilter({ className, plotKey }: PlotControlProps) {
  const globalLoader = useGlobalLoader();
  const { meta, plotType, filterIn, filterNotIn, groupBy } = useStatsHouseShallow(
    ({ params: { plots }, metricMeta }) => ({
      plot: plots[plotKey],
      plotType: plots[plotKey]?.type ?? PLOT_TYPE.Metric,
      filterIn: plots[plotKey]?.filterIn ?? emptyFilter,
      filterNotIn: plots[plotKey]?.filterNotIn ?? emptyFilter,
      groupBy: plots[plotKey]?.groupBy ?? emptyGroup,
      meta: metricMeta[plots[plotKey]?.metricName ?? ''],
    })
  );
  const filterInfo = useMemo(() => ({ filterIn, filterNotIn, groupBy }), [filterIn, filterNotIn, groupBy]);

  return (
    <div className={className}>
      <div className="d-flex flex-column gap-2">
        <div className="d-flex flex-row gap-3">
          {/*<div className="d-flex flex-row input-group">*/}
          <PlotControlMetricName plotKey={plotKey} />
          {/*</div>*/}
          {plotType === PLOT_TYPE.Metric && <PlotControlPromQLSwitch plotKey={plotKey} />}
        </div>
        {globalLoader && (
          <div className="text-center">
            <div className="text-info spinner-border spinner-border-sm m-5" role="status" aria-hidden="true" />
          </div>
        )}
        {!!meta && (
          <>
            <div className="d-flex flex-row mb-1 gap-4 w-100">
              <PlotControlWhats plotKey={plotKey} />
              {plotType === PLOT_TYPE.Metric && <PlotControlMaxHost plotKey={plotKey} />}
            </div>

            <div className="d-flex flex-column gap-2">
              <div className="d-flex flex-row gap-1 w-100">
                <PlotControlFrom />
                {plotType === PLOT_TYPE.Metric && <PlotControlView plotKey={plotKey} />}
              </div>
              <PlotControlTo />
              <PlotControlGlobalTimeShifts className="w-100" />
              {plotType === PLOT_TYPE.Metric && (
                <PlotControlEventOverlay plotKey={plotKey} className="input-group-sm" />
              )}
            </div>
            <div className="d-flex flex-row gap-4 mb-1">
              <div className="d-flex flex-row gap-3 flex-grow-1">
                <PlotControlAggregation plotKey={plotKey} />
                <PlotControlNumSeries plotKey={plotKey} />
              </div>
              <PlotControlVersion plotKey={plotKey} />
            </div>

            <div className={'d-flex flex-column gap-3'}>
              {(meta?.tagsOrder || []).map((tagKey) =>
                !tagKey || (!isTagEnabled(meta, tagKey) && !filterHasTagID(filterInfo, tagKey)) ? null : (
                  <PlotControlFilterTag key={tagKey} plotKey={plotKey} tagKey={tagKey} />
                )
              )}
              {!isTagEnabled(meta, TAG_KEY._s) && !filterHasTagID(filterInfo, TAG_KEY._s) ? null : (
                <PlotControlFilterTag key={TAG_KEY._s} plotKey={plotKey} tagKey={TAG_KEY._s} />
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
