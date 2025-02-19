// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useMemo } from 'react';
import type { PlotControlProps } from './PlotControl';
import { PLOT_TYPE, TAG_KEY } from '@/api/enum';
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
import { filterHasTagID } from '@/store2/helpers';
import { useGlobalLoader } from '@/store2/plotQueryStore';
import { isTagEnabled } from '@/view/utils2';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useMetricMeta } from '@/hooks/useMetricMeta';
import { useMetricName } from '@/hooks/useMetricName';

export function PlotControlFilter({ className }: PlotControlProps) {
  const {
    plot: { type, filterIn, filterNotIn, groupBy },
  } = useWidgetPlotContext();

  const metricName = useMetricName(true);
  const meta = useMetricMeta(metricName);
  const globalLoader = useGlobalLoader();

  const filterInfo = useMemo(() => ({ filterIn, filterNotIn, groupBy }), [filterIn, filterNotIn, groupBy]);

  return (
    <div className={className}>
      <div className="d-flex flex-column gap-2">
        <div className="d-flex flex-row gap-3">
          <PlotControlMetricName />
          {type === PLOT_TYPE.Metric && <PlotControlPromQLSwitch />}
        </div>
        {globalLoader && (
          <div className="text-center">
            <div className="text-info spinner-border spinner-border-sm m-5" role="status" aria-hidden="true" />
          </div>
        )}
        {!!meta && (
          <>
            <div className="d-flex flex-row mb-1 gap-4 w-100">
              <PlotControlWhats />
              {type === PLOT_TYPE.Metric && <PlotControlMaxHost />}
            </div>

            <div className="d-flex flex-column gap-2">
              <div className="d-flex flex-row gap-1 w-100">
                <PlotControlFrom />
                {type === PLOT_TYPE.Metric && <PlotControlView />}
              </div>
              <PlotControlTo />
              <PlotControlGlobalTimeShifts className="w-100" />
              {type === PLOT_TYPE.Metric && <PlotControlEventOverlay className="input-group-sm" />}
            </div>
            <div className="d-flex flex-row gap-4 mb-1">
              <div className="d-flex flex-row gap-3 flex-grow-1">
                <PlotControlAggregation />
                <PlotControlNumSeries />
              </div>
              <PlotControlVersion />
            </div>

            <div className={'d-flex flex-column gap-3'}>
              {(meta?.tagsOrder || []).map((tagKey) =>
                !tagKey || (!isTagEnabled(meta, tagKey) && !filterHasTagID(filterInfo, tagKey)) ? null : (
                  <PlotControlFilterTag key={tagKey} tagKey={tagKey} />
                )
              )}
              {!isTagEnabled(meta, TAG_KEY._s) && !filterHasTagID(filterInfo, TAG_KEY._s) ? null : (
                <PlotControlFilterTag key={TAG_KEY._s} tagKey={TAG_KEY._s} />
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
