// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import { type PlotControlProps } from './PlotControl';
import { ErrorMessages } from 'components';
import { isTagEnabled } from '../../../view/utils';
import { PLOT_TYPE, TAG_KEY } from '../../../api/enum';
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
import { getNewPlot } from 'url2';
import { useStatsHouse } from 'store2';
import { filterHasTagID } from 'store2/helpers';

const emptyPlot = getNewPlot();

export function PlotControlFilter({ className, plot = emptyPlot }: PlotControlProps) {
  const plotKey = plot?.id;
  const meta = useStatsHouse((s) => s.metricMeta[plot?.metricName]);
  // const meta = useMetricsStore((s) => s.meta[plot?.metricName ?? '']);
  const metaLoading = !meta;

  if (!plotKey) {
    return (
      <div className={className}>
        <ErrorMessages />
      </div>
    );
  }
  return (
    <div className={className}>
      <ErrorMessages />
      <form spellCheck="false">
        <div className="d-flex mb-2 gap-3">
          <div className="col input-group">
            <PlotControlMetricName plotKey={plotKey} />
          </div>
          {plot.type === PLOT_TYPE.Metric && <PlotControlPromQLSwitch plotKey={plotKey} />}
        </div>
        {!!meta && (
          <>
            <div className="row mb-3">
              <div className="d-flex gap-4">
                <PlotControlWhats plotKey={plot.id} />
                {plot.type === PLOT_TYPE.Metric && <PlotControlMaxHost plotKey={plotKey} />}
              </div>
            </div>

            <div className="row mb-2 align-items-baseline">
              <div className="d-flex align-items-baseline gap-1">
                <PlotControlFrom />
                {plot.type === PLOT_TYPE.Metric && <PlotControlView plotKey={plotKey} />}
              </div>
              <div className="align-items-baseline mt-2">
                <PlotControlTo />
              </div>
              <PlotControlGlobalTimeShifts className="w-100 mt-2" />
            </div>
            {plot.type === PLOT_TYPE.Metric && (
              <PlotControlEventOverlay plotKey={plotKey} className="input-group-sm mb-3" />
            )}
            <div className="mb-3 d-flex">
              <div className="d-flex me-4 gap-3 flex-grow-1">
                <PlotControlAggregation plotKey={plotKey} />
                <PlotControlNumSeries plotKey={plotKey} />
              </div>
              <PlotControlVersion plotKey={plotKey} />
            </div>
            {metaLoading && (
              <div className="text-center">
                <div className="text-info spinner-border spinner-border-sm m-5" role="status" aria-hidden="true" />
              </div>
            )}
            {!metaLoading && (
              <div>
                {(meta?.tagsOrder || []).map((tagKey) =>
                  !tagKey || (!isTagEnabled(meta, tagKey) && !filterHasTagID(plot, tagKey)) ? null : (
                    <PlotControlFilterTag key={tagKey} plotKey={plot.id} tagKey={tagKey} className="mb-3" />
                  )
                )}
                {!isTagEnabled(meta, TAG_KEY._s) && !filterHasTagID(plot, TAG_KEY._s) ? null : (
                  <PlotControlFilterTag key={TAG_KEY._s} plotKey={plot.id} tagKey={TAG_KEY._s} className="mb-3" />
                )}
              </div>
            )}
          </>
        )}
      </form>
    </div>
  );
}
