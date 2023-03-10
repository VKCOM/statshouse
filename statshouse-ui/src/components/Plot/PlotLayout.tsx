// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { SetStateAction, useCallback } from 'react';
import { PlotControls } from './PlotControls';
import { metricMeta, querySelector } from '../../view/api';
import { promQLMetric, timeRangeAbbrev } from '../../view/utils';
import { MetricItem } from '../../hooks';
import { PlotControlsPromQL } from './PlotControlsPromQL';

export type PlotLayoutProps = {
  embed?: boolean;
  children: React.ReactNode;
  meta: metricMeta;
  indexPlot: number;
  metricsOptions: MetricItem[];
  sel: querySelector;
  setParams: (index: number, params: SetStateAction<querySelector>, forceReplace?: boolean) => void;
  setBaseRange: React.Dispatch<React.SetStateAction<timeRangeAbbrev>>;
  numQueries: number;
};

export const PlotLayout: React.FC<PlotLayoutProps> = ({
  children,
  embed = false,
  indexPlot,
  sel,
  setBaseRange,
  setParams,
  meta,
  metricsOptions,
  numQueries,
}) => {
  const setSel = useCallback(
    (param: SetStateAction<querySelector>, forceReplace?: boolean) => {
      setParams(indexPlot, param, forceReplace);
    },
    [indexPlot, setParams]
  );

  if (embed) {
    return <div className="my-2">{children}</div>;
  }
  return (
    <div className="row flex-wrap">
      <div className="mb-3 col-lg-7 col-xl-8">
        <div className="position-relative">{children}</div>
      </div>
      <div className="mb-3 col-lg-5 col-xl-4">
        {sel.metricName === promQLMetric ? (
          <PlotControlsPromQL
            indexPlot={indexPlot}
            setBaseRange={setBaseRange}
            sel={sel}
            setSel={setSel}
            meta={meta}
            numQueries={numQueries}
            metricsOptions={metricsOptions}
          />
        ) : (
          <PlotControls
            indexPlot={indexPlot}
            setBaseRange={setBaseRange}
            sel={sel}
            setSel={setSel}
            meta={meta}
            numQueries={numQueries}
            metricsOptions={metricsOptions}
          />
        )}
      </div>
    </div>
  );
};
