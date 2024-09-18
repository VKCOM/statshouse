// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { SetStateAction, useCallback, useState } from 'react';
import { PlotControls } from './PlotControls';

import { PlotControlsPromQL } from './PlotControlsPromQL';
import cn from 'classnames';
import css from './style.module.css';
import { MetricMetaValue } from '../../api/metric';
import { PlotParams } from '../../url/queryParams';
import { promQLMetric } from '../../view/promQLMetric';
import { timeRangeAbbrev } from '../../view/utils2';

export type PlotLayoutProps = {
  embed?: boolean;
  children: React.ReactNode;
  meta?: MetricMetaValue;
  indexPlot: number;
  sel: PlotParams;
  setParams: (index: number, params: SetStateAction<PlotParams>, forceReplace?: boolean) => void;
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
  numQueries,
}) => {
  const [bigControl, setBigControl] = useState(false);
  const setSel = useCallback(
    (param: SetStateAction<PlotParams>, forceReplace?: boolean) => {
      setParams(indexPlot, param, forceReplace);
    },
    [indexPlot, setParams]
  );
  const toggleBigControl = useCallback(() => {
    setBigControl((s) => !s);
  }, []);

  const big = sel.metricName === promQLMetric && bigControl;

  if (embed) {
    return <div className="my-2">{children}</div>;
  }
  return (
    <div className="row flex-wrap my-3">
      <div className={cn(css.plotColumn, 'position-relative mb-3', big ? 'col-lg-5 col-xl-4' : 'col-lg-7 col-xl-8')}>
        <div className="position-relative flex-grow-1 d-flex flex-column">{children}</div>
      </div>
      <div className={cn('mb-3', big ? 'col-lg-7 col-xl-8' : 'col-lg-5 col-xl-4')}>
        {sel.metricName === promQLMetric ? (
          <PlotControlsPromQL
            key={indexPlot}
            indexPlot={indexPlot}
            setBaseRange={setBaseRange}
            sel={sel}
            setSel={setSel}
            meta={meta}
            numQueries={numQueries}
            bigControl={bigControl}
            toggleBigControl={toggleBigControl}
          />
        ) : (
          <PlotControls
            key={indexPlot}
            indexPlot={indexPlot}
            setBaseRange={setBaseRange}
            meta={meta}
            numQueries={numQueries}
          />
        )}
      </div>
    </div>
  );
};
