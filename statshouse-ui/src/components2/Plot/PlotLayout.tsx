// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo } from 'react';
import { PlotControl } from './PlotControl';
import cn from 'classnames';
import { useStatsHouseShallow } from 'store2';
import css from './style.module.css';
import { PlotView } from './PlotView';
import { isPromQL } from 'store2/helpers';

export type PlotLayoutProps = {
  className?: string;
};
export function _PlotLayout({ className }: PlotLayoutProps) {
  const { tabNum, plotDataPromqlExpand, isEmbed } = useStatsHouseShallow(
    ({ params: { tabNum, plots }, plotsData, isEmbed }) => ({
      tabNum,
      plotDataPromqlExpand: (isPromQL(plots[tabNum]) && plotsData[tabNum]?.promqlExpand) ?? false,
      isEmbed,
    })
  );

  if (isEmbed) {
    return (
      <div className={className}>
        <PlotView plotKey={tabNum} />
      </div>
    );
  }
  return (
    <div className={cn('container-xl', className)}>
      <div className="row">
        <div
          className={cn(
            css.plotColumn,
            'position-relative col col-12',
            plotDataPromqlExpand ? 'col-lg-5 col-xl-4' : 'col-lg-7 col-xl-8'
          )}
        >
          {/*<div className="position-relative flex-grow-1 d-flex flex-column">*/}
          <PlotView plotKey={tabNum} />
          {/*</div>*/}
        </div>
        <div className={cn('col col-12', plotDataPromqlExpand ? 'col-lg-7 col-xl-8' : 'col-lg-5 col-xl-4')}>
          <PlotControl plotKey={tabNum} />
        </div>
      </div>
    </div>
  );
}
export const PlotLayout = memo(_PlotLayout);
