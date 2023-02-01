// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useMemo } from 'react';
import { selectorIsServer, selectorSetParams, useStore } from '../../store';
import { NavLink, To } from 'react-router-dom';
import { QueryParams } from '../../common/plotQueryParams';
import produce from 'immer';
import { usePlotLink } from '../../hooks';

export type PlotLinkProps = {
  indexPlot?: number;
  to?: To;
} & Omit<React.AnchorHTMLAttributes<HTMLAnchorElement>, 'href'> &
  React.RefAttributes<HTMLAnchorElement>;

export const PlotLink: React.ForwardRefExoticComponent<PlotLinkProps> = React.forwardRef<
  HTMLAnchorElement,
  PlotLinkProps
>(function _PlotLink({ indexPlot, to, children, ...attributes }, ref) {
  const isServer = useStore(selectorIsServer);
  const setParams = useStore(selectorSetParams);
  const plotSearchFn = useMemo<(value: QueryParams) => QueryParams>(
    () =>
      produce((p) => {
        if (indexPlot !== undefined && p.plots.length > indexPlot) {
          p.tabNum = indexPlot;
        } else if (p.plots.length && p.plots.length === indexPlot) {
          const cloneId = p.tabNum < 0 ? p.plots.length - 1 : p.tabNum;
          p.plots.push({
            metricName: p.plots[cloneId].metricName,
            what: p.plots[cloneId].what.slice(),
            customAgg: p.plots[cloneId].customAgg,
            groupBy: p.plots[cloneId].groupBy.slice(),
            filterIn: Object.fromEntries(
              Object.entries(p.plots[cloneId].filterIn).map(([key, value]) => [key, value.slice()])
            ),
            filterNotIn: Object.fromEntries(
              Object.entries(p.plots[cloneId].filterNotIn).map(([key, value]) => [key, value.slice()])
            ),
            numSeries: p.plots[cloneId].numSeries,
            timeShifts: p.plots[cloneId].timeShifts.slice(),
            useV2: p.plots[cloneId].useV2,
            yLock: {
              min: 0,
              max: 0,
            },
          });
          if (p.dashboard?.groupInfo?.length) {
            p.dashboard.groupInfo[p.dashboard.groupInfo.length - 1].count++;
          }
          p.tabNum = p.plots.length - 1;
        }
      }),
    [indexPlot]
  );
  const plotSearch = usePlotLink(plotSearchFn);
  const onClick = useCallback(() => {
    setParams(plotSearchFn, false, true);
  }, [plotSearchFn, setParams]);

  if (isServer && !to) {
    return (
      <span role="button" onClick={onClick} ref={ref} {...attributes}>
        {children}
      </span>
    );
  }

  return (
    <NavLink to={to ?? plotSearch} end {...attributes} ref={ref}>
      {children}
    </NavLink>
  );
});
