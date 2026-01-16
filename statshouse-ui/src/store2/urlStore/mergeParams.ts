// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { PlotParams, QueryParams, sortUniqueKeys } from '@/url2';
import { dequal } from 'dequal/lite';

export function mergeParams(target: QueryParams, value: QueryParams): QueryParams {
  if (target === value) {
    return target;
  }
  let changePlots = false;
  const nextPlots = Object.keys(value.plots).reduce(
    (res, pK) => {
      res[pK] = mergePlot(target.plots[pK], value.plots[pK]!);
      if (res[pK] !== target.plots[pK]) {
        changePlots = true;
      }
      return res;
    },
    { ...value.plots }
  );
  let changeGroups = !dequal(target.orderGroup, value.orderGroup);
  const nextGroups = value.orderGroup.reduce(
    (res, gK) => {
      if (dequal(target.groups[gK], value.groups[gK])) {
        res[gK] = target.groups[gK];
      } else {
        changeGroups = true;
      }
      return res;
    },
    { ...value.groups }
  );
  let changeVariables = !dequal(target.orderVariables, value.orderVariables);
  const nextVariables = value.orderVariables.reduce(
    (res, gK) => {
      if (dequal(target.variables[gK], value.variables[gK])) {
        res[gK] = target.variables[gK];
      } else {
        changeVariables = true;
      }
      return res;
    },
    { ...value.variables }
  );

  const nextTimeShifts = value.timeShifts.slice().sort();

  return {
    ...value,
    timeShifts: dequal(target.timeShifts, nextTimeShifts) ? target.timeShifts : nextTimeShifts,
    orderPlot: [],
    plots: changePlots ? nextPlots : target.plots,
    orderGroup: changeGroups ? value.orderGroup : target.orderGroup,
    groups: changeGroups ? nextGroups : target.groups,
    orderVariables: changeVariables ? value.orderVariables : target.orderVariables,
    variables: changeVariables ? nextVariables : target.variables,
  };
}

function mergePlot(target: PlotParams | undefined = undefined, value: PlotParams): PlotParams {
  if (!target) {
    return value;
  }
  if (dequal(target, value)) {
    return target;
  }
  return {
    ...value,
    groupBy: dequal(target.groupBy, value.groupBy) ? target.groupBy : sortUniqueKeys(value.groupBy),
    filterIn: dequal(target.filterIn, value.filterIn) ? target.filterIn : value.filterIn,
    filterNotIn: dequal(target.filterNotIn, value.filterNotIn) ? target.filterNotIn : value.filterNotIn,
    what: dequal(target.what, value.what) ? target.what : value.what.slice().sort(),
    events: dequal(target.events, value.events) ? target.events : sortUniqueKeys(value.events),
    eventsHide: dequal(target.eventsHide, value.eventsHide) ? target.eventsHide : sortUniqueKeys(value.eventsHide),
    eventsBy: dequal(target.eventsBy, value.eventsBy) ? target.eventsBy : sortUniqueKeys(value.eventsBy),
    yLock: dequal(target.yLock, value.yLock) ? target.yLock : value.yLock,
    layout: dequal(target.layout, value.layout) ? target.layout : value.layout,
  };
}
