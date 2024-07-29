// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { To } from 'react-router-dom';
import { type TagKey, TimeRangeAbbrev } from 'api/enum';
import { type GroupKey, type PlotKey, type VariableKey } from 'url2';
import { type StoreSlice } from '../createStore';
import { defaultBaseRange } from '../constants';
import { StatsHouseStore } from '../statsHouseStore';
import { updatePlotsLink } from './updatePlotsLink';
import { mergeLeft } from 'common/helpers';
import { updatePlotVariablesLink } from './updatePlotVariablesLink';
import { updateGroupInfo } from './updateGroupInfo';

export type PlotVariablesLink = Partial<
  Record<
    TagKey,
    {
      variableKey: VariableKey;
      variableName: string;
    }
  >
>;

export type PlotLink = {
  link: To;
  singleLink: To;
};

export type PlotsInfoLinks = {
  plotsLink: Partial<Record<PlotKey, PlotLink>>;
  dashboardLink: To;
  dashboardOuterLink: To;
  dashboardSettingLink: To;
  addLink: To;
};

export type PlotsInfoStore = {
  links: PlotsInfoLinks;
  baseRange: TimeRangeAbbrev;
  groupPlots: Partial<Record<GroupKey, PlotKey[]>>;
  viewOrderPlot: PlotKey[];
  plotToGroupMap: Partial<Record<PlotKey, GroupKey>>;
  plotVariablesLink: Partial<Record<PlotKey, PlotVariablesLink>>;
  updatePlotsInfo(): void;
  setBaseRange(r: TimeRangeAbbrev): void;
};

export const plotsInfoStore: StoreSlice<StatsHouseStore, PlotsInfoStore> = (setState, getState, store) => {
  store.subscribe((state, prevState) => {
    if (state.params !== prevState.params || state.saveParams !== prevState.saveParams) {
      // getState().updatePlotsInfo();
    }
  });
  return {
    links: {
      plotsLink: {},
      dashboardLink: '',
      dashboardOuterLink: '',
      dashboardSettingLink: '',
      addLink: '',
    },
    baseRange: defaultBaseRange,
    viewOrderPlot: [],
    plotToGroupMap: {},
    plotVariablesLink: {},
    groupPlots: {},
    setBaseRange(r: TimeRangeAbbrev) {
      setState((s) => {
        s.baseRange = r;
      });
    },
    updatePlotsInfo() {
      setState((s) => {
        s.links = mergeLeft(s.links, updatePlotsLink(s.params, s.saveParams));
        const { groupPlots, plotToGroupMap, viewOrderPlot } = updateGroupInfo(s.params);
        s.groupPlots = mergeLeft(s.groupPlots, groupPlots);
        s.plotToGroupMap = mergeLeft(s.plotToGroupMap, plotToGroupMap);
        s.viewOrderPlot = mergeLeft(s.viewOrderPlot, viewOrderPlot);
        s.plotVariablesLink = mergeLeft(s.plotVariablesLink, updatePlotVariablesLink(s.params));
      });
      getState().updatePlotsData();
    },
  };
};
