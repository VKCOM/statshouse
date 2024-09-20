// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useStore } from '../statshouse';
import { encodeParams, getNewPlot, QueryParams } from '../../url/queryParams';
import { deepClone } from '../../common/helpers';
import { createStore } from '../createStore';
import { fixMessageTrouble } from '../../url/fixMessageTrouble';

export type LinkListStore = {
  links: Record<string, string>;
};

export const useLinkListStore = createStore<LinkListStore>((setState) => {
  let prevParams = useStore.getState().params;
  let defaultParams = useStore.getState().defaultParams;
  useStore.subscribe((state) => {
    if (prevParams !== state.params) {
      prevParams = state.params;
      defaultParams = state.defaultParams;
      setState((linksState) => {
        linksState.links = getLinks(prevParams, defaultParams);
      });
    }
  });
  const links = getLinks(prevParams, defaultParams);
  return {
    links,
  };
}, 'LinkListStore');

export function getLinks(params: QueryParams, defaultParams: QueryParams): Record<string, string> {
  return {
    ...Object.fromEntries(
      params.plots.map((plot, indexPlot) => [indexPlot.toString(), getLinkById(indexPlot, params, defaultParams)])
    ),
    '-1': getLinkById(-1, params, defaultParams),
    '-2': getLinkById(-2, params, defaultParams),
    [params.plots.length]: getAddNewPlotLink(params, defaultParams),
  };
}

export function getLinkById(indexPlot: number, params: QueryParams, defaultParams: QueryParams) {
  const s = encodeParams({ ...params, theme: undefined, tabNum: indexPlot }, defaultParams);
  return '?' + fixMessageTrouble(new URLSearchParams(s).toString());
}

export function getAddNewPlotLink(params: QueryParams, defaultParams: QueryParams) {
  let tabNum = params.tabNum < 0 ? params.plots.length - 1 : params.tabNum;
  const copyPlot = deepClone(params.plots[tabNum]) ?? getNewPlot();
  const groupInfo = params.dashboard?.groupInfo?.map((g) => ({ ...g }));
  if (groupInfo?.length) {
    groupInfo[groupInfo.length - 1].count++;
  }
  const s = encodeParams(
    {
      ...params,
      dashboard: { ...params.dashboard, groupInfo },
      theme: undefined,
      plots: [...params.plots, { ...copyPlot }],
      tabNum: params.plots.length,
    },
    defaultParams
  );
  return '?' + fixMessageTrouble(new URLSearchParams(s).toString());
}
