// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { useStore } from '../store';
import { QueryParams } from '../../common/plotQueryParams';
import { encodeParams } from '../../url/queryParams';

export type LinkListStore = {
  links: Record<string, string>;
};

export const useLinkListStore = create<LinkListStore>()(
  immer(() => {
    let prevParams = useStore.getState().params;
    let defaultParams = useStore.getState().defaultParams;
    useStore.subscribe((state) => {
      if (prevParams !== state.params) {
        prevParams = state.params;
        defaultParams = state.defaultParams;
        useLinkListStore.setState((linksState) => {
          linksState.links = getLinks(prevParams, defaultParams);
        });
      }
    });
    const links = getLinks(prevParams, defaultParams);
    return {
      links,
    };
  })
);

export function getLinks(params: QueryParams, defaultParams: QueryParams): Record<string, string> {
  return {
    ...Object.fromEntries(
      params.plots.map((plot, indexPlot) => [indexPlot.toString(), getLinkById(indexPlot, params, defaultParams)])
    ),
    '-1': getLinkById(-1, params, defaultParams),
    '-2': getLinkById(-2, params, defaultParams),
  };
}

export function getLinkById(indexPlot: number, params: QueryParams, defaultParams: QueryParams) {
  const s = encodeParams({ ...params, tabNum: indexPlot }, defaultParams);
  return '?' + s.toString();
}
