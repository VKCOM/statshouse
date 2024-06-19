// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { PlotKey, QueryParams } from 'url2';
import { viewPath } from '../constants';
import { getAddPlotLink, getPlotLink, getSinglePlotLink } from '../helpers';
import { isNotNil } from 'common/helpers';
import { PlotLink, PlotsInfoLinks } from './plotsInfoStore';

export function updatePlotsLink(params: QueryParams, saveParams?: QueryParams): PlotsInfoLinks {
  const dashboardLink = { pathname: viewPath, search: getPlotLink('-1', params, saveParams) };
  const dashboardOuterLink = { pathname: viewPath, search: getPlotLink('-1', params, saveParams) };
  const dashboardSettingLink = { pathname: viewPath, search: getPlotLink('-1', params, saveParams) };
  const addLink = { pathname: viewPath, search: getAddPlotLink(params, saveParams) };
  const plotsLink = Object.fromEntries(
    Object.entries(params.plots)
      .map(([plotKey, plot]): undefined | [PlotKey, PlotLink] => {
        if (!plot) {
          return undefined;
        }
        const link = { pathname: viewPath, search: getPlotLink(plot.id, params, saveParams) };
        const singleLink = { pathname: viewPath, search: getSinglePlotLink(plot.id, params) };
        return [plotKey, { link, singleLink }];
      })
      .filter(isNotNil)
  );
  return {
    dashboardLink,
    dashboardOuterLink,
    dashboardSettingLink,
    addLink,
    plotsLink,
  };
}
