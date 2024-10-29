// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { getNewMetric, getNewGroup, type PlotKey, type QueryParams, urlEncode } from 'url2';
import { produce } from 'immer';
import { clonePlot } from 'url2/clonePlot';
import { fixMessageTrouble } from 'url/fixMessageTrouble';

let localParams: QueryParams;
let localSaveParams: QueryParams;
let templateSaveFn: (plotKey: PlotKey) => string;

const plotKeyPh = '#$$$[pk]$$$#';

function createTemplateFn(params: QueryParams, saveParams?: QueryParams) {
  const link =
    '?' +
    new URLSearchParams(
      urlEncode(
        produce(params, (p) => {
          p.tabNum = plotKeyPh;
        }),
        saveParams
      )
    ).toString();
  const [f, p] = fixMessageTrouble(link).split(encodeURIComponent(plotKeyPh)).map(String);
  return (plotKey: PlotKey) => `${f}${plotKey}${p}`;
}

export function getPlotLink(plotKey: PlotKey, params: QueryParams, saveParams: QueryParams): string {
  if (localParams === params && saveParams === localSaveParams && !!templateSaveFn) {
    return templateSaveFn(plotKey);
  } else {
    localParams = params;
    localSaveParams = saveParams;
    return (templateSaveFn = createTemplateFn(params, saveParams))(plotKey);
  }
}

export function getFreePlot(plotKey: PlotKey, params: QueryParams) {
  if (params.plots[plotKey]) {
    const plot = clonePlot(params.plots[plotKey]!);
    params.orderVariables.forEach((vK) => {
      const variable = params.variables[vK];
      if (variable) {
        variable.link.forEach(([iPlot, keyTag]) => {
          if (iPlot === plotKey) {
            if (variable.negative) {
              plot.filterNotIn[keyTag] = [...variable.values];
            } else {
              plot.filterIn[keyTag] = [...variable.values];
            }
            if (variable.groupBy) {
              if (plot.groupBy.indexOf(keyTag) < 0) {
                plot.groupBy.push(keyTag);
              }
            } else {
              if (plot.groupBy.indexOf(keyTag) > -1) {
                plot.groupBy = plot.groupBy.filter((g) => g !== keyTag);
              }
            }
          }
        });
      }
    });
    return plot;
  }
  return getNewMetric();
}

export function getPlotSingleLink(plotKey: PlotKey, params: QueryParams): string {
  if (plotKey === '-1') {
    return (
      '?' +
      new URLSearchParams(
        urlEncode(
          produce(params, (p) => {
            p.tabNum = plotKey;
            p.dashboardId = undefined;
          })
        )
      ).toString()
    );
  }
  return (
    '?' +
    new URLSearchParams(
      urlEncode(
        produce(params, (p) => {
          const plot = { ...getFreePlot(plotKey, params), id: '0' };
          const plotEvents = plot.events.map((pK, index) => ({
            ...getFreePlot(pK, params),
            id: (index + 1).toString(),
          }));
          plot.events = plotEvents.map(({ id }) => id);
          p.live = false;
          p.theme = undefined;
          p.dashboardId = undefined;
          p.dashboardName = '';
          p.dashboardDescription = '';
          p.dashboardVersion = undefined;
          p.tabNum = plot.id;
          p.plots = {
            [plot.id]: plot,
          };
          p.orderPlot = [plot.id];
          plotEvents.forEach((pE) => {
            p.plots[pE.id] = pE;
            p.orderPlot.push(pE.id);
          });
          p.variables = {};
          p.orderVariables = [];
          p.groups = {
            '0': {
              ...getNewGroup(),
              id: '0',
              count: 1 + plotEvents.length,
            },
          };
          p.orderGroup = ['0'];
        })
      )
    ).toString()
  );
}
