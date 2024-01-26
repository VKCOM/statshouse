// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useEffect, useState } from 'react';
import { Dashboard, ErrorMessages, PlotLayout, PlotView } from '../components';
import { Store, useStore } from '../store';
import { shallow } from 'zustand/shallow';
import { useEmbedMessage } from '../hooks/useEmbedMessage';

const { setPlotParams, setBaseRange, setCompact } = useStore.getState();

export type ViewPageProps = {
  embed?: boolean;
  yAxisSize?: number;
};

const selector = ({ params, metricsMeta, globalNumQueriesPlot }: Store) => ({
  params,
  activePlot: params.plots[params.tabNum],
  activePlotMeta: metricsMeta[params.plots[params.tabNum]?.metricName ?? ''] ?? undefined,
  globalNumQueriesPlot,
});
export const ViewPage: React.FC<ViewPageProps> = ({ embed, yAxisSize = 54 }) => {
  const { params, activePlotMeta, activePlot, globalNumQueriesPlot } = useStore(selector, shallow);
  const [refPage, setRefPage] = useState<HTMLDivElement | null>(null);
  useEmbedMessage(refPage, embed);
  useEffect(() => {
    setCompact(!!embed);
  }, [embed]);

  useEffect(() => {
    if (params.tabNum >= 0 && !useStore.getState().dashboardLayoutEdit) {
      window.scrollTo(0, 0);
    }
  }, [params.tabNum]);

  if (params.plots.length === 0) {
    return <ErrorMessages />;
  }
  return (
    <div ref={setRefPage} className="d-flex flex-column flex-md-row">
      <div className={embed ? 'flex-grow-1' : 'flex-grow-1 pt-3 pb-3'}>
        <div className="tab-content position-relative">
          {params.tabNum >= 0 && (
            <div className={`container-xl tab-pane show active ${params.tabNum >= 0 ? '' : 'hidden-dashboard'}`}>
              <PlotLayout
                embed={embed}
                indexPlot={params.tabNum}
                setParams={setPlotParams}
                sel={activePlot}
                meta={activePlotMeta}
                numQueries={globalNumQueriesPlot}
                setBaseRange={setBaseRange}
              >
                {params.plots.map((plot, index) => (
                  <PlotView
                    key={index}
                    indexPlot={index}
                    type={plot.type}
                    compact={!!embed}
                    embed={embed}
                    className={index === params.tabNum ? '' : 'hidden-dashboard'}
                    yAxisSize={yAxisSize}
                    dashboard={false}
                  />
                ))}
              </PlotLayout>
            </div>
          )}
          {params.tabNum < 0 && <Dashboard embed={embed} yAxisSize={yAxisSize} />}
        </div>
      </div>
    </div>
  );
};
