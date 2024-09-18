// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useState } from 'react';
import { ToggleButton } from 'components/UI';
import {
  defaultInterval,
  selectorDisabledLive,
  setIntervalTVMode,
  setLiveMode,
  Store,
  toggleEnableTVMode,
  useLiveModeStore,
  useStore,
  useTVModeStore,
} from '../store';
import { shallow } from 'zustand/shallow';
import { useEmbedMessage } from '../hooks/useEmbedMessage';
import { ReactComponent as SVGFullscreen } from 'bootstrap-icons/icons/fullscreen.svg';
import { ReactComponent as SVGFullscreenExit } from 'bootstrap-icons/icons/fullscreen-exit.svg';
import { ReactComponent as SVGPlayFill } from 'bootstrap-icons/icons/play-fill.svg';
import { toNumber } from '../common/helpers';
import { Dashboard, PlotLayout, PlotView } from '../components';
import { ErrorMessages } from '../components/ErrorMessages';

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

  const { enable: tvMode, interval: tvModeInterval } = useTVModeStore((state) => state);

  const tvModeIntervalChange = useCallback<React.ChangeEventHandler<HTMLSelectElement>>((event) => {
    const value = toNumber(event.currentTarget.value, defaultInterval);
    setIntervalTVMode(value);
  }, []);

  const live = useLiveModeStore((s) => s.live);
  const disabledLive = useStore(selectorDisabledLive);

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
    <div ref={setRefPage} className="d-flex flex-column flex-md-row dashLayout">
      <div className="flex-grow-1">
        {tvMode && (
          <div className="position-fixed z-1000 top-0 end-0 pt-1 pe-1 ">
            <div className="input-group input-group-sm">
              <ToggleButton
                className="btn btn-outline-primary btn-sm rounded-start-1"
                title="Follow live"
                checked={live}
                onChange={setLiveMode}
                disabled={disabledLive}
              >
                <SVGPlayFill />
              </ToggleButton>
              <select className="form-select" value={tvModeInterval} onChange={tvModeIntervalChange}>
                <option value="0">none</option>
                <option value="5000">5 sec.</option>
                <option value="10000">10 sec.</option>
                <option value="15000">15 sec.</option>
                <option value="20000">20 sec.</option>
                <option value="30000">30 sec.</option>
                <option value="45000">45 sec.</option>
                <option value="60000">60 sec.</option>
                <option value="120000">2 min.</option>
                <option value="300000">6 min.</option>
              </select>
              <ToggleButton
                className="btn btn-outline-primary btn-sm"
                checked={tvMode}
                onChange={toggleEnableTVMode}
                title={tvMode ? 'TV mode Off' : 'TV mode On'}
              >
                {tvMode ? <SVGFullscreenExit /> : <SVGFullscreen />}
              </ToggleButton>
            </div>
          </div>
        )}
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
