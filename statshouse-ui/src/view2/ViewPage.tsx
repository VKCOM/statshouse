// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useEffect, useState } from 'react';
import { useStatsHouse, useStatsHouseShallow } from '@/store2';
import { Dashboard, TvModePanel } from '@/components2';
import { useEmbedMessage } from '@/hooks/useEmbedMessage';
import { ErrorMessages } from '@/components/ErrorMessages';
import { useTvModeStore } from '@/store2/tvModeStore';
import { PlotLayout } from '@/components2/Plot/PlotLayout';

export function ViewPage() {
  // const { params, activePlotMeta, activePlot, globalNumQueriesPlot } = useStore(selector, shallow);
  const tvModeEnable = useTvModeStore((s) => s.enable);
  const { isEmbed, plotsLength, isPlot, tabNum } = useStatsHouseShallow(
    ({ params: { orderPlot, tabNum }, isEmbed }) => ({
      isEmbed,
      plotsLength: orderPlot.length,
      isPlot: +tabNum >= 0,
      tabNum,
    })
  );
  const [refPage, setRefPage] = useState<HTMLDivElement | null>(null);

  useEmbedMessage(refPage, isEmbed);

  useEffect(() => {
    if (+tabNum >= 0 && !useStatsHouse.getState().dashboardLayoutEdit) {
      window.scrollTo(0, 0);
    }
  }, [tabNum]);
  if (plotsLength === 0) {
    return (
      <div className="w-100 p-2">
        <ErrorMessages />
      </div>
    );
  }
  return (
    <div ref={setRefPage} className="d-flex flex-column flex-md-row dashLayout w-100">
      <div className="flex-grow-1">
        {tvModeEnable && <TvModePanel className="position-fixed z-1000 top-0 end-0 pt-1 pe-1" />}
        <div className="position-relative">
          <Dashboard />
          {/*{isPlot && <PlotWidgetFull plotKey={tabNum} className="py-3" isEmbed={isEmbed} />}*/}
          {isPlot && <PlotLayout className="py-3" plotKey={tabNum} isEmbed={isEmbed} />}
        </div>
      </div>
    </div>
  );
}

export default ViewPage;
