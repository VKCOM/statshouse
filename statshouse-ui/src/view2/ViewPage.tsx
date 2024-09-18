import React, { useEffect, useState } from 'react';
import { useStatsHouse, useStatsHouseShallow } from 'store2';
import { Dashboard, TvModePanel } from 'components2';
import { useEmbedMessage } from 'hooks/useEmbedMessage';
import { ErrorMessages } from 'components/ErrorMessages';
import { PlotLayout } from 'components2/Plot/PlotLayout';
import { useTvModeStore } from '../store2/tvModeStore';

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
          {isPlot && <PlotLayout className="py-3" />}
        </div>
      </div>
    </div>
  );
}

export default ViewPage;
