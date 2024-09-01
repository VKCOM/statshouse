import React, { useState } from 'react';
import { useStatsHouseShallow } from 'store2';
import { Dashboard, TvModePanel } from 'components2';
import { useEmbedMessage } from 'hooks/useEmbedMessage';
import { ErrorMessages } from '../components';
import { PlotLayout } from '../components2/Plot/PlotLayout';

export function ViewPage() {
  // const { params, activePlotMeta, activePlot, globalNumQueriesPlot } = useStore(selector, shallow);
  const { tvModeEnable, isEmbed, plotsLength, isPlot } = useStatsHouseShallow(
    ({ params: { orderPlot, tabNum }, tvMode: { enable }, isEmbed }) => ({
      tvModeEnable: enable,
      isEmbed,
      plotsLength: orderPlot.length,
      isPlot: +tabNum >= 0,
    })
  );
  const [refPage, setRefPage] = useState<HTMLDivElement | null>(null);

  // const live = useLiveModeStore((s) => s.live);
  // const disablesdLive = useStore(selectorDisabledLive);

  useEmbedMessage(refPage, isEmbed);

  // useEffect(() => {
  //   if (params.tabNum >= 0 && !useStore.getState().dashboardLayoutEdit) {
  //     window.scrollTo(0, 0);
  //   }
  // }, [params.tabNum]);

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
