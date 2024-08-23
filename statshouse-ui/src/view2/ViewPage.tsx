import React, { useState } from 'react';
import { useStatsHouseShallow } from 'store2';
import { Dashboard, TvModePanel } from 'components2';
import { useEmbedMessage } from 'hooks/useEmbedMessage';
import { ErrorMessages } from '../components';
import { PlotLayout } from '../components2/Plot/PlotLayout';
import { useErrorStore } from '../store';

export function ViewPage() {
  // const { params, activePlotMeta, activePlot, globalNumQueriesPlot } = useStore(selector, shallow);
  const { tvModeEnable, isEmbed, plotsLength, tabNum } = useStatsHouseShallow(
    ({ params: { plots, orderPlot, tabNum }, tvMode: { enable }, isEmbed }) => ({
      tvModeEnable: enable,
      isEmbed,
      plotsLength: orderPlot.length,

      tabNum,
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
          {+tabNum >= 0 && <PlotLayout className="py-3" />}
          {/*{params.tabNum >= 0 && (*/}
          {/*  <div className={`container-xl tab-pane show active ${params.tabNum >= 0 ? '' : 'hidden-dashboard'}`}>*/}
          {/*    <PlotLayout*/}
          {/*      embed={embed}*/}
          {/*      indexPlot={params.tabNum}*/}
          {/*      setParams={setPlotParams}*/}
          {/*      sel={activePlot}*/}
          {/*      meta={activePlotMeta}*/}
          {/*      numQueries={globalNumQueriesPlot}*/}
          {/*      setBaseRange={setBaseRange}*/}
          {/*    >*/}
          {/*      {params.plots.map((plot, index) => (*/}
          {/*        <PlotView*/}
          {/*          key={index}*/}
          {/*          indexPlot={index}*/}
          {/*          type={plot.type}*/}
          {/*          compact={!!embed}*/}
          {/*          embed={embed}*/}
          {/*          className={index === params.tabNum ? '' : 'hidden-dashboard'}*/}
          {/*          yAxisSize={yAxisSize}*/}
          {/*          dashboard={false}*/}
          {/*        />*/}
          {/*      ))}*/}
          {/*    </PlotLayout>*/}
          {/*  </div>*/}
          {/*)}*/}
          {/*{params.tabNum < 0 && <Dashboard embed={embed} yAxisSize={yAxisSize} />}*/}
        </div>
      </div>
    </div>
  );

  // return <DashboardWidget></DashboardWidget>;
}

export default ViewPage;
