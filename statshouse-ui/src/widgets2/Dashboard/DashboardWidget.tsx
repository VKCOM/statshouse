import React, { useCallback, useMemo } from 'react';
import {
  Dashboard,
  DashboardGroup,
  DashboardPlot,
  MetricName,
  PlotControl,
  PlotLegend,
  PlotPanel,
  PlotView,
} from 'components2';
import { type GroupKey, setUrlStore, usePlotsDataStore, usePlotsInfoStore, useUrlStore } from 'store2';
import css from './style.module.css';
import { PLOT_TYPE } from 'api/enum';
import { useShallow } from 'zustand/react/shallow';

export function DashboardWidget() {
  const { groupPlots, orderGroup, plotsInfo } = usePlotsInfoStore(
    useShallow((s) => ({ groupPlots: s.groupPlots, orderGroup: s.orderGroup, plotsInfo: s.plotsInfo }))
  );
  const params = useUrlStore((s) => s.params);
  const { tabNum, plots, groups } = params;
  const plotsData = usePlotsDataStore((s) => s.plotsData);
  const toggleGroupShow = useCallback((groupKey: GroupKey) => {
    setUrlStore((s) => {
      const group = s.params.groups[groupKey];
      if (group) {
        group.show = !group.show;
      }
    });
  }, []);
  const activePlotInfo = useMemo(() => plotsInfo[tabNum], [plotsInfo, tabNum]);
  const activePlot = useMemo(() => plots[tabNum], [plots, tabNum]);
  const activePlotData = useMemo(() => plotsData[tabNum], [plotsData, tabNum]);

  return (
    <Dashboard>
      {orderGroup.map((groupKey) => (
        <DashboardGroup key={groupKey} groupInfo={groups[groupKey]} toggleShow={toggleGroupShow}>
          {groupPlots[groupKey]?.map((plotKey) => (
            <DashboardPlot key={plotKey} plotInfo={plotsInfo[plotKey]}>
              <PlotView
                className={css[`plotView_${plots[plotKey]?.type ?? '0'}`]}
                plot={plots[plotKey]}
                plotInfo={plotsInfo[plotKey]}
                plotData={plotsData[plotKey]}
              ></PlotView>
              <PlotLegend></PlotLegend>
            </DashboardPlot>
          ))}
        </DashboardGroup>
      ))}
      {+tabNum > -1 && (
        <PlotPanel className={css.plotPanel}>
          <div className={css.plotPanelLeft}>
            <div className={css.plotPanelTop}>
              <MetricName metricName={activePlotInfo?.metricName} metricWhat={activePlotInfo?.metricWhat} />
            </div>
            <div className={css.plotPanelMiddle}>
              <PlotView
                className={css.plotViewFull}
                plot={activePlot}
                plotInfo={activePlotInfo}
                plotData={activePlotData}
              ></PlotView>
            </div>
            <div className={css.plotPanelBottom}>
              {activePlot?.type === PLOT_TYPE.Metric && <PlotLegend></PlotLegend>}
            </div>
          </div>
          <div className={css.plotPanelRight}>
            <PlotControl className={css.plotControl} plot={activePlot}></PlotControl>
          </div>
        </PlotPanel>
      )}
    </Dashboard>
  );
}
