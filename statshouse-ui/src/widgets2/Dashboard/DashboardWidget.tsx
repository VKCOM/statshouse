import React, { useCallback, useMemo } from 'react';
import { Dashboard, DashboardGroup, DashboardPlot, MetricName, PlotPanel, PlotView } from 'components2';
import { GroupKey, setUrlStore, usePlotsInfoStore, useUrlStore } from '../../store2';
import { PlotLegend } from '../../components2/Plot/PlotLegend';

export function DashboardWidget() {
  const { groupPlots, orderGroup, plotsInfo } = usePlotsInfoStore();
  const groups = useUrlStore((s) => s.params.groups);
  const tabNum = useUrlStore((s) => s.params.tabNum);
  const plots = useUrlStore((s) => s.params.plots);
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

  return (
    <Dashboard>
      {orderGroup.map((groupKey) => (
        <DashboardGroup key={groupKey} groupInfo={groups[groupKey]} toggleShow={toggleGroupShow}>
          {groupPlots[groupKey]?.map((plotKey) => (
            <DashboardPlot key={plotKey} plotInfo={plotsInfo[plotKey]}>
              <PlotView plot={plots[plotKey]} plotInfo={plotsInfo[plotKey]}></PlotView>
              <PlotLegend></PlotLegend>
            </DashboardPlot>
          ))}
        </DashboardGroup>
      ))}
      {!!activePlotInfo && (
        <PlotPanel>
          <MetricName metricName={activePlotInfo.metricName} metricWhat={activePlotInfo.metricWhat} />
          <PlotView plot={activePlot} plotInfo={activePlotInfo}></PlotView>
          <PlotLegend></PlotLegend>
        </PlotPanel>
      )}
    </Dashboard>
  );
}
