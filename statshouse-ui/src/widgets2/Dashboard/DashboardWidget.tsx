import React, { useMemo } from 'react';
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
import css from './style.module.css';
import { PLOT_TYPE } from 'api/enum';
import { Link } from 'react-router-dom';
import { useStatsHouseShallow } from 'store2';

export function DashboardWidget() {
  const { groupPlots, params, plotsData, toggleGroupShow, plotsLink } = useStatsHouseShallow(
    ({ groupPlots, params, plotsData, toggleGroupShow, links: { plotsLink } }) => ({
      groupPlots,
      params,
      plotsData,
      toggleGroupShow,
      plotsLink,
    })
  );

  const { tabNum, plots, groups, orderGroup } = params;

  const activePlot = useMemo(() => plots[tabNum], [plots, tabNum]);
  const activePlotData = useMemo(() => plotsData[tabNum], [plotsData, tabNum]);
  // const activePlotsLink = useMemo(() => plotsLink[tabNum], [plotsLink, tabNum]);

  return (
    <Dashboard>
      {orderGroup.map((groupKey) => (
        <DashboardGroup key={groupKey} groupInfo={groups[groupKey]} toggleShow={toggleGroupShow}>
          {groupPlots[groupKey]?.map((plotKey) => (
            <DashboardPlot key={plotKey}>
              <div className={css.dashboardPlotHeader}>
                <Link to={plotsLink[plotKey]?.link ?? ''} className={css.dashboardPlotHeaderLink}>
                  <MetricName
                    metricName={plotsData[plotKey]?.metricName}
                    metricWhat={plotsData[plotKey]?.metricWhat}
                    className={'flex-grow-1 w-0'}
                  ></MetricName>
                </Link>
              </div>

              <PlotView className={css[`plotView_${plots[plotKey]?.type ?? '0'}`]} plotKey={plotKey}></PlotView>
              <PlotLegend></PlotLegend>
            </DashboardPlot>
          ))}
        </DashboardGroup>
      ))}
      {+tabNum > -1 && (
        <PlotPanel className={css.plotPanel}>
          <div className={css.plotPanelLeft}>
            <div className={css.plotPanelTop}>
              <MetricName metricName={activePlotData?.metricName} metricWhat={activePlotData?.metricWhat} />
            </div>
            <div className={css.plotPanelMiddle}>
              <PlotView className={css.plotViewFull} plotKey={tabNum}></PlotView>
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
