// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useCallback, useMemo, useState } from 'react';
import { Responsive, WidthProvider } from 'react-grid-layout';
import type { Layout, Layouts } from 'react-grid-layout';
import { useStatsHouseShallow } from '@/store2';
import { Button } from '@/components/UI';
import { ReactComponent as SVGPlus } from 'bootstrap-icons/icons/plus.svg';
import { DashboardPlotWrapper } from './DashboardPlotWrapper';
import { PlotView } from '../Plot';
import { DashboardGroup } from '@/components2';
import { prepareItemsGroup } from '@/common/prepareItemsGroup';
import 'react-grid-layout/css/styles.css';
import 'react-resizable/css/styles.css';
import cn from 'classnames';
import { getNextGroupKey } from '@/store2/urlStore/updateParamsPlotStruct';
import { toNumber } from '@/common/helpers';
import css from './style.module.css';

const ResponsiveGridLayout = WidthProvider(Responsive);

const COLS = {
  lg: 12,
  md: 10,
  sm: 6,
  xs: 4,
  xxs: 2,
};

export type DashboardLayoutProps = {
  className?: string;
};

export const DashboardLayoutNew = memo(function DashboardLayoutNew({ className }: DashboardLayoutProps) {
  const { groups, orderGroup, orderPlot, dashboardLayoutEdit, isEmbed, addDashboardGroup, setNextDashboardSchemePlot } =
    useStatsHouseShallow(
      ({
        params: { groups, orderGroup, orderPlot },
        dashboardLayoutEdit,
        isEmbed,
        addDashboardGroup,
        setNextDashboardSchemePlot,
      }) => ({
        groups,
        orderGroup,
        orderPlot,
        dashboardLayoutEdit,
        isEmbed,
        addDashboardGroup,
        setNextDashboardSchemePlot,
      })
    );

  const [layouts, setLayouts] = useState({});
  const itemsGroup = prepareItemsGroup({ groups, orderGroup, orderPlot });

  const nextGroupKey = useMemo(() => getNextGroupKey({ orderGroup }), [orderGroup]);

  const [isDragging, setIsDragging] = useState(false);

  const onLayoutChange = useCallback((layout: Layout[], layouts: Layouts) => {
    // setLayouts(layouts);
    // if (dashboardLayoutEdit) {
    //   const formattedLayout = [
    //     {
    //       groupKey: nextGroupKey,
    //       plots: layout.map((item) => item.i),
    //     },
    //   ];
    //   setNextDashboardSchemePlot(formattedLayout);
    // }
  }, []);

  const onAddGroup = useCallback(
    (e: React.MouseEvent<HTMLButtonElement>) => {
      const groupKey = e.currentTarget.getAttribute('data-index-group') ?? '0';
      addDashboardGroup(groupKey);
    },
    [addDashboardGroup]
  );

  const onDragStart = useCallback(() => {
    setIsDragging(true);
  }, []);

  const onDragStop = useCallback(() => {
    setIsDragging(false);
  }, []);

  return (
    <div className="container-fluid">
      <div className={cn(dashboardLayoutEdit && 'dashboard-edit', className)}>
        {itemsGroup.map(({ groupKey, plots }) => (
          <DashboardGroup key={groupKey} groupKey={groupKey}>
            {groups[groupKey]?.show !== false && (
              <ResponsiveGridLayout
                className={cn(
                  'layout',
                  'd-flex flex-row flex-wrap',
                  (!plots.length && !dashboardLayoutEdit) || isEmbed ? 'pb-0' : 'pb-3',
                  toNumber(groups[groupKey]?.size ?? '2') != null ? 'container-xl' : null
                )}
                layouts={layouts}
                breakpoints={{ lg: 1200, md: 996, sm: 768, xs: 480, xxs: 0 }}
                cols={COLS}
                autoSize
                rowHeight={450} /// сделать динамическим
                isDraggable={dashboardLayoutEdit}
                isResizable={dashboardLayoutEdit}
                // compactType="horizontal"
                // preventCollision
                onLayoutChange={onLayoutChange}
                onDragStart={onDragStart}
                onDragStop={onDragStop}
              >
                {plots.map((plotKey, index) => (
                  <DashboardPlotWrapper
                    key={plotKey}
                    data-grid={{ w: 6, h: 1, x: (index % 2) * 6, y: 0 }} /// поправить здесь выстраивание в ряд
                    // data-grid={{ w: 6, h: 1, x: 0, y: 0 }}
                    className={cn('plot-item p-1', dashboardLayoutEdit && css.cursorMove)}
                  >
                    <PlotView
                      className={cn(dashboardLayoutEdit && css.pointerEventsNone)}
                      key={plotKey}
                      plotKey={plotKey}
                      isDashboard
                    />
                  </DashboardPlotWrapper>
                ))}
              </ResponsiveGridLayout>
            )}
          </DashboardGroup>
        ))}

        {dashboardLayoutEdit &&
          (isDragging ? (
            <div className="pb-5" data-group={nextGroupKey}>
              <h6 className="border-bottom"> </h6>
              <div className="text-center text-secondary py-4">Drop here for create new group</div>
            </div>
          ) : (
            <div className="pb-5 text-center container-xl" data-group={nextGroupKey}>
              <h6 className="border-bottom"> </h6>
              <Button
                className="btn btn-outline-primary py-4 w-100"
                data-index-group={nextGroupKey}
                onClick={onAddGroup}
              >
                <SVGPlus /> Add new group
              </Button>
            </div>
          ))}
      </div>
    </div>
  );
});
