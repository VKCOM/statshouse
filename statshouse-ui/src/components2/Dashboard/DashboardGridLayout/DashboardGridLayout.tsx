// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Layout, Layouts, Responsive, WidthProvider } from 'react-grid-layout';
import cn from 'classnames';
import { memo, useCallback, useMemo, useRef, useState } from 'react';
import { GroupInfo } from '@/url2';
import { emptyArray, isNotNil } from '@/common/helpers';
import { StatsHouseStore, useStatsHouse } from '@/store2';
import { createSelector } from 'reselect';
import { ReactComponent as SVGPlus } from 'bootstrap-icons/icons/plus.svg';
import { getMapGroupPlotKeys } from '@/common/migrate/migrate3to4';
import {
  AVAILABLE_HANDLES,
  BREAKPOINT,
  BREAKPOINTS,
  BREAKPOINTS_LIST,
  COLS,
  CONTAINER_PADDING,
  LAYOUT_COLUMNS,
  LAYOUT_WIDGET_SIZE,
  MARGIN,
  ROW_HEIGHTS,
  toBreakpoints,
} from '@/components2/Dashboard/DashboardGridLayout/constant';
import 'react-grid-layout/css/styles.css';
import 'react-resizable/css/styles.css';
import './style.css';
import { PlotWidget } from '@/components2/PlotWidgets/PlotWidget';
import css from '@/components2/Dashboard/style.module.css';
import { addDashboardGroup, setParams } from '@/store2/methods';
import { DashboardGroup } from '@/components2';
import { Button } from '@/components/UI';

const ReactGridLayout = WidthProvider(Responsive);
export type DashboardGridLayoutProps = {
  className?: string;
  grid?: GridLayout[];
  isDraggable: boolean;
  isResizable: boolean;
  isEmbed?: boolean;
  onChange?: (value: Layout[]) => void;
};

const selectDashboardGrid = createSelector(
  [
    (s: StatsHouseStore) => s.params.plots,
    (s: StatsHouseStore) => s.params.groups,
    (s: StatsHouseStore) => s.params.orderGroup,
  ],
  (plots, groups, orderGroup): GridLayout[] => {
    const mapPlotByGroup = getMapGroupPlotKeys(plots);
    return orderGroup
      .map(
        (groupKey): GridLayout | undefined =>
          groups[groupKey] && {
            groupInfo: groups[groupKey],
            children:
              mapPlotByGroup[groupKey]?.plotKeys
                .map(
                  (plotKey): Layout | undefined =>
                    plots[plotKey] && {
                      i: plotKey,
                      x: 1,
                      y: 1,
                      w: 1,
                      h: 1,
                      ...(plots[plotKey].layout ?? {}),
                      ...(LAYOUT_WIDGET_SIZE[plots[plotKey]?.type] ?? {}),
                    }
                )
                .filter(isNotNil) ?? [],
          }
      )
      .filter(isNotNil);
  }
);

type GridLayout = {
  groupInfo: GroupInfo;
  children: Layout[];
};

export const DashboardGridLayout = memo(function DashboardGridLayout({
  className,
  grid = emptyArray,
  isResizable,
  isDraggable,
  isEmbed,
  onChange,
}: DashboardGridLayoutProps) {
  return (
    <div className={cn(className, 'p-2')}>
      {grid.map(({ groupInfo, children }) => (
        <DashboardGroupLayout
          key={groupInfo.id}
          className={cn('')}
          groupInfo={groupInfo}
          items={children}
          isResizable={isResizable}
          isDraggable={isDraggable}
          isEmbed={isEmbed}
          onChange={onChange}
        />
      ))}
    </div>
  );
});

const selectorDashboardLayoutEdit = ({ dashboardLayoutEdit }: StatsHouseStore) => dashboardLayoutEdit;
const selectorIsEmbed = ({ isEmbed }: StatsHouseStore) => isEmbed;

type DashboardGridLayoutHOCProps = {
  className?: string;
};
export function DashboardGridLayoutHOC({ className }: DashboardGridLayoutHOCProps) {
  const grid = useStatsHouse(selectDashboardGrid);
  const dashboardLayoutEdit = useStatsHouse(selectorDashboardLayoutEdit);
  const isEmbed = useStatsHouse(selectorIsEmbed);
  const onChange = useCallback((value: Layout[]) => {
    setParams((params) => {
      value.forEach(({ i, x, y, w, h }) => {
        if (params.plots[i]) {
          params.plots[i].layout = {
            ...params.plots[i].layout,
            x,
            y,
            w,
            h,
          };
        }
      });
    });
  }, []);
  return (
    <div className={className}>
      <DashboardGridLayout
        isDraggable={dashboardLayoutEdit}
        isResizable={dashboardLayoutEdit}
        grid={grid}
        isEmbed={isEmbed}
        onChange={onChange}
      />
      {dashboardLayoutEdit && <ButtonAddGroup />}
    </div>
  );
}

export function ButtonAddGroup() {
  const onAddGroup = useCallback(() => {
    addDashboardGroup('');
  }, []);
  return (
    <div className="p-2 pb-5 text-center">
      <h6 className="border-bottom" />
      <div className="container-xl">
        <Button className="btn btn-outline-primary py-4 w-100" onClick={onAddGroup}>
          <SVGPlus /> Add new group
        </Button>
      </div>
    </div>
  );
}

export type DashboardGroupLayoutProps = {
  className?: string;
  groupInfo: GroupInfo;
  items: Layout[];
  isDraggable: boolean;
  isResizable: boolean;
  isEmbed?: boolean;
  onChange?: (value: Layout[]) => void;
};
export function DashboardGroupLayout({
  groupInfo,
  items,
  className,
  isDraggable,
  isResizable,
  isEmbed,
  onChange,
}: DashboardGroupLayoutProps) {
  const [breakpoint, setBreakpoint] = useState(BREAKPOINT);
  const [cols, setCols] = useState<number>(LAYOUT_COLUMNS);
  const colsRef = useRef<number>(LAYOUT_COLUMNS);
  const lockEdit = cols !== LAYOUT_COLUMNS;
  const onBreakpointChange = useCallback((newBreakpoint: string, newCols: number) => {
    setBreakpoint(toBreakpoints(newBreakpoint, BREAKPOINT));
    setCols(newCols);
    colsRef.current = newCols;
  }, []);
  const layouts = useMemo(
    () =>
      BREAKPOINTS_LIST.reduce((res, bp) => {
        res[bp] = items;
        return res;
      }, {} as Layouts),
    [items]
  );
  const rowHeight = useMemo<number>(() => ROW_HEIGHTS[breakpoint] ?? 100, [breakpoint]);

  const onLayoutChange = useCallback(
    (currentLayout: Layout[], _allLayouts: Layouts) => {
      if (colsRef.current === LAYOUT_COLUMNS) {
        onChange?.(currentLayout);
      }
    },
    [onChange]
  );

  return (
    <div className={cn(className)}>
      <DashboardGroup key={groupInfo.id} groupKey={groupInfo.id}>
        <ReactGridLayout
          breakpoints={BREAKPOINTS}
          layouts={layouts}
          compactType={'vertical'}
          cols={COLS}
          autoSize
          rowHeight={rowHeight}
          containerPadding={CONTAINER_PADDING}
          margin={MARGIN}
          onLayoutChange={onLayoutChange}
          isDraggable={!lockEdit && isDraggable}
          isResizable={!lockEdit && isResizable}
          verticalCompact
          resizeHandles={AVAILABLE_HANDLES}
          onBreakpointChange={onBreakpointChange}
        >
          {items.map((item) => (
            <div key={item.i} className={'plot-item d-flex'}>
              <PlotWidget
                className={cn(isDraggable && css.pointerEventsNone)}
                plotKey={item.i}
                isDashboard
                isEmbed={isEmbed}
              />
            </div>
          ))}
        </ReactGridLayout>
      </DashboardGroup>
    </div>
  );
}
