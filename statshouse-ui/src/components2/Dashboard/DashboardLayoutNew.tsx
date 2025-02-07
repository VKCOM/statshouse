// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
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
import { GroupKey } from '@/url2';

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
  const [isDragging, setIsDragging] = useState(false);
  const [draggedPlotKey, setDraggedPlotKey] = useState<string | null>(null);
  const [draggedGroupKey, setDraggedGroupKey] = useState<string | null>(null);
  const [selectTargetGroup, setSelectTargetGroup] = useState<GroupKey | null>(null);

  const itemsGroup = useMemo(
    () => prepareItemsGroup({ groups, orderGroup, orderPlot }),
    [groups, orderGroup, orderPlot]
  );

  const itemsGroupRef = useRef(itemsGroup);

  const nextGroupKey = useMemo(() => getNextGroupKey({ orderGroup }), [orderGroup]);

  // Обновляем ref при изменении itemsGroup
  useEffect(() => {
    itemsGroupRef.current = itemsGroup;
  }, [itemsGroup]);

  const save = useCallback(
    (plotKey: string | null, targetGroup: GroupKey | null) => {
      if (plotKey != null && targetGroup != null) {
        const updatedItemsGroup = itemsGroupRef.current.map((group) => {
          if (group.groupKey === draggedGroupKey) {
            return {
              ...group,
              plots: group.plots.filter((p) => p !== plotKey),
            };
          }
          if (group.groupKey === targetGroup) {
            return {
              ...group,
              plots: [...group.plots, plotKey],
            };
          }
          return group;
        });

        // Если это новая группа
        if (!updatedItemsGroup.find((g) => g.groupKey === targetGroup)) {
          updatedItemsGroup.push({
            groupKey: targetGroup,
            plots: [plotKey],
          });
        }

        setNextDashboardSchemePlot(updatedItemsGroup);
      }
    },
    [draggedGroupKey, setNextDashboardSchemePlot]
  );

  const onDragStart = useCallback((layout: Layout[], oldItem: Layout) => {
    setIsDragging(true);
    const [groupKey, plotKey] = oldItem.i.split('::');
    setDraggedPlotKey(plotKey);
    setDraggedGroupKey(groupKey);
    setSelectTargetGroup(null);
  }, []);

  const onDragStop = useCallback(
    (layout: Layout[], oldItem: Layout, newItem: Layout, placeholder: Layout, e: MouseEvent, element: HTMLElement) => {
      setIsDragging(false);

      const dropElement = document.elementsFromPoint(e.clientX, e.clientY);
      const targetGroup = dropElement.find((e) => e.getAttribute('data-group'))?.getAttribute('data-group') ?? null;

      if (targetGroup && draggedPlotKey) {
        setSelectTargetGroup(targetGroup);
        save(draggedPlotKey, targetGroup);
      }

      setDraggedPlotKey(null);
      setDraggedGroupKey(null);
      setSelectTargetGroup(null);
    },
    [draggedPlotKey, save]
  );

  const onLayoutChange = useCallback(
    (layout: Layout[], layouts: Layouts) => {
      setLayouts(layouts);
      // Обрабатываем изменения layout только если это не перетаскивание между группами
      if (dashboardLayoutEdit && !isDragging && !selectTargetGroup) {
        const updatedItemsGroup = itemsGroup.map((group) => ({
          ...group,
          plots: layout.filter((item) => item.i.startsWith(`${group.groupKey}::`)).map((item) => item.i.split('::')[1]),
        }));
        setNextDashboardSchemePlot(updatedItemsGroup);
      }
    },
    [dashboardLayoutEdit, isDragging, itemsGroup, selectTargetGroup, setNextDashboardSchemePlot]
  );

  const onAddGroup = useCallback(
    (e: React.MouseEvent<HTMLButtonElement>) => {
      const groupKey = e.currentTarget.getAttribute('data-index-group') ?? '0';
      addDashboardGroup(groupKey);
    },
    [addDashboardGroup]
  );

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
                // onLayoutChange={onLayoutChange}
                onDragStart={onDragStart}
                onDragStop={onDragStop}
              >
                {plots.map((plotKey, index) => (
                  <DashboardPlotWrapper
                    key={`${groupKey}::${plotKey}`}
                    data-grid={{ w: 6, h: 1, x: (index % 2) * 6, y: 0 }}
                    className={cn(
                      'plot-item p-1',
                      dashboardLayoutEdit && css.cursorMove
                      // draggedPlotKey === plotKey && 'opacity-50'
                    )}
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

        {dashboardLayoutEdit && (
          <div className={cn('pb-5', isDragging ? '' : 'text-center container-xl')} data-group={nextGroupKey}>
            <h6 className="border-bottom"> </h6>
            {isDragging ? (
              <div className="text-center text-secondary py-4">Drop here for create new group</div>
            ) : (
              <Button
                className="btn btn-outline-primary py-4 w-100"
                data-index-group={nextGroupKey}
                onClick={onAddGroup}
              >
                <SVGPlus /> Add new group
              </Button>
            )}
          </div>
        )}
      </div>
    </div>
  );
});
