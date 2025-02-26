import { memo, useCallback, useMemo, useRef, useState } from 'react';
import { Responsive, WidthProvider } from 'react-grid-layout';
import type { Layout, Layouts } from 'react-grid-layout';
import { useStatsHouseShallow } from '@/store2';
import { Button } from '@/components/UI';
import { ReactComponent as SVGPlus } from 'bootstrap-icons/icons/plus.svg';
import { DashboardPlotWrapper } from './DashboardPlotWrapper';
import { PlotView } from '../Plot';
import { DashboardGroup } from '@/components2';
import { prepareItemsGroupWithLayout } from '@/common/prepareItemsGroup';
import 'react-grid-layout/css/styles.css';
import 'react-resizable/css/styles.css';
import cn from 'classnames';
import { getNextGroupKey } from '@/store2/urlStore/updateParamsPlotStruct';
import css from './style.module.css';
import { GroupKey } from '@/url2';
import { BREAKPOINT_WIDTH, BREAKPOINTS_SIZES, COLS, ROW_HEIGHTS } from './constants';
import { calculateDynamicRowHeight, calculateMaxRows, getBreakpointConfig } from '@/common/helpers';

const ResponsiveGridLayout = WidthProvider(Responsive);

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

  const [layouts, setLayouts] = useState<Layouts>({
    xxxl: [],
    xxl: [],
    xl: [],
    lg: [],
    md: [],
    sm: [],
    xs: [],
    xxs: [],
  });

  const [isDragging, setIsDragging] = useState(false);
  const [draggedPlotKey, setDraggedPlotKey] = useState<string | null>(null);
  const [draggedGroupKey, setDraggedGroupKey] = useState<string | null>(null);
  const [draggedItemDimensions, setDraggedItemDimensions] = useState<{ w: number; h: number } | null>(null);

  const isCrossingGroupsRef = useRef(false);
  const { breakpointKey } = useMemo(() => getBreakpointConfig(), []);

  const { itemsGroup, layoutsCoords } = useMemo(
    () =>
      prepareItemsGroupWithLayout({
        groups,
        orderGroup,
        orderPlot,
        breakpoint: breakpointKey,
      }),
    [groups, orderGroup, orderPlot, breakpointKey]
  );

  const save = useCallback(
    (
      plotKey: string | null,
      targetGroup: GroupKey | null,
      dropIndex: number | null,
      layout: Layout[],
      isResize: boolean = false
    ) => {
      console.log('HERE222222', plotKey, targetGroup, dropIndex, layout, isResize);
      if (plotKey != null && targetGroup != null) {
        // If crossing groups and we have original dimensions
        if (targetGroup !== draggedGroupKey && draggedItemDimensions && !isResize) {
          // Find the item in the layout
          const itemIndex = layout.findIndex((item) => item.i === `${targetGroup}::${plotKey}`);
          if (itemIndex >= 0) {
            // Preserve original dimensions
            layout[itemIndex].w = draggedItemDimensions.w;
            layout[itemIndex].h = draggedItemDimensions.h;
          }
        }
        const updatedItemsGroup = itemsGroup.map((group) => {
          // If this is a resize operation, update the layout of the item in its current group
          if (isResize && group.groupKey === targetGroup) {
            const resizedLayout = layout.find((l) => l.i === `${targetGroup}::${plotKey}`);
            if (resizedLayout) {
              // Create a deep copy of the layout to avoid reference issues
              const currentGroupLayout = {
                groupKey: targetGroup,
                layout: layout.map((item) => ({ ...item })),
              };
              console.log('----HEREEEE', currentGroupLayout);
              setNextDashboardSchemePlot(itemsGroup, currentGroupLayout, breakpointKey);
              return group; // Return the group unchanged as the layout is updated separately
            }
          }

          // If this is the source group from which the plot is being dragged
          if (group.groupKey === draggedGroupKey) {
            if (group.groupKey === targetGroup) {
              if (layout) {
                const newPlots = layout
                  .filter((item) => item.i.startsWith(`${targetGroup}::`))
                  .sort((a, b) => {
                    if (a.y !== b.y) return a.y - b.y;
                    return a.x - b.x;
                  })
                  .map((item) => item.i.split('::')[1])
                  .reverse();
                return {
                  groupKey: group.groupKey,
                  plots: newPlots,
                };
              }
              return group;
            }
            // Otherwise, remove the dragged plot from the group
            return {
              groupKey: group.groupKey,
              plots: group.plots.filter((p) => p !== plotKey),
            };
          }

          // If the current group is the target group, add the dragged plot to it
          if (group.groupKey === targetGroup) {
            const newPlots = [...group.plots];

            if (dropIndex !== null) {
              const dropIndexInGroup = newPlots.length - dropIndex;
              if (dropIndexInGroup >= 0 && dropIndexInGroup < newPlots.length) {
                newPlots.splice(dropIndexInGroup + 1, 0, newPlots[dropIndexInGroup]);
                newPlots[dropIndexInGroup] = plotKey;
              } else {
                newPlots.push(plotKey);
              }
            } else {
              newPlots.push(plotKey);
            }

            return {
              groupKey: group.groupKey,
              plots: newPlots,
            };
          }
          return group;
        });

        // If the target group doesn't exist in the updated items group, create a new group with the dragged plot
        if (!updatedItemsGroup.find((g) => g.groupKey === targetGroup)) {
          updatedItemsGroup.push({
            groupKey: targetGroup,
            plots: [plotKey],
          });
        }

        const currentGroupLayout = { groupKey: targetGroup, layout };

        setNextDashboardSchemePlot(updatedItemsGroup, currentGroupLayout, breakpointKey);
      }
    },
    [itemsGroup, setNextDashboardSchemePlot, breakpointKey, draggedGroupKey, draggedItemDimensions]
  );

  const onDragStart = useCallback((_layout: Layout[], oldItem: Layout) => {
    setIsDragging(true);
    const [groupKey, plotKey] = oldItem.i.split('::');
    setDraggedPlotKey(plotKey);
    setDraggedGroupKey(groupKey);

    // Store original dimensions of the dragged item
    const originalDimensions = {
      w: oldItem.w,
      h: oldItem.h,
    };
    setDraggedItemDimensions(originalDimensions);

    isCrossingGroupsRef.current = false;
  }, []);

  const getHoveredGroupKey = (e: MouseEvent): string | null => {
    const dropElement = document.elementsFromPoint(e.clientX, e.clientY);
    return dropElement.find((el) => el.getAttribute('data-group'))?.getAttribute('data-group') ?? null;
  };

  const onDrag = useCallback(
    (_layout: Layout[], _oldItem: Layout, _newItem: Layout, _placeholder: Layout, e: MouseEvent) => {
      if (!isDragging || !draggedGroupKey) return;

      const hoveredGroup = getHoveredGroupKey(e);

      if (hoveredGroup && hoveredGroup !== draggedGroupKey) {
        isCrossingGroupsRef.current = true;
      }
    },
    [isDragging, draggedGroupKey]
  );

  const onLayoutChange = useCallback(
    (layout: Layout[], layouts: Layouts) => {
      if (isDragging && draggedGroupKey && !isCrossingGroupsRef.current) {
        const [groupKey, plotKey] = layout[0].i.split('::');
        if (groupKey === draggedGroupKey) {
          save(plotKey, groupKey, null, layout);
        }
      }
      setLayouts(layouts);
    },
    [draggedGroupKey, isDragging, save]
  );

  const handleResizeStop = useCallback(
    (layout: Layout[], groupKey: string) => {
      const plotKey = layout[0]?.i?.split('::')[1];

      save(plotKey, groupKey, null, layout, true);
    },
    [save]
  );

  const onDragStop = useCallback(
    (layout: Layout[], _oldItem: Layout, _newItem: Layout, _placeholder: Layout, e: MouseEvent) => {
      const targetGroup = getHoveredGroupKey(e);

      if (!targetGroup || !draggedPlotKey) {
        setDraggedPlotKey(null);
        setDraggedGroupKey(null);
        setIsDragging(false);
        isCrossingGroupsRef.current = false;
        return;
      }

      if (targetGroup !== draggedGroupKey) {
        isCrossingGroupsRef.current = true;

        const targetGroupElement = document.querySelector(`[data-group="${targetGroup}"]`);
        if (!targetGroupElement) {
          save(draggedPlotKey, targetGroup, null, layout);
          setDraggedPlotKey(null);
          setDraggedGroupKey(null);
          setIsDragging(false);
          isCrossingGroupsRef.current = false;
          return;
        }

        const plotElements = Array.from(targetGroupElement.querySelectorAll('.plot-item'));
        const dropIndex = findDropIndex(plotElements, e.clientX, e.clientY);

        save(draggedPlotKey, targetGroup, dropIndex, layout);
      }

      setDraggedPlotKey(null);
      setDraggedGroupKey(null);
      setIsDragging(false);
    },
    [draggedPlotKey, draggedGroupKey, save]
  );

  function findDropIndex(plotElements: Element[], mouseX: number, mouseY: number): number {
    const index = [...plotElements].reverse().findIndex((element) => {
      const rect = element.getBoundingClientRect();

      return mouseX >= rect.left && mouseX <= rect.right && mouseY >= rect.top && mouseY <= rect.bottom;
    });

    return index === -1 ? plotElements.length : index;
  }

  const onAddGroup = useCallback(
    (e: React.MouseEvent<HTMLButtonElement>) => {
      const groupKey = e.currentTarget.getAttribute('data-index-group') ?? '0';
      addDashboardGroup(groupKey);
    },
    [addDashboardGroup]
  );

  const nextGroupKey = useMemo(() => getNextGroupKey({ orderGroup }), [orderGroup]);

  const isNotMobile = BREAKPOINT_WIDTH[breakpointKey] >= BREAKPOINT_WIDTH.md;
  const isDashboardEditAllowed = dashboardLayoutEdit && isNotMobile;

  const dynamicRowHeight = useMemo(() => {
    const currentWidth = window.innerWidth;
    if (breakpointKey === BREAKPOINTS_SIZES.xxxl) {
      return calculateDynamicRowHeight(currentWidth);
    }
    return ROW_HEIGHTS[breakpointKey];
  }, [breakpointKey]);

  console.log('----11111111111layoutsCoords', layoutsCoords);

  return (
    <div className="container-fluid">
      <div className={cn(isDashboardEditAllowed && 'dashboard-edit', className)}>
        {itemsGroup.map(({ groupKey, plots }) => (
          <DashboardGroup key={`group-${groupKey}`} groupKey={groupKey}>
            {groups[groupKey]?.show !== false && (
              <ResponsiveGridLayout
                className={cn(
                  'layout',
                  'd-flex flex-row flex-wrap',
                  (!plots.length && !isDashboardEditAllowed) || isEmbed ? 'pb-0' : 'pb-3'
                )}
                breakpoints={BREAKPOINT_WIDTH}
                margin={[0, 30]}
                cols={COLS}
                rowHeight={dynamicRowHeight}
                autoSize
                isDraggable={isDashboardEditAllowed}
                isResizable={isDashboardEditAllowed}
                compactType="horizontal"
                maxRows={calculateMaxRows(
                  plots,
                  COLS[breakpointKey],
                  layouts[breakpointKey]?.filter((item) => item.i.startsWith(`${groupKey}::`))
                )}
                onDragStop={onDragStop}
                onDragStart={onDragStart}
                onDrag={onDrag}
                onResizeStop={(layout) => handleResizeStop(layout, groupKey)}
                onLayoutChange={onLayoutChange}
                isDroppable={isDashboardEditAllowed}
              >
                {plots.map((plot) => {
                  const layout = layoutsCoords.find((l) => l.groupKey === groupKey)?.layout;
                  // console.log('----layout', layout);
                  const plotLayout = Array.isArray(layout)
                    ? layout.find((l) => l.i === `${groupKey}::${plot}`)
                    : layout;
                  // console.log('----plotLayout', plotLayout);
                  return (
                    <DashboardPlotWrapper
                      key={`${groupKey}::${plot}`}
                      data-grid={{
                        x: plotLayout?.x || 0,
                        y: plotLayout?.y || 0,
                        w: plotLayout?.w || 1,
                        h: plotLayout?.h || 1,
                        i: `${groupKey}::${plot}`,
                      }}
                      className={cn('plot-item p-1', isDashboardEditAllowed && css.cursorMove)}
                    >
                      <PlotView
                        className={cn(
                          isDashboardEditAllowed && css.pointerEventsNone,
                          isNotMobile && 'position-relative overflow-hidden w-100 h-100'
                        )}
                        key={`plot-${plot}`}
                        plotKey={plot}
                        isDashboard
                      />
                    </DashboardPlotWrapper>
                  );
                })}
              </ResponsiveGridLayout>
            )}
          </DashboardGroup>
        ))}

        {isDashboardEditAllowed && (
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
