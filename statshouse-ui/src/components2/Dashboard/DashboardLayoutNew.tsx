import { memo, useCallback, useMemo, useRef, useState } from 'react';
import { Responsive, WidthProvider } from 'react-grid-layout';
import type { Layout } from 'react-grid-layout';
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

  const [isDragging, setIsDragging] = useState(false);
  const [draggedPlotKey, setDraggedPlotKey] = useState<string | null>(null);
  const [draggedGroupKey, setDraggedGroupKey] = useState<string | null>(null);
  const [draggedItemDimensions, setDraggedItemDimensions] = useState<{ w: number; h: number } | null>(null);

  const isCrossingGroupsRef = useRef(false);
  const { breakpointKey } = useMemo(() => getBreakpointConfig(), []);

  // itemsGroup: Contains the structure of groups and their plots
  // layoutsCoords: Contains the layout coordinates for each group
  const { itemsGroup, layoutsCoords } = useMemo(
    () =>
      prepareItemsGroupWithLayout({
        groups,
        orderGroup,
        orderPlot,
      }),
    [groups, orderGroup, orderPlot]
  );

  const save = useCallback(
    (plotKey: string | null, targetGroup: GroupKey | null, layout: Layout[], isResize: boolean = false) => {
      if (plotKey != null && targetGroup != null) {
        // If crossing groups and we have original dimensions
        if (targetGroup !== draggedGroupKey && draggedItemDimensions && !isResize) {
          // Find the item in the layout
          const itemIndex = layout.findIndex((item) => item.i === `${targetGroup}::${plotKey}`);
          if (itemIndex >= 0) {
            // Create a new layout array with preserved original dimensions
            layout = layout.map((item, index) => {
              if (index === itemIndex) {
                return {
                  ...item,
                  w: draggedItemDimensions.w,
                  h: draggedItemDimensions.h,
                };
              }
              return item;
            });
          }
        }

        // Update groups and items in them
        const updatedItemsGroup = itemsGroup.map((group) => {
          // If this is a resize operation, update the layout of the item in its current group
          if (isResize && group.groupKey === targetGroup) {
            const resizedLayout = layout.find((l) => l.i === `${targetGroup}::${plotKey}`);
            if (resizedLayout) {
              const currentGroupLayout = {
                groupKey: targetGroup,
                layout: layout.map((item) => ({ ...item })),
              };

              setNextDashboardSchemePlot(itemsGroup, currentGroupLayout);
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
                  .map((item) => item.i.split('::')[1]);

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
            if (!group.plots.includes(plotKey)) {
              return {
                groupKey: group.groupKey,
                plots: [...group.plots, plotKey],
              };
            }
            return group;
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

        // Update groups layout
        if (targetGroup !== draggedGroupKey && draggedPlotKey) {
          // 1. Find current layouts for source and target groups
          const sourceGroupLayout = layoutsCoords.find((l) => l.groupKey === draggedGroupKey)?.layout || [];
          const targetGroupLayout = layoutsCoords.find((l) => l.groupKey === targetGroup)?.layout || [];

          // 2. Find the item we are dragging, from layout
          const draggedItemLayout = layout.find((item) => item.i === `${draggedGroupKey}::${draggedPlotKey}`);

          if (draggedItemLayout && draggedGroupKey) {
            let maxY = 0;
            if (Array.isArray(targetGroupLayout) && targetGroupLayout.length > 0) {
              targetGroupLayout.forEach((item) => {
                if (item.y + item.h > maxY) {
                  maxY = item.y + item.h;
                }
              });
            }

            let maxX = 0;
            if (Array.isArray(targetGroupLayout) && targetGroupLayout.length > 0) {
              // Consider only items that are in the bottom row
              const bottomRowItems = targetGroupLayout.filter(
                (item) =>
                  (item.y < maxY && item.y + item.h > maxY) || // Items that span into the bottom row
                  item.y + item.h === maxY // Items that end exactly at the bottom row
              );

              bottomRowItems.forEach((item) => {
                if (item.x + item.w > maxX) {
                  maxX = item.x + item.w;
                }
              });
            }

            // Check if adding to maxX would exceed grid width
            const cols = COLS[breakpointKey] || 8;
            const itemWidth = draggedItemDimensions?.w || draggedItemLayout.w;

            let newX = maxX;
            let newY = maxY > 0 ? maxY - 1 : 0; // Position at the bottom row, but never negative

            if (maxX + itemWidth > cols) {
              newX = 0;
              newY = maxY;
            }

            // 3. Create a new layout for the target group: keep existing + add new element
            const newTargetLayout = [
              ...(Array.isArray(targetGroupLayout) ? targetGroupLayout : []),
              {
                ...draggedItemLayout,
                i: `${targetGroup}::${draggedPlotKey}`,
                // Keep original dimensions
                ...(draggedItemDimensions
                  ? {
                      w: draggedItemDimensions.w,
                      h: draggedItemDimensions.h,
                    }
                  : {}),

                x: newX,
                y: newY,
              },
            ];

            // 4. Create a new layout for the source group: without the dragged element
            const newSourceLayout = Array.isArray(sourceGroupLayout)
              ? sourceGroupLayout.filter((item) => !item.i.endsWith(`::${draggedPlotKey}`))
              : [];

            // 5. Create an array of layout schemes for both groups
            const layoutSchemes = [
              {
                groupKey: targetGroup,
                layout: newTargetLayout,
              },
              {
                groupKey: draggedGroupKey,
                layout: newSourceLayout,
              },
            ];

            // 6. Update layouts of both groups in one call
            layoutSchemes.forEach((scheme) => setNextDashboardSchemePlot(updatedItemsGroup, scheme));
          }
        } else {
          // Moving within the same group or resize
          const currentGroupLayout = { groupKey: targetGroup, layout };
          setNextDashboardSchemePlot(updatedItemsGroup, currentGroupLayout);
        }
      }
    },
    [
      draggedGroupKey,
      draggedItemDimensions,
      itemsGroup,
      draggedPlotKey,
      setNextDashboardSchemePlot,
      breakpointKey,
      layoutsCoords,
    ]
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

  // Helper function to determine which group is currently being hovered over during drag
  const getHoveredGroupKey = (e: MouseEvent): string | null => {
    const dropElement = document.elementsFromPoint(e.clientX, e.clientY);
    return dropElement.find((el) => el.getAttribute('data-group'))?.getAttribute('data-group') ?? null;
  };

  // Track when dragging crosses between different groups
  const onDrag = useCallback(
    (_layout: Layout[], _oldItem: Layout, _newItem: Layout, _placeholder: Layout, e: MouseEvent) => {
      if (!isDragging || !draggedGroupKey) return;

      const hoveredGroup = getHoveredGroupKey(e);

      // Set flag when dragging between different groups
      if (hoveredGroup && hoveredGroup !== draggedGroupKey) {
        isCrossingGroupsRef.current = true;
      }
    },
    [isDragging, draggedGroupKey]
  );

  // Handle layout changes during drag within the same group
  const onLayoutChange = useCallback(
    (layout: Layout[]) => {
      if (isDragging && draggedGroupKey && !isCrossingGroupsRef.current) {
        const [groupKey, plotKey] = layout[0].i.split('::');
        if (groupKey === draggedGroupKey) {
          save(plotKey, groupKey, layout);
        }
      }
    },
    [draggedGroupKey, isDragging, save]
  );

  // Handle resize operations for widgets
  const handleResizeStop = useCallback(
    (layout: Layout[], groupKey: string) => {
      const plotKey = layout[0]?.i?.split('::')[1];
      save(plotKey, groupKey, layout, true);
    },
    [save]
  );

  // Handle the end of drag operations, including cross-group drags
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

      // Handle cross-group dragging
      if (targetGroup !== draggedGroupKey) {
        isCrossingGroupsRef.current = true;
        save(draggedPlotKey, targetGroup, layout);
      }

      setDraggedPlotKey(null);
      setDraggedGroupKey(null);
      setIsDragging(false);
    },
    [draggedPlotKey, draggedGroupKey, save]
  );

  // Add a new group to the dashboard
  const onAddGroup = useCallback(
    (e: React.MouseEvent<HTMLButtonElement>) => {
      const groupKey = e.currentTarget.getAttribute('data-index-group') ?? '0';
      addDashboardGroup(groupKey);
    },
    [addDashboardGroup]
  );

  // Get the key for the next group to be created
  const nextGroupKey = useMemo(() => getNextGroupKey({ orderGroup }), [orderGroup]);

  // Determine if dashboard editing is allowed based on device and settings
  const isNotMobile = BREAKPOINT_WIDTH[breakpointKey] >= BREAKPOINT_WIDTH.md;
  const isDashboardEditAllowed = dashboardLayoutEdit && isNotMobile;

  // Calculate row height based on screen width and breakpoint
  const dynamicRowHeight = useMemo(() => {
    const currentWidth = window.innerWidth;
    if (breakpointKey === BREAKPOINTS_SIZES.xxxl) {
      return calculateDynamicRowHeight(currentWidth);
    }
    return ROW_HEIGHTS[breakpointKey];
  }, [breakpointKey]);

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
                  layoutsCoords.find((l) => l.groupKey === groupKey)?.layout
                )}
                onDragStop={onDragStop}
                onDragStart={onDragStart}
                onDrag={onDrag}
                onResizeStop={(layout) => handleResizeStop(layout, groupKey)}
                onLayoutChange={onLayoutChange}
                isDroppable={isDashboardEditAllowed}
              >
                {/* mapping right to left because elements are rendered from right to left */}
                {plots
                  .slice()
                  .reverse()
                  .map((plot) => {
                    const layout = layoutsCoords.find((l) => l.groupKey === groupKey)?.layout;
                    const plotLayout = Array.isArray(layout)
                      ? layout.find((l) => l.i === `${groupKey}::${plot}`)
                      : undefined;
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
                            isDashboardEditAllowed && isNotMobile && 'position-relative overflow-hidden w-100 h-100'
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
