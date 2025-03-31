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
// import { GroupKey } from '@/url2';
import { BREAKPOINT_WIDTH, BREAKPOINTS_SIZES, COLS, ROW_HEIGHTS } from './constants';
import { calculateDynamicRowHeight, getBreakpointConfig, getSizeColumns, isMobile } from '@/common/helpers';

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
  const [lastMovedItem, setLastMovedItem] = useState<string | null>(null);

  const { breakpointKey } = useMemo(() => getBreakpointConfig(), []);

  // Move isMobile() call to component level with useMemo to prevent recalculation
  const mobileDevice = useMemo(() => isMobile(), []);

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

  // Calculate row height based on screen width and breakpoint
  const dynamicRowHeight = useMemo(() => {
    const currentWidth = window.innerWidth;
    if (breakpointKey === BREAKPOINTS_SIZES.xxxl) {
      return calculateDynamicRowHeight(currentWidth);
    }
    return ROW_HEIGHTS[breakpointKey];
  }, [breakpointKey]);

  // calculate dynamic row height based on widgetColsWidth
  const calculateRowHeightForGroup = useCallback(
    (groupKey: string) => {
      const size = groups[groupKey]?.size;
      const widgetColsWidth = getSizeColumns(size);

      let baseRowHeight = dynamicRowHeight;

      if (breakpointKey === BREAKPOINTS_SIZES.xxxl) {
        const currentWidth = window.innerWidth;
        baseRowHeight = calculateDynamicRowHeight(currentWidth);
      } else {
        baseRowHeight = ROW_HEIGHTS[breakpointKey];
      }

      const twoColMultiplier =
        {
          xxxl: 3.4,
          xxl: 3,
          xl: 2.45,
          lg: 1.95,
          md: 2.2,
          sm: 1.3,
          xs: 0.8,
          xxs: 0.3,
        }[breakpointKey] || 2.18;

      const threeColMultiplier =
        {
          xxxl: 2.65,
          xxl: 2.35,
          xl: 1.93,
          lg: 1.52,
          md: 1.34,
          sm: 1,
          xs: 0.5,
          xxs: 0.1,
        }[breakpointKey] || 1.32;

      const fourColMultiplier =
        {
          xxxl: 2.65,
          xxl: 2.35,
          xl: 1.93,
          lg: 1.52,
          md: 1.91,
          sm: 1.24,
          xs: 0.5,
          xxs: 0.1,
        }[breakpointKey] || 1.32;

      let scaleFactor;

      switch (widgetColsWidth) {
        case 2:
          return baseRowHeight * twoColMultiplier;
        case 3:
          return baseRowHeight * threeColMultiplier;
        case 4:
          return baseRowHeight * fourColMultiplier;
        default:
          scaleFactor = 3 / widgetColsWidth;
          return baseRowHeight * Math.min(1.875, Math.max(1.0, scaleFactor));
      }
    },
    [groups, breakpointKey, dynamicRowHeight]
  );

  // Generate unified layout for all groups and plots
  const unifiedLayout = useMemo(() => {
    const cols = COLS[breakpointKey] || 12;
    const layout: Layout[] = [];
    let currentY = 0;

    // Process each group
    itemsGroup.forEach(({ groupKey, plots }) => {
      const size = groups[groupKey]?.size;
      const widgetColsWidth = getSizeColumns(size);

      // Get row height specific to this group
      const groupRowHeight = calculateRowHeightForGroup(groupKey);
      const rowHeightRatio = groupRowHeight / dynamicRowHeight;

      // Add group header as static element
      layout.push({
        i: `group::${groupKey}`,
        x: 0,
        y: currentY,
        w: cols,
        h: dashboardLayoutEdit ? 2 : 1,
        // h: 2,
        isDraggable: false,
        isResizable: false,
      });

      currentY += 1;

      if (groups[groupKey]?.show === false || plots.length === 0) return;

      // Find existing layout for this group
      const existingGroupLayout = layoutsCoords.find((l) => l.groupKey === groupKey);
      let plotLayouts: Layout[] = [];

      if (existingGroupLayout && existingGroupLayout.layout.length >= plots.length) {
        console.log('HERE1111');
        plotLayouts = existingGroupLayout.layout
          .filter((item) => {
            const plotKey = item.i.split('::')[1];
            return plots.includes(plotKey);
          })
          .map((item) => ({
            ...item,
            minW: 3,
            minH: 7,
            w: item.w,
            y: item.y + currentY,
            h: item.h,
          }));

        if (plotLayouts.length === plots.length) {
          layout.push(...plotLayouts);

          const maxY = plotLayouts.reduce((max, item) => Math.max(max, item.y + item.h), currentY);
          currentY = maxY;
          return;
        }
      }

      let itemWidth = 0;

      if (mobileDevice) {
        itemWidth = cols;
      } else {
        switch (widgetColsWidth) {
          case 2:
            itemWidth = Math.floor(cols / 2);
            break;
          case 3:
            itemWidth = Math.floor(cols / 3);
            break;
          case 4:
            itemWidth = Math.floor(cols / 4);
            break;
          default:
            itemWidth = Math.floor(cols / widgetColsWidth);
        }
      }

      let defaultHeight = 5;
      if (breakpointKey === BREAKPOINTS_SIZES.xxxl || breakpointKey === BREAKPOINTS_SIZES.xxl) {
        defaultHeight = 6;
      } else if (breakpointKey === BREAKPOINTS_SIZES.xl || breakpointKey === BREAKPOINTS_SIZES.lg) {
        defaultHeight = 6;
      } else if (breakpointKey === BREAKPOINTS_SIZES.xs || breakpointKey === BREAKPOINTS_SIZES.xxs) {
        defaultHeight = 4;
      }

      // Calculate a balanced height based on column count
      let widthRatio;
      if (widgetColsWidth <= 2) {
        widthRatio = itemWidth / (cols / 2);
      } else {
        widthRatio = Math.max(0.5, itemWidth / (cols / 2.7));
      }

      // Apply a minimum height that scales with column count
      const minimumHeight = Math.max(2, 4 - widgetColsWidth * 0.5);
      defaultHeight = Math.max(minimumHeight, Math.round(defaultHeight * widthRatio));
      // Generate layouts for plots
      plots.forEach((plot, index) => {
        // On mobile, each chart gets its own row
        const row = mobileDevice ? index : Math.floor(index / widgetColsWidth);
        const col = mobileDevice ? 0 : index % widgetColsWidth;
        const startX = col * itemWidth;

        layout.push({
          i: `${groupKey}::${plot}`,
          x: startX,
          y: currentY + row * defaultHeight,
          w: itemWidth,
          h: Math.round(defaultHeight * rowHeightRatio + 0.8),
          minW: 3,
          minH: 7,
        });
      });

      // Update currentY to be after this group
      const maxPlotY =
        plots.length > 0
          ? Math.max(...layout.filter((item) => item.i.startsWith(`${groupKey}::`)).map((item) => item.y + item.h))
          : currentY;

      currentY = maxPlotY + 1; // Add extra space between groups
    });

    return layout;
  }, [
    breakpointKey,
    itemsGroup,
    groups,
    calculateRowHeightForGroup,
    dynamicRowHeight,
    dashboardLayoutEdit,
    layoutsCoords,
    mobileDevice,
  ]);

  const save = useCallback(
    (layout: Layout[]) => {
      if (!layout.length) return;

      // Extract plot moves and group assignments from layout
      const updatedGroupsMap = new Map<string, string[]>();

      // Initialize groups with empty plot arrays
      itemsGroup.forEach(({ groupKey }) => {
        updatedGroupsMap.set(groupKey, []);
      });

      // Parse layout to determine group assignments
      layout.forEach((item) => {
        if (item.i.startsWith('group::')) return; // Skip group headers

        const [groupKey, plotKey] = item.i.split('::');
        if (!groupKey || !plotKey) return;

        const plots = updatedGroupsMap.get(groupKey) || [];
        if (!plots.includes(plotKey)) {
          plots.push(plotKey);
        }
        updatedGroupsMap.set(groupKey, plots);
      });

      // Convert to format expected by setNextDashboardSchemePlot
      const updatedItemsGroup = Array.from(updatedGroupsMap.entries()).map(([groupKey, plots]) => ({
        groupKey,
        plots,
      }));

      // Process layout updates for each group
      const groupLayouts = new Map<string, Layout[]>();

      layout.forEach((item) => {
        if (item.i.startsWith('group::')) return; // Skip group headers

        const [groupKey] = item.i.split('::');
        if (!groupKey) return;

        const groupLayout = groupLayouts.get(groupKey) || [];
        groupLayout.push(item);
        groupLayouts.set(groupKey, groupLayout);
      });

      // Update each group's layout
      groupLayouts.forEach((groupLayout, groupKey) => {
        setNextDashboardSchemePlot(updatedItemsGroup, {
          groupKey,
          layout: groupLayout,
        });
      });
    },
    [itemsGroup, setNextDashboardSchemePlot]
  );

  const onDragStart = useCallback((_layout: Layout[], oldItem: Layout) => {
    if (oldItem.i.startsWith('group::')) return; // Don't allow dragging group headers

    setIsDragging(true);
    const [groupKey, plotKey] = oldItem.i.split('::');
    setDraggedPlotKey(plotKey);
    setDraggedGroupKey(groupKey);

    // Store original dimensions of the dragged item
    setDraggedItemDimensions({
      w: oldItem.w,
      h: oldItem.h,
    });
  }, []);

  // Get the key for the next group to be created
  const nextGroupKey = useMemo(() => getNextGroupKey({ orderGroup }), [orderGroup]);

  // Helper function to check if dragged below last group
  const isDroppedBelowLastGroup = useCallback((layout: Layout[], draggedY: number): boolean => {
    const groupHeaders = layout.filter((item) => item.i.startsWith('group::'));
    if (groupHeaders.length === 0) return true;

    const sortedGroupHeaders = [...groupHeaders].sort((a, b) => a.y - b.y);
    const lastHeaderItem = sortedGroupHeaders[sortedGroupHeaders.length - 1];

    const lastGroupItems = layout.filter(
      (item) => !item.i.startsWith('group::') && item.i.startsWith(lastHeaderItem.i.replace('group::', ''))
    );
    const maxLastGroupY =
      lastGroupItems.length > 0
        ? Math.max(...lastGroupItems.map((item) => item.y + item.h))
        : lastHeaderItem.y + lastHeaderItem.h + 6;

    return draggedY > maxLastGroupY;
  }, []);

  // Helper function to determine which group is currently being hovered over during drag
  const getHoveredGroupKey = useCallback(
    (layout: Layout[], draggedY: number): string | null => {
      // Find all group header items in the layout
      const groupHeaders = layout.filter((item) => item.i.startsWith('group::'));
      if (groupHeaders.length === 0) return null;

      // Sort group headers by Y position
      const sortedGroupHeaders = [...groupHeaders].sort((a, b) => a.y - b.y);

      if (isDroppedBelowLastGroup(layout, draggedY)) {
        return nextGroupKey;
      }

      for (let i = 0; i < sortedGroupHeaders.length; i++) {
        const currentHeader = sortedGroupHeaders[i];
        const nextHeader = sortedGroupHeaders[i + 1];
        const groupKey = currentHeader.i.replace('group::', '');

        if (!nextHeader || (draggedY >= currentHeader.y && draggedY < nextHeader.y)) {
          return groupKey;
        }
      }

      return null;
    },
    [isDroppedBelowLastGroup, nextGroupKey]
  );

  // Track when dragging crosses between different groups
  const onDrag = useCallback(
    (layout: Layout[], _oldItem: Layout, newItem: Layout, _placeholder: Layout, e: MouseEvent) => {
      if (!isDragging || !draggedGroupKey) return;

      // Prevent dragging above the first group header
      const groupHeaders = layout.filter((item) => item.i.startsWith('group::'));
      const firstHeaderY = groupHeaders.length > 0 ? Math.min(...groupHeaders.map((header) => header.y)) : 0;
      if (newItem.y < firstHeaderY + 1) {
        // Force the item to stay below the first header
        newItem.y = firstHeaderY + 1;
      }
    },
    [isDragging, draggedGroupKey]
  );

  // Handle the end of drag operations
  const onDragStop = useCallback(
    (layout: Layout[], _oldItem: Layout, newItem: Layout, _placeholder: Layout, e: MouseEvent) => {
      if (!draggedPlotKey || !draggedGroupKey) {
        setDraggedPlotKey(null);
        setDraggedGroupKey(null);
        setIsDragging(false);
        return;
      }

      // Handle dragging above first header
      const groupHeaders = layout.filter((item) => item.i.startsWith('group::'));
      if (groupHeaders.length > 0) {
        const firstHeaderY = Math.min(...groupHeaders.map((header) => header.y));
        if (newItem.y < firstHeaderY + 1) {
          newItem.y = firstHeaderY + 1;
        }
      }

      const targetGroup = getHoveredGroupKey(layout, newItem.y);

      // Check if the target group is the new group key (dropped below all groups)
      if (targetGroup === nextGroupKey) {
        // Create a new layout with the dragged item now in the new group
        const updatedLayout = layout.map((item) => {
          if (item.i === `${draggedGroupKey}::${draggedPlotKey}`) {
            return {
              ...item,
              i: `${nextGroupKey}::${draggedPlotKey}`,
              ...(draggedItemDimensions ? { w: draggedItemDimensions.w, h: draggedItemDimensions.h } : {}),
            };
          }
          return item;
        });

        // Add the new group and save the updated layout
        addDashboardGroup(nextGroupKey);
        save(updatedLayout);

        // Установить последний перемещенный элемент
        setLastMovedItem(`${nextGroupKey}::${draggedPlotKey}`);
      } else if (targetGroup && targetGroup !== draggedGroupKey) {
        // Cross-group drag: update the item ID to reflect new group
        const updatedLayout = layout.map((item) => {
          if (item.i === `${draggedGroupKey}::${draggedPlotKey}`) {
            return {
              ...item,
              i: `${targetGroup}::${draggedPlotKey}`,
              ...(draggedItemDimensions ? { w: draggedItemDimensions.w, h: draggedItemDimensions.h } : {}),
            };
          }
          return item;
        });

        save(updatedLayout);

        // Установить последний перемещенный элемент
        setLastMovedItem(`${targetGroup}::${draggedPlotKey}`);
      } else {
        save(layout);

        // Установить последний перемещенный элемент
        setLastMovedItem(`${draggedGroupKey}::${draggedPlotKey}`);
      }

      setDraggedPlotKey(null);
      setDraggedGroupKey(null);
      setIsDragging(false);
    },
    [draggedPlotKey, draggedGroupKey, getHoveredGroupKey, nextGroupKey, addDashboardGroup, save, draggedItemDimensions]
  );

  const handleResizeStop = useCallback(
    (layout: Layout[]) => {
      save(layout);
    },
    [save]
  );

  // Add a new group to the dashboard
  const onAddGroup = useCallback(
    (e: React.MouseEvent<HTMLButtonElement>) => {
      const groupKey = e.currentTarget.getAttribute('data-index-group') ?? '0';
      addDashboardGroup(groupKey);
    },
    [addDashboardGroup]
  );

  // Determine if dashboard editing is allowed based on device and settings
  const isDashboardEditAllowed = dashboardLayoutEdit && !mobileDevice;

  return (
    <div className="container-fluid">
      <div className={cn(isDashboardEditAllowed && 'dashboard-edit', className)}>
        <ResponsiveGridLayout
          className={cn('layout', 'd-flex flex-row flex-wrap', !isEmbed ? 'pb-3' : 'pb-0')}
          breakpoints={BREAKPOINT_WIDTH}
          // margin={[0, 30]}
          cols={COLS}
          rowHeight={dynamicRowHeight}
          isDraggable={isDashboardEditAllowed}
          isResizable={isDashboardEditAllowed}
          onDragStop={onDragStop}
          onDragStart={onDragStart}
          onDrag={onDrag}
          // onLayoutChange={onLayoutChange}
          onResizeStop={handleResizeStop}
          isDroppable={isDashboardEditAllowed}
          layouts={{
            [breakpointKey]: unifiedLayout,
          }}
          // compactType="vertical"
        >
          {/* Render group headers as static items */}
          {itemsGroup.map(({ groupKey }) => (
            <div key={`group::${groupKey}`} className="w-100">
              <DashboardGroup groupKey={groupKey} data-group={groupKey} />
            </div>
          ))}
          {/* Render all plots */}
          {itemsGroup.map(
            ({ groupKey, plots }) =>
              groups[groupKey]?.show !== false &&
              plots.map((plot) => (
                <DashboardPlotWrapper
                  key={`${groupKey}::${plot}`}
                  className={cn(
                    'plot-item p-1',
                    isDashboardEditAllowed && css.cursorMove,

                    lastMovedItem === `${groupKey}::${plot}` && 'border border-primary'
                  )}
                  data-item-id={`${groupKey}::${plot}`}
                  data-group-key={groupKey}
                >
                  <PlotView
                    className={cn(
                      isDashboardEditAllowed && [css.pointerEventsNone, 'position-relative overflow-hidden w-100 h-100']
                    )}
                    key={`plot-${plot}`}
                    plotKey={plot}
                    isDashboard
                  />
                </DashboardPlotWrapper>
              ))
          )}
        </ResponsiveGridLayout>

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
