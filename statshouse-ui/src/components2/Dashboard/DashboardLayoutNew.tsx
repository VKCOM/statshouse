import { memo, useCallback, useEffect, useState } from 'react';
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
import css from './style.module.css';
// import { GroupKey } from '@/url2';

const ResponsiveGridLayout = WidthProvider(Responsive);

const COLS = {
  lg: 12,
  md: 10,
  sm: 6,
  xs: 4,
  xxs: 2,
};

const BREAKPOINTS = { lg: 1200, md: 996, sm: 768, xs: 480, xxs: 0 };

type DashboardLayoutProps = {
  className?: string;
};

type GroupLayout = {
  groupKey: string;
  layouts: Layouts;
};

type DragState = {
  isDragging: boolean;
  sourceGroupKey: string | null;
  plotKey: string | null;
  mousePosition: { x: number; y: number } | null;
};

const initialDragState: DragState = {
  isDragging: false,
  sourceGroupKey: null,
  plotKey: null,
  mousePosition: null,
};

const createLayoutItem = (x: number, y: number, groupKey: string, plotKey: string) => ({
  w: 2,
  h: 1,
  x,
  y,
  i: `${groupKey}::${plotKey}`,
  static: false,
});

const convertItemsGroupToLayout = (itemsGroup: ReturnType<typeof prepareItemsGroup>): GroupLayout[] =>
  itemsGroup.map((group) => {
    const initLayouts = group.plots.map((plotKey, index) => createLayoutItem(index * 2, 0, group.groupKey, plotKey));

    return {
      groupKey: group.groupKey,
      layouts: {
        lg: initLayouts,
        md: [...initLayouts],
        sm: [...initLayouts],
        xs: [...initLayouts],
        xxs: [...initLayouts],
      },
    };
  });

export const DashboardLayoutNew = memo(function DashboardLayoutNew({ className }: DashboardLayoutProps) {
  const { groups, orderGroup, orderPlot, dashboardLayoutEdit, isEmbed, addDashboardGroup } = useStatsHouseShallow(
    ({ params: { groups, orderGroup, orderPlot }, dashboardLayoutEdit, isEmbed, addDashboardGroup }) => ({
      groups,
      orderGroup,
      orderPlot,
      dashboardLayoutEdit,
      isEmbed,
      addDashboardGroup,
    })
  );

  const [layouts, setLayouts] = useState<GroupLayout[]>(() =>
    convertItemsGroupToLayout(prepareItemsGroup({ groups, orderGroup, orderPlot }))
  );

  const [dragState, setDragState] = useState<DragState>(initialDragState);

  useEffect(() => {
    setLayouts(convertItemsGroupToLayout(prepareItemsGroup({ groups, orderGroup, orderPlot })));
  }, [groups, orderGroup, orderPlot]);

  const handleDragStart = useCallback((layout: Layout[], oldItem: Layout) => {
    const [groupKey, plotKey] = oldItem.i.split('::');
    setDragState({
      isDragging: true,
      sourceGroupKey: groupKey,
      plotKey: plotKey,
      mousePosition: null,
    });
  }, []);

  const findDropIndex = useCallback((plotElements: Element[], mouseX: number, mouseY: number): number => {
    for (let i = 0; i < plotElements.length; i++) {
      const rect = plotElements[i].getBoundingClientRect();
      if (mouseX <= rect.right) {
        return i;
      }
    }
    return plotElements.length;
  }, []);

  const handleLayoutChange = useCallback(
    (currentLayout: Layout[], allLayouts: Layouts, e?: MouseEvent, element?: HTMLElement) => {
      if (!currentLayout.length) return;

      setLayouts((prevLayouts) => {
        let updatedLayouts = [...prevLayouts];
        const currentGroupKey = currentLayout[0].i.split('::')[0];

        // Если это обычное изменение layout (resize/reorder)
        if (!dragState.isDragging || !e) {
          return prevLayouts.map((group) => {
            if (group.groupKey !== currentGroupKey) {
              return group;
            }

            return {
              ...group,
              layouts: {
                ...group.layouts,
                lg: currentLayout.filter((item) => item.i.startsWith(group.groupKey)),
              },
            };
          });
        }

        // Если это окончание drag&drop операции
        const dropElements = document.elementsFromPoint(e.clientX, e.clientY);
        const targetGroup = dropElements.find((el) => el.getAttribute('data-group'))?.getAttribute('data-group');

        if (!targetGroup || !dragState.plotKey || !dragState.sourceGroupKey) {
          return prevLayouts;
        }

        // Удаляем из исходной группы
        updatedLayouts = updatedLayouts.map((group) => {
          if (group.groupKey === dragState.sourceGroupKey) {
            return {
              ...group,
              layouts: {
                ...group.layouts,
                lg: group.layouts.lg.filter(
                  (layout) => layout.i !== `${dragState.sourceGroupKey}::${dragState.plotKey}`
                ),
              },
            };
          }
          return group;
        });

        // Добавляем в целевую группу
        const existingTargetGroup = updatedLayouts.find((group) => group.groupKey === targetGroup);
        const targetGroupElement = document.querySelector(`[data-group="${targetGroup}"]`);
        const dropIndex = targetGroupElement
          ? findDropIndex(Array.from(targetGroupElement.querySelectorAll('.plot-item')), e.clientX, e.clientY)
          : 0;

        if (existingTargetGroup) {
          updatedLayouts = updatedLayouts.map((group) => {
            if (group.groupKey === targetGroup) {
              const newItem = createLayoutItem(dropIndex * 2, 0, targetGroup, dragState.plotKey!);
              return {
                ...group,
                layouts: {
                  ...group.layouts,
                  lg: [...group.layouts.lg, newItem],
                },
              };
            }
            return group;
          });
        } else {
          // Создаем новую группу
          const newItem = createLayoutItem(0, 0, targetGroup, dragState.plotKey);
          updatedLayouts.push({
            groupKey: targetGroup,
            layouts: {
              lg: [newItem],
              md: [newItem],
              sm: [newItem],
              xs: [newItem],
              xxs: [newItem],
            },
          });
        }

        return updatedLayouts;
      });

      // Сбрасываем состояние перетаскивания только если это было окончание drag&drop
      if (e) {
        setDragState(initialDragState);
      }
    },
    [dragState, findDropIndex]
  );

  const handleAddGroup = useCallback(
    (e: React.MouseEvent<HTMLButtonElement>) => {
      const groupKey = e.currentTarget.getAttribute('data-index-group') ?? '0';
      addDashboardGroup(groupKey);
    },
    [addDashboardGroup]
  );

  const nextGroupKey = getNextGroupKey({ orderGroup });

  return (
    <div className="container-fluid">
      <div className={cn(dashboardLayoutEdit && 'dashboard-edit', className)}>
        {layouts.map(({ groupKey, layouts: groupLayouts }) => (
          <DashboardGroup key={groupKey} groupKey={groupKey}>
            {groups[groupKey]?.show !== false && (
              <ResponsiveGridLayout
                className={cn(
                  'layout',
                  'd-flex flex-row flex-wrap',
                  (!groupLayouts.lg.length && !dashboardLayoutEdit) || isEmbed ? 'pb-0' : 'pb-3'
                )}
                layouts={groupLayouts}
                breakpoints={BREAKPOINTS}
                margin={[0, 30]}
                cols={COLS}
                autoSize
                rowHeight={200}
                isDraggable={dashboardLayoutEdit}
                isResizable={dashboardLayoutEdit}
                compactType="horizontal"
                onDragStart={handleDragStart}
                onLayoutChange={handleLayoutChange}
                // onDragStop={(layout, oldItem, newItem, placeholder, event) => {
                //   handleLayoutChange(layout, { lg: layout }, event as unknown as MouseEvent);
                // }}
                isDroppable={dashboardLayoutEdit}
                preventCollision={true}
              >
                {groupLayouts.lg.map((layoutItem) => (
                  <DashboardPlotWrapper
                    key={layoutItem.i}
                    data-grid={layoutItem}
                    className={cn('plot-item p-1', dashboardLayoutEdit && css.cursorMove)}
                  >
                    <PlotView
                      className={cn(dashboardLayoutEdit && css.pointerEventsNone)}
                      key={layoutItem.i.split('::')[1]}
                      plotKey={layoutItem.i.split('::')[1]}
                      isDashboard
                    />
                  </DashboardPlotWrapper>
                ))}
              </ResponsiveGridLayout>
            )}
          </DashboardGroup>
        ))}

        {dashboardLayoutEdit && (
          <div className={cn('pb-5', dragState.isDragging ? '' : 'text-center container-xl')} data-group={nextGroupKey}>
            <h6 className="border-bottom"> </h6>
            {dragState.isDragging ? (
              <div className="text-center text-secondary py-4">Drop here for create new group</div>
            ) : (
              <Button
                className="btn btn-outline-primary py-4 w-100"
                data-index-group={nextGroupKey}
                onClick={handleAddGroup}
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
