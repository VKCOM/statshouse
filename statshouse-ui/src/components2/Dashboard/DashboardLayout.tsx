import React, { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { isNotNil, toNumber, toNumberM } from 'common/helpers';
import css from './style.module.css';
import cn from 'classnames';
import { useResizeObserver } from 'view/utils';
import { useStatsHouseShallow } from 'store2';
import { GroupKey, PlotKey } from 'url2';
import { Button } from 'components';
import { ReactComponent as SVGPlus } from 'bootstrap-icons/icons/plus.svg';
import { DashboardPlotWrapper } from './DashboardPlotWrapper';
import { PlotView } from '../Plot';
import { toPlotKey } from '../../url/queryParams';
import { DashboardGroup } from './DashboardGroup';

function getStylePreview(
  targetRect: DOMRect,
  offset: { top: number; left: number },
  scale: number = 0.5
): React.CSSProperties {
  const o = {
    top: -targetRect.height / 2 + offset.top,
    left: -targetRect.width / 2 + offset.left,
  };
  return {
    width: targetRect.width,
    height: targetRect.height,
    transform: `matrix(${scale},0,0,${scale},${o.left * scale},${o.top * scale})`,
  };
}

function getClassRow(size?: string) {
  const col = toNumber(size);
  if (col != null) {
    return `col-${Math.round(12 / (col ?? 2))}`;
  }
  if (size) {
    return css[`dashRowSize_${size}`];
  }
  return css.dashRowSize_2;
}

function getGroupStyle(width: number, size?: string): React.CSSProperties {
  const w = Math.min(width, 1320);
  let cols = 2;
  switch (size) {
    case 'l':
      cols = 2;
      break;
    case 'm':
      cols = 3;
      break;
    case 's':
      cols = 4;
      break;
  }
  const col = toNumber(size);
  if (col != null) {
    cols = col;
  }
  let maxCols = Math.floor(width / (w / cols));
  return {
    '--base-cols': cols,
    '--max-cols': maxCols,
  } as React.CSSProperties;
}

export type DashboardLayoutProps = {
  className?: string;
};
export function _DashboardLayout({ className }: DashboardLayoutProps) {
  const { groups, orderGroup, dashboardLayoutEdit, isEmbed, groupPlots, addDashboardGroup, moveDashboardPlot } =
    useStatsHouseShallow(
      ({
        params: { groups, orderGroup },
        dashboardLayoutEdit,
        isEmbed,
        groupPlots,
        addDashboardGroup,
        moveDashboardPlot,
      }) => ({
        groups,
        orderGroup,
        dashboardLayoutEdit,
        isEmbed,
        groupPlots,
        addDashboardGroup,
        moveDashboardPlot,
      })
    );
  //   const moveAndResortPlot = useStore(selectorMoveAndResortPlot);
  const preview = useRef<HTMLDivElement>(null);
  const zone = useRef<HTMLDivElement>(null);
  const { width: zoneWidth } = useResizeObserver(zone);
  const [select, setSelect] = useState<PlotKey | null>(null);
  const [selectTarget, setSelectTarget] = useState<PlotKey | null>(null);
  const [selectTargetGroup, setSelectTargetGroup] = useState<GroupKey | null>(null);
  const [stylePreview, setStylePreview] = useState<React.CSSProperties>({});
  //
  const itemsGroup = useMemo(
    () =>
      orderGroup.map((groupKey) => ({
        groupKey,
        plots: groupPlots[groupKey] ?? [],
      })),
    // const plots = params.orderPlot.map((plotKey) => ({ plot: params.plots[plotKey]!, plotKey }));
    // const indexPlotMap = new Map<
    //   PlotKey,
    //   {
    //     plot: PlotParams;
    //     plotKey: PlotKey;
    //     groupKey: GroupKey;
    //     selected: boolean;
    //     // gpoupKeyInGroup: PlotKey;
    //   }
    // >();
    // const groups = params.orderGroup.map((groupKey) => ({ groupKey, group: params.groups[groupKey]! }));
    // let indexStart: number = 0;
    // const itemsG = groups.map(({ group, groupKey }) => {
    //   const r = {
    //     group,
    //     groupKey,
    //     plots: plots
    //       .slice(indexStart, indexStart + group.count)
    //       .map(({ plot, plotKey }) => ({ plot, plotKey, selected: select === plotKey })),
    //   };
    //   r.plots.forEach(({ plot, plotKey, selected }) => {
    //     indexPlotMap.set(plotKey, { plot, plotKey, selected, groupKey });
    //   });
    //   indexStart += group.count;
    //   return r;
    // });
    // if (selectTargetGroup != null && !itemsG[selectTargetGroup]) {
    //   itemsG[selectTargetGroup] = {
    //     group: { name: '', size: '2', show: true, count: 1, description: '' },
    //     indexGroup: selectTargetGroup,
    //     plots: [],
    //   };
    // }
    // if (select != null) {
    //   let drop = indexPlotMap.get(select);
    //   if (drop != null && selectTargetGroup != null && itemsG[selectTargetGroup] && itemsG[drop.indexGroup]) {
    //     const moveItem = itemsG[drop.indexGroup].plots.splice(drop.indexPlotInGroup, 1)[0];
    //     if (selectTarget != null) {
    //       let dropTarget = indexPlotMap.get(selectTarget);
    //       if (dropTarget?.indexGroup !== selectTargetGroup) {
    //         itemsG[selectTargetGroup].plots.push(moveItem);
    //       } else {
    //         itemsG[selectTargetGroup].plots.splice(
    //           select < selectTarget && drop.indexGroup === selectTargetGroup
    //             ? Math.max(0, dropTarget.indexPlotInGroup - 1)
    //             : dropTarget.indexPlotInGroup,
    //           0,
    //           moveItem
    //         );
    //       }
    //     } else {
    //       itemsG[selectTargetGroup].plots.push(moveItem);
    //     }
    //   }
    // }
    // return itemsG;
    [groupPlots, orderGroup]
  );

  const maxGroup = useMemo<GroupKey>(
    () => Math.max(...orderGroup.map(toNumberM).filter(isNotNil)).toString(),
    [orderGroup]
  );

  const nextGroupKey = useMemo(() => (+maxGroup + 1).toString(), [maxGroup]);

  const save = useCallback(
    (index: PlotKey | null, indexTarget: PlotKey | null, indexGroup: GroupKey | null) => {
      moveDashboardPlot(index, indexTarget, indexGroup);
    },
    [moveDashboardPlot]
  );

  const onDown = useCallback(
    (e: React.PointerEvent) => {
      const target = e.currentTarget as HTMLElement;
      let autoScroll: NodeJS.Timeout;
      let scrollSpeed = 0;

      const targetRect = target.getBoundingClientRect();
      const offset = { top: targetRect.top - e.clientY, left: targetRect.left - e.clientX };
      setStylePreview(getStylePreview(targetRect, offset));
      if (preview.current) {
        preview.current.style.transform = `matrix(1,0,0,1,${e.clientX},${e.clientY})`;
      }
      let index = toPlotKey(target.getAttribute('data-index'));
      let indexTarget: PlotKey | null = index;
      let indexGroup: GroupKey | null = null;

      setSelect(index);
      setSelectTarget(indexTarget);
      const move = (e: PointerEvent) => {
        if (preview.current) {
          preview.current.style.transform = `matrix(1,0,0,1,${e.clientX},${e.clientY})`;
        }
        const dropElement = document.elementsFromPoint(e.clientX, e.clientY);
        const elem = dropElement.find((e) => e.getAttribute('data-index'));
        if (elem) {
          indexTarget = toPlotKey(elem.getAttribute('data-index'), '0');
          setSelectTarget(indexTarget);
          //   if (indexT !== index) {
          //     if (indexT < (indexTarget ?? 0)) {
          //       indexTarget = Math.max(0, indexT);
          //     } else {
          //       indexTarget = Math.max(0, indexT + 1);
          //     }
          //     // setSelectTarget(indexTarget);
          //   }
        } else {
          indexTarget = null;
          setSelectTarget(indexTarget);
        }
        indexGroup = dropElement.find((e) => e.getAttribute('data-group'))?.getAttribute('data-group') ?? null;
        setSelectTargetGroup(indexGroup);
        if (window.innerHeight - e.clientY < window.innerHeight / 10) {
          scrollSpeed = 10 * (50 / Math.max(0, window.innerHeight - e.clientY));
        } else if (e.clientY < window.innerHeight / 10) {
          scrollSpeed = -10 * (50 / Math.max(0, e.clientY));
        } else {
          scrollSpeed = 0;
        }
        clearInterval(autoScroll);
        autoScroll = setInterval(() => {
          if (scrollSpeed) {
            window.scrollBy({
              top: scrollSpeed,
              // @ts-ignore
              behavior: 'instant',
            });
          }
        }, 10);
        e.preventDefault();
      };
      const end = () => {
        clearInterval(autoScroll);
        save(index, indexTarget, indexGroup);
        setSelect(null);
        setSelectTarget(null);
        setSelectTargetGroup(null);
        document.removeEventListener('pointerup', end);
        document.removeEventListener('pointermove', move);
        e.preventDefault();
      };

      document.addEventListener('pointerup', end, { passive: false });
      document.addEventListener('pointermove', move, { passive: false });
      e.preventDefault();
    },
    [save]
  );
  useEffect(() => {
    if (dashboardLayoutEdit) {
      const prev = (e: TouchEvent) => {
        if ((e.target as HTMLElement).getAttribute('data-index')) {
          e.preventDefault();
        }
      };
      const z = zone.current;
      z?.addEventListener('touchstart', prev, { passive: false });
      return () => {
        z?.removeEventListener('touchstart', prev);
      };
    }
  }, [dashboardLayoutEdit]);

  const onAddGroup = useCallback(
    (e: React.MouseEvent<HTMLElement>) => {
      const groupKey = e.currentTarget.getAttribute('data-index-group') ?? '0';
      addDashboardGroup(groupKey);
    },
    [addDashboardGroup]
  );

  return (
    <div className="container-fluid">
      <div
        className={cn(
          select !== null ? css.cursorDrag : css.cursorDefault,
          // dashboardLayoutEdit && 'dashboard-edit',
          className
        )}
        ref={zone}
      >
        {itemsGroup.map(({ groupKey, plots }) => (
          <DashboardGroup key={groupKey} groupKey={groupKey}>
            {groups[groupKey]?.show !== false && (
              <div className={cn('mx-auto', css.dashRowWidth)} style={getGroupStyle(zoneWidth, groups[groupKey]?.size)}>
                <div
                  className={cn(
                    'd-flex flex-row flex-wrap',
                    (!plots.length && !dashboardLayoutEdit) || isEmbed ? 'pb-0' : 'pb-3',
                    toNumber(groups[groupKey]?.size ?? '2') != null ? 'container-xl' : null
                  )}
                >
                  {plots.map((plotKey) => (
                    <DashboardPlotWrapper
                      key={plotKey}
                      className={cn(
                        'plot-item p-1',
                        getClassRow(groups[groupKey]?.size),
                        select === plotKey && 'opacity-50',
                        dashboardLayoutEdit && css.cursorMove
                      )}
                      data-index={plotKey}
                      onPointerDown={dashboardLayoutEdit ? onDown : undefined}
                    >
                      <PlotView
                        className={cn(dashboardLayoutEdit && css.pointerEventsNone)}
                        key={plotKey}
                        plotKey={plotKey}
                      />
                    </DashboardPlotWrapper>
                  ))}
                </div>
              </div>
            )}
          </DashboardGroup>
        ))}
        {dashboardLayoutEdit &&
          (select !== null /*&& maxGroup + 1 === itemsGroup.length*/ ? (
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
        <div hidden={select === null} className="position-fixed opacity-75 top-0 start-0" ref={preview}>
          {select !== null && (
            <div style={stylePreview}>
              <PlotView className={css.pointerEventsNone} key={select} plotKey={select} />
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
export const DashboardLayout = memo(_DashboardLayout);
