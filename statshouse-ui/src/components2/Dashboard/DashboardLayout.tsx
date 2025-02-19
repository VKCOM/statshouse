// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { toNumber } from '@/common/helpers';
import css from './style.module.css';
import cn from 'classnames';
import { useStatsHouseShallow } from '@/store2';
import { GroupKey, PlotKey } from '@/url2';
import { Button } from '@/components/UI';
import { ReactComponent as SVGPlus } from 'bootstrap-icons/icons/plus.svg';
import { DashboardPlotWrapper } from './DashboardPlotWrapper';
import { toPlotKey } from '@/url/queryParams';
import { DashboardGroup } from '@/components2';
import { produce } from 'immer';
import { getNextGroupKey } from '@/store2/urlStore/updateParamsPlotStruct';
import { prepareItemsGroup } from '@/common/prepareItemsGroup';
import { useResizeObserver } from '@/hooks/useResizeObserver';
import { PlotWidget } from '@/components2/PlotWidgets/PlotWidget';

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
  const maxCols = Math.floor(width / (w / cols));
  return {
    '--base-cols': cols,
    '--max-cols': maxCols,
  } as React.CSSProperties;
}

export type DashboardLayoutProps = {
  className?: string;
};

export const DashboardLayout = memo(function DashboardLayout({ className }: DashboardLayoutProps) {
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
  const preview = useRef<HTMLDivElement>(null);
  const zone = useRef<HTMLDivElement>(null);
  const { width: zoneWidth } = useResizeObserver(zone);
  const [select, setSelect] = useState<PlotKey | null>(null);
  const [selectTarget, setSelectTarget] = useState<PlotKey | null>(null);
  const [selectTargetGroup, setSelectTargetGroup] = useState<GroupKey | null>(null);
  const [stylePreview, setStylePreview] = useState<React.CSSProperties>({});
  const originItemsGroupRef = useRef(prepareItemsGroup({ groups, orderGroup, orderPlot }));
  const itemsGroupRef = useRef(prepareItemsGroup({ groups, orderGroup, orderPlot }));
  const [itemsGroup, setItemsGroup] = useState(originItemsGroupRef.current);

  useEffect(() => {
    originItemsGroupRef.current = prepareItemsGroup({ groups, orderGroup, orderPlot });
    setItemsGroup(originItemsGroupRef.current);
  }, [groups, orderGroup, orderPlot]);

  useEffect(() => {
    setItemsGroup(
      produce((ig) => {
        if (selectTargetGroup != null && select != null && selectTarget !== select) {
          ig.forEach((g) => {
            if (g.groupKey !== selectTargetGroup) {
              g.plots = g.plots.filter((p) => p !== select);
            }
          });
          const groupIndex = ig.findIndex(({ groupKey }) => groupKey === selectTargetGroup);
          if (groupIndex === -1) {
            ig.push({ plots: [select], groupKey: selectTargetGroup });
          } else {
            const selectIndex = ig[groupIndex].plots.indexOf(select);
            ig[groupIndex].plots = ig[groupIndex].plots.filter((p) => p !== select);
            let selectTargetIndex = ig[groupIndex].plots.indexOf(selectTarget!);
            if (selectTargetIndex > -1 && selectIndex > -1 && selectTargetIndex >= selectIndex) {
              selectTargetIndex++;
            }
            if (selectTargetIndex > -1) {
              ig[groupIndex].plots.splice(selectTargetIndex, 0, select);
            } else {
              ig[groupIndex].plots.push(select);
            }
          }
        }
      })
    );
  }, [select, selectTarget, selectTargetGroup]);
  useEffect(() => {
    itemsGroupRef.current = itemsGroup;
  }, [itemsGroup]);

  const nextGroupKey = useMemo(() => getNextGroupKey({ orderGroup }), [orderGroup]);

  const save = useCallback(
    (index: PlotKey | null, _indexTarget: PlotKey | null, _indexGroup: GroupKey | null) => {
      if (index != null) {
        setNextDashboardSchemePlot(itemsGroupRef.current);
      }
    },
    [setNextDashboardSchemePlot]
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
      const dropElement = document.elementsFromPoint(e.clientX, e.clientY);
      const index = toPlotKey(target.getAttribute('data-index'));
      let indexTarget: PlotKey | null = index;
      let indexGroup: GroupKey | null =
        dropElement.find((e) => e.getAttribute('data-group'))?.getAttribute('data-group') ?? null;

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
          dashboardLayoutEdit && 'dashboard-edit',
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
                        'plot-item p-1 d-flex',
                        getClassRow(groups[groupKey]?.size),
                        select === plotKey && 'opacity-50',
                        dashboardLayoutEdit && css.cursorMove
                      )}
                      data-index={plotKey}
                      onPointerDown={dashboardLayoutEdit ? onDown : undefined}
                    >
                      <PlotWidget
                        className={cn(dashboardLayoutEdit && css.pointerEventsNone)}
                        key={plotKey}
                        plotKey={plotKey}
                        isDashboard
                        isEmbed={isEmbed}
                        fixRatio
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
              <PlotWidget className={css.pointerEventsNone} key={select} plotKey={select} fixRatio />
            </div>
          )}
        </div>
      </div>
    </div>
  );
});
