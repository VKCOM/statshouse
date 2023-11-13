// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import cn from 'classnames';
import { ReactComponent as SVGChevronDown } from 'bootstrap-icons/icons/chevron-down.svg';
import { ReactComponent as SVGChevronRight } from 'bootstrap-icons/icons/chevron-right.svg';
import { ReactComponent as SVGChevronCompactUp } from 'bootstrap-icons/icons/chevron-compact-up.svg';
import { ReactComponent as SVGChevronCompactDown } from 'bootstrap-icons/icons/chevron-compact-down.svg';
import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';
import { ReactComponent as SVGPlus } from 'bootstrap-icons/icons/plus.svg';
import { PlotView } from '../Plot';
import {
  addDashboardGroup,
  moveGroup,
  removeDashboardGroup,
  selectorDashboardLayoutEdit,
  selectorMoveAndResortPlot,
  selectorParams,
  selectorSetGroupName,
  selectorSetGroupShow,
  selectorSetGroupSize,
  useStore,
} from '../../store';

import css from './style.module.css';
import { PlotParams } from '../../url/queryParams';
import { toNumber } from '../../common/helpers';
import { useResizeObserver } from '../../view/utils';
import { Button } from '../UI';
import { DashboardPlotWrapper } from './DashboardPlotWrapper';

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
  yAxisSize?: number;
  className?: string;
  embed?: boolean;
};

export function DashboardLayout({ yAxisSize = 54, embed, className }: DashboardLayoutProps) {
  const params = useStore(selectorParams);
  const moveAndResortPlot = useStore(selectorMoveAndResortPlot);
  const dashboardLayoutEdit = useStore(selectorDashboardLayoutEdit);
  const setGroupName = useStore(selectorSetGroupName);
  const setGroupShow = useStore(selectorSetGroupShow);
  const setGroupSize = useStore(selectorSetGroupSize);
  const preview = useRef<HTMLDivElement>(null);
  const zone = useRef<HTMLDivElement>(null);
  const { width: zoneWidth } = useResizeObserver(zone);
  const [select, setSelect] = useState<number | null>(null);
  const [selectTarget, setSelectTarget] = useState<number | null>(null);
  const [selectTargetGroup, setSelectTargetGroup] = useState<number | null>(null);
  const [stylePreview, setStylePreview] = useState<React.CSSProperties>({});

  const itemsGroup = useMemo(() => {
    const plots = params.plots.map((plot, indexPlot) => ({ plot, indexPlot }));
    const indexPlotMap = new Map<
      number,
      { plot: PlotParams; indexPlot: number; indexGroup: number; selected: boolean; indexPlotInGroup: number }
    >();
    const groups = params.dashboard?.groupInfo?.length
      ? params.dashboard?.groupInfo
      : [{ count: plots.length, name: '', size: '2', show: true }];
    let indexStart: number = 0;
    const itemsG = groups.map((group, indexGroup) => {
      const r = {
        group,
        indexGroup,
        plots: plots
          .slice(indexStart, indexStart + group.count)
          .map(({ plot, indexPlot }) => ({ plot, indexPlot, selected: select === indexPlot })),
      };
      r.plots.forEach(({ plot, indexPlot, selected }, indexPlotInGroup) => {
        indexPlotMap.set(indexPlot, { plot, indexPlot, selected, indexGroup, indexPlotInGroup });
      });
      indexStart += group.count;
      return r;
    });
    if (selectTargetGroup != null && !itemsG[selectTargetGroup]) {
      itemsG[selectTargetGroup] = {
        group: { name: '', size: '2', show: true, count: 1 },
        indexGroup: selectTargetGroup,
        plots: [],
      };
    }
    if (select != null) {
      let drop = indexPlotMap.get(select);
      if (drop != null && selectTargetGroup != null && itemsG[selectTargetGroup] && itemsG[drop.indexGroup]) {
        const moveItem = itemsG[drop.indexGroup].plots.splice(drop.indexPlotInGroup, 1)[0];
        if (selectTarget != null) {
          let dropTarget = indexPlotMap.get(selectTarget);
          if (dropTarget?.indexGroup !== selectTargetGroup) {
            itemsG[selectTargetGroup].plots.push(moveItem);
          } else {
            itemsG[selectTargetGroup].plots.splice(
              select < selectTarget && drop.indexGroup === selectTargetGroup
                ? Math.max(0, dropTarget.indexPlotInGroup - 1)
                : dropTarget.indexPlotInGroup,
              0,
              moveItem
            );
          }
        } else {
          itemsG[selectTargetGroup].plots.push(moveItem);
        }
      }
    }
    return itemsG;
  }, [params.dashboard?.groupInfo, params.plots, select, selectTarget, selectTargetGroup]);

  const maxGroup = useMemo(() => {
    const groups = params.dashboard?.groupInfo?.length
      ? params.dashboard?.groupInfo
      : [{ count: params.plots.length, name: '', size: '2', show: true }];
    return groups.reduce((res, group, indexGroup) => {
      if (group.count) {
        res = Math.max(res, indexGroup);
      }
      return res;
    }, 0);
  }, [params.dashboard?.groupInfo, params.plots.length]);

  const save = useCallback(
    (index: number | null, indexTarget: number | null, indexGroup: number | null) => {
      moveAndResortPlot(index, indexTarget, indexGroup);
    },
    [moveAndResortPlot]
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
      let index = parseInt(target.getAttribute('data-index') ?? '-1');
      let indexTarget: number | null = index;
      let indexGroup = -1;

      setSelect(index);
      setSelectTarget(indexTarget);
      const move = (e: PointerEvent) => {
        if (preview.current) {
          preview.current.style.transform = `matrix(1,0,0,1,${e.clientX},${e.clientY})`;
        }
        const dropElement = document.elementsFromPoint(e.clientX, e.clientY);
        const elem = dropElement.find((e) => e.getAttribute('data-index'));
        if (elem) {
          const indexT = parseInt(elem.getAttribute('data-index') ?? '-1');
          if (indexT !== index) {
            if (indexT < (indexTarget ?? 0)) {
              indexTarget = Math.max(0, indexT);
            } else {
              indexTarget = Math.max(0, indexT + 1);
            }
            setSelectTarget(indexTarget);
          }
        } else {
          indexTarget = null;
          setSelectTarget(indexTarget);
        }
        indexGroup = parseInt(
          dropElement.find((e) => e.getAttribute('data-group'))?.getAttribute('data-group') ?? '-1'
        );

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
  const onEditGroupName = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const index = parseInt(e.currentTarget.getAttribute('data-group') ?? '0');
      const name = e.currentTarget.value;
      setGroupName(index, name);
    },
    [setGroupName]
  );
  const onGroupShowToggle = useCallback(
    (e: React.MouseEvent<HTMLElement>) => {
      const index = parseInt(e.currentTarget.getAttribute('data-group') ?? '0');
      setGroupShow(index, (s) => !s);
    },
    [setGroupShow]
  );

  const onEditGroupSize = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) => {
      const index = parseInt(e.currentTarget.getAttribute('data-group') ?? '0');
      const size = e.currentTarget.value ?? '2';
      setGroupSize(index, size);
    },
    [setGroupSize]
  );

  const onAddGroup = useCallback((e: React.MouseEvent<HTMLElement>) => {
    const index = parseInt(e.currentTarget.getAttribute('data-index-group') ?? '0');
    addDashboardGroup(index);
  }, []);
  const onRemoveGroup = useCallback((e: React.MouseEvent<HTMLElement>) => {
    const index = parseInt(e.currentTarget.getAttribute('data-index-group') ?? '0');
    removeDashboardGroup(index);
  }, []);
  const onMoveGroupUp = useCallback((e: React.MouseEvent<HTMLElement>) => {
    const index = parseInt(e.currentTarget.getAttribute('data-index-group') ?? '0');
    moveGroup(index, -1);
  }, []);
  const onMoveGroupDown = useCallback((e: React.MouseEvent<HTMLElement>) => {
    const index = parseInt(e.currentTarget.getAttribute('data-index-group') ?? '0');
    moveGroup(index, 1);
  }, []);

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
        {itemsGroup.map(({ group, indexGroup, plots }) => (
          <div key={indexGroup} className={cn(!embed ? 'pb-5' : 'pb-2')} data-group={indexGroup}>
            <h6
              hidden={itemsGroup.length <= 1 && group.show !== false && !dashboardLayoutEdit && !group.name}
              className="border-bottom pb-1"
            >
              {dashboardLayoutEdit ? (
                <div className="d-flex p-0 container-xl">
                  <Button className="btn me-2" onClick={onGroupShowToggle} data-group={indexGroup}>
                    {group.show === false ? <SVGChevronRight /> : <SVGChevronDown />}
                  </Button>
                  <div className="input-group">
                    <input
                      className="form-control"
                      data-group={indexGroup}
                      value={group.name ?? ''}
                      onInput={onEditGroupName}
                      placeholder="Enter group name"
                    />
                    <select
                      className="form-select flex-grow-0 w-auto"
                      data-group={indexGroup}
                      value={group.size?.toString() || '2'}
                      onChange={onEditGroupSize}
                    >
                      <option value="2">L, 2 per row</option>
                      <option value="l">L, auto width</option>
                      <option value="3">M, 3 per row</option>
                      <option value="m">M, auto width</option>
                      <option value="4">S, 4 per row</option>
                      <option value="s">S, auto width</option>
                    </select>
                    <div className="d-flex flex-column">
                      <Button
                        className="btn btn-sm btn-outline-primary py-0 rounded-0"
                        style={{ height: 19 }}
                        title="Group move up"
                        data-index-group={indexGroup}
                        disabled={indexGroup === 0}
                        onClick={onMoveGroupUp}
                      >
                        <SVGChevronCompactUp className="align-baseline" />
                      </Button>
                      <Button
                        className="btn btn-sm btn-outline-primary py-0 rounded-0 border-top-0"
                        style={{ height: 19 }}
                        title="Group move down"
                        data-index-group={indexGroup}
                        disabled={indexGroup === itemsGroup.length - 1}
                        onClick={onMoveGroupDown}
                      >
                        <SVGChevronCompactDown className="align-baseline" />
                      </Button>
                    </div>
                    <Button
                      className="btn btn-outline-primary px-1"
                      title="Add group before this"
                      data-index-group={indexGroup}
                      onClick={onAddGroup}
                    >
                      <SVGPlus />
                    </Button>
                    {itemsGroup.length > 1 && plots.length === 0 && (
                      <Button
                        className="btn btn-outline-danger px-1"
                        title="Remove group"
                        data-index-group={indexGroup}
                        onClick={onRemoveGroup}
                      >
                        <SVGTrash />
                      </Button>
                    )}
                  </div>
                </div>
              ) : (
                <div
                  className="d-flex container-xl flex-row"
                  role="button"
                  onClick={onGroupShowToggle}
                  data-group={indexGroup}
                >
                  <div className="me-2">{group.show === false ? <SVGChevronRight /> : <SVGChevronDown />}</div>

                  <div className="flex-grow-1">
                    {group.name || <span className="text-body-tertiary">Group {indexGroup + 1}</span>}
                  </div>
                </div>
              )}
            </h6>
            {group.show !== false && (
              <div className={cn('mx-auto', css.dashRowWidth)} style={getGroupStyle(zoneWidth, group.size)}>
                <div
                  className={cn(
                    'd-flex flex-row flex-wrap',
                    (!plots.length && !dashboardLayoutEdit) || embed ? 'pb-0' : 'pb-5',
                    toNumber(group.size ?? '2') != null ? 'container-xl' : null
                  )}
                >
                  {plots.map(({ plot, indexPlot, selected }) => (
                    <DashboardPlotWrapper
                      key={indexPlot}
                      className={cn(
                        'plot-item p-1',
                        getClassRow(group.size),
                        selected && 'opacity-50',
                        dashboardLayoutEdit && css.cursorMove
                      )}
                      data-index={indexPlot}
                      onPointerDown={dashboardLayoutEdit ? onDown : undefined}
                    >
                      <PlotView
                        className={cn(dashboardLayoutEdit && css.pointerEventsNone)}
                        key={indexPlot}
                        indexPlot={indexPlot}
                        type={plot.type}
                        compact={true}
                        embed={embed}
                        yAxisSize={yAxisSize}
                        dashboard={true}
                        group="1"
                      />
                    </DashboardPlotWrapper>
                  ))}
                </div>
              </div>
            )}
          </div>
        ))}
        {dashboardLayoutEdit &&
          (select !== null && maxGroup + 1 === itemsGroup.length ? (
            <div className="pb-5" data-group={maxGroup + 1}>
              <h6 className="border-bottom"> </h6>
              <div className="text-center text-secondary py-4">Drop here for create new group</div>
            </div>
          ) : (
            <div className="pb-5 text-center container-xl" data-group={maxGroup + 1}>
              <h6 className="border-bottom"> </h6>
              <Button
                className="btn btn-outline-primary py-4 w-100"
                data-index-group={maxGroup + 1}
                onClick={onAddGroup}
              >
                <SVGPlus /> Add new group
              </Button>
            </div>
          ))}
        <div hidden={select === null} className="position-fixed opacity-75 top-0 start-0" ref={preview}>
          {select !== null && (
            <div style={stylePreview}>
              <PlotView
                className={css.pointerEventsNone}
                key={select}
                indexPlot={select}
                type={params.plots[select].type}
                compact={true}
                embed={embed}
                yAxisSize={yAxisSize}
                dashboard={true}
                group="1"
              />
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
