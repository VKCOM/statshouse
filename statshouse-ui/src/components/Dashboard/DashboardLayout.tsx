// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import cn from 'classnames';
import { ReactComponent as SVGChevronDown } from 'bootstrap-icons/icons/chevron-down.svg';
import { ReactComponent as SVGChevronRight } from 'bootstrap-icons/icons/chevron-right.svg';
import PlotView from '../../view/PlotView';
import {
  selectorDashboardLayoutEdit,
  selectorDashboardPlotList,
  selectorIsServer,
  selectorMoveAndResortPlot,
  selectorParams,
  selectorSetGroupName,
  selectorSetGroupShow,
  useStore,
} from '../../store';
import { PlotParams } from '../../common/plotQueryParams';

import css from './style.module.css';

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

export type DashboardLayoutProps = {
  yAxisSize?: number;
  className?: string;
};

export function DashboardLayout({ yAxisSize = 54, className }: DashboardLayoutProps) {
  const params = useStore(selectorParams);
  const dashboardPlots = useStore(selectorDashboardPlotList);
  const moveAndResortPlot = useStore(selectorMoveAndResortPlot);
  const dashboardLayoutEdit = useStore(selectorDashboardLayoutEdit);
  const setGroupName = useStore(selectorSetGroupName);
  const setGroupShow = useStore(selectorSetGroupShow);
  const isServer = useStore(selectorIsServer);
  const preview = useRef<HTMLDivElement>(null);
  const zone = useRef<HTMLDivElement>(null);
  const [select, setSelect] = useState<number | null>(null);
  const [selectTarget, setSelectTarget] = useState<number | null>(null);
  const [selectTargetGroup, setSelectTargetGroup] = useState<number | null>(null);
  const [stylePreview, setStylePreview] = useState<React.CSSProperties>({});

  const itemsGroup = useMemo(() => {
    const i = dashboardPlots.slice();
    if (select !== null && select >= 0 && selectTarget !== null && selectTarget >= 0) {
      let drop = i.splice(select, 1)[0];
      if (selectTargetGroup !== null && selectTargetGroup >= 0 && drop.group !== selectTargetGroup) {
        drop = {
          ...drop,
          group: selectTargetGroup,
        };
      }
      i.splice(select < selectTarget ? Math.max(0, selectTarget - 1) : selectTarget, 0, drop);
    }
    return i.reduce((res, value) => {
      res[value.group] = res[value.group] ?? [];
      res[value.group].push(value);
      return res;
    }, [] as { plot: PlotParams; group: number; indexPlot: number }[][]);
  }, [dashboardPlots, select, selectTarget, selectTargetGroup]);

  const maxGroup = useMemo(() => dashboardPlots.reduce((res, item) => Math.max(res, item.group), 0), [dashboardPlots]);

  const save = useCallback(
    (index: number, indexTarget: number, indexGroup: number) => {
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
      let indexTarget = index;
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
            if (indexT < indexTarget) {
              indexTarget = Math.max(0, indexT);
            } else {
              indexTarget = Math.max(0, indexT + 1);
            }
            setSelectTarget(indexTarget);
          }
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
    (e: React.MouseEvent<HTMLDivElement>) => {
      const index = parseInt(e.currentTarget.getAttribute('data-group') ?? '0');
      setGroupShow(index, (s) => !s);
    },
    [setGroupShow]
  );
  return (
    <div className={cn(select !== null ? css.cursorDrag : css.cursorDefault, className)} ref={zone}>
      {itemsGroup.map((group, indexGroup) => (
        <div key={indexGroup} className="pb-5" data-group={indexGroup}>
          <h6
            hidden={itemsGroup.length <= 1 && !dashboardLayoutEdit && !params.dashboard?.groupInfo?.[indexGroup]?.name}
            className="border-bottom pb-1"
          >
            {dashboardLayoutEdit && isServer ? (
              <input
                className="form-control"
                data-group={indexGroup.toString()}
                value={params.dashboard?.groupInfo?.[indexGroup]?.name ?? ''}
                onInput={onEditGroupName}
                placeholder="Enter group name"
              />
            ) : (
              <div className="d-flex flex-row" role="button" onClick={onGroupShowToggle} data-group={indexGroup}>
                {isServer && (
                  <div className="me-2">
                    {params.dashboard?.groupInfo?.[indexGroup]?.show === false ? (
                      <SVGChevronRight />
                    ) : (
                      <SVGChevronDown />
                    )}
                  </div>
                )}
                <div className="flex-grow-1">{params.dashboard?.groupInfo?.[indexGroup]?.name ?? ' '}</div>
              </div>
            )}
          </h6>
          {params.dashboard?.groupInfo?.[indexGroup]?.show !== false && (
            <div className="d-flex flex-row flex-wrap">
              {group.map((value) => (
                <div
                  key={value.indexPlot}
                  className={cn(
                    'plot-item col-6 p-1',
                    select === value.indexPlot && 'opacity-50',
                    dashboardLayoutEdit && css.cursorMove
                  )}
                  data-index={value.indexPlot}
                  onPointerDown={dashboardLayoutEdit ? onDown : undefined}
                >
                  <PlotView
                    className={cn(dashboardLayoutEdit && css.pointerEventsNone)}
                    key={value.indexPlot}
                    indexPlot={value.indexPlot}
                    compact={true}
                    yAxisSize={yAxisSize}
                    dashboard={true}
                    group="1"
                  />
                </div>
              ))}
            </div>
          )}
        </div>
      ))}
      {isServer && select !== null && maxGroup + 1 === itemsGroup.length && (
        <div className="pb-5" data-group={maxGroup + 1}>
          <h6 className="border-bottom"> </h6>
        </div>
      )}
      <div hidden={select === null} className="position-fixed opacity-75 top-0 start-0" ref={preview}>
        {select !== null && (
          <div style={stylePreview}>
            <PlotView
              className={css.pointerEventsNone}
              key={select}
              indexPlot={select}
              compact={true}
              yAxisSize={yAxisSize}
              dashboard={true}
              group="1"
            />
          </div>
        )}
      </div>
    </div>
  );
}
