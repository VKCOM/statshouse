// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useMemo, useRef } from 'react';
import cn from 'classnames';

import css from './style.module.css';
import { AlignByDot } from './AlignByDot';
import { PlotValueUnit } from './PlotValueUnit';
import { METRIC_TYPE, MetricType } from '@/api/enum';
import { PlotValues, usePlotsDataStore } from '@/store2/plotDataStore';
import { useResizeObserver } from '@/hooks/useResizeObserver';
import { formatPercent, secondsRangeToString, timeShiftDesc } from '@/view/utils2';
import { LegendItem } from '@/components/UPlotWrapper';
import { Tooltip } from '@/components/UI';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useShallow } from 'zustand/react/shallow';
import { emptyArray } from '@/common/helpers';
import { PlotLegendMaxHost } from '@/components2/Plot/PlotLegend/PlotLegendMaxHost';
import { pxPerChar } from '@/common/settings';

type PlotLegendProps = {
  legend: LegendItem<PlotValues>[];
  compact?: boolean;
  onLegendFocus?: (index: number, focus: boolean) => void;
  onLegendShow?: (index: number, show: boolean, single: boolean) => void;
  className?: string;
  unit?: MetricType;
  visible?: boolean;
  priority?: number;
};

export const PlotLegend = memo(function PlotLegend({
  legend,
  onLegendShow,
  onLegendFocus,
  className,
  compact,
  unit = METRIC_TYPE.none,
  visible = false,
  priority = 2,
}: PlotLegendProps) {
  const {
    plot: { id, maxHost },
  } = useWidgetPlotContext();
  const { seriesTimeShift, data } = usePlotsDataStore(
    useShallow(
      useCallback(
        ({ plotsData }) => ({
          seriesTimeShift: plotsData[id]?.seriesTimeShift,
          data: plotsData[id]?.data ?? emptyArray,
        }),
        [id]
      )
    )
  );
  // const [maxHostLists, maxHostValues, legendMaxHostWidth] = useMaxHosts();
  const refDiv = useRef<HTMLDivElement>(null);
  const { width } = useResizeObserver(refDiv);
  const onFocus = useCallback(
    (event: React.MouseEvent) => {
      const index = parseInt(event.currentTarget.getAttribute('data-index') ?? '') || null;
      const focus = event.type === 'mouseover';
      if (index) {
        onLegendFocus?.(index, focus);
      }
    },
    [onLegendFocus]
  );

  const totalLine = useMemo(
    () =>
      data?.[0]?.map((_, index) => {
        let s = 0;
        for (let i = 1; i < data.length; i++) {
          if (data[i][index] != null && seriesTimeShift && seriesTimeShift[i - 1] <= 0) {
            s += data[i][index] ?? 0;
          }
        }
        return s;
      }) ?? [],
    [data, seriesTimeShift]
  );

  const onShow = useCallback(
    (event: React.MouseEvent) => {
      const index = parseInt(event.currentTarget.getAttribute('data-index') ?? '') || null;
      if (index) {
        const show = !legend[index]?.show;
        onLegendShow?.(index, show, event.ctrlKey || event.metaKey);
      }
    },
    [legend, onLegendShow]
  );
  const min = useMemo<number>(() => {
    const v = legend.some((l) => l.values?.value);
    if (v && maxHost) {
      return 0.3;
    } else if (v) {
      return 0.5;
    }
    return 0.8;
  }, [legend, maxHost]);

  const legendWidth = useMemo(
    () =>
      Math.min(
        (Math.max(
          ...legend.map((l, index) => {
            const tsw = timeShiftDesc(seriesTimeShift?.[index - 1] ?? 0).length;
            return (l.label?.length ?? 0) + tsw;
          })
        ) +
          2) *
          pxPerChar,
        width * min
      ),
    [legend, width, min, seriesTimeShift]
  );

  const legendRows = useMemo(
    () =>
      legend.map((l, indexL) => {
        const idx = l.values?.idx;
        const rawValue = l.values?.rawValue;
        const percent = (idx != null && rawValue != null && formatPercent(rawValue / totalLine[idx])) || '';
        let timeShift = '';
        let baseLabel = l.label;
        if (seriesTimeShift && indexL && seriesTimeShift[indexL - 1] < 0) {
          timeShift = timeShiftDesc(seriesTimeShift[indexL - 1]);
          baseLabel = l.label.replace(timeShift + ' ', '');
        }
        return {
          ...l,
          percent,
          baseLabel,
          timeShift,
        };
      }),
    [legend, seriesTimeShift, totalLine]
  );

  return (
    <div ref={refDiv} className={cn(css.legend, compact && css.compact, className)}>
      {compact ? (
        <div className={css.innerLegendCompact}>
          {legendRows.slice(1).map((l, index) => (
            <div
              key={index}
              className={cn(css.labelOuter, !l.show && css.hide)}
              data-index={index + 1}
              onMouseOut={onFocus}
              onMouseOver={onFocus}
              onClick={onShow}
            >
              <div
                className={css.marker}
                style={{ border: l.stroke && `${l.width}px solid ${l.stroke}`, background: l.fill }}
              ></div>
              <Tooltip className={css.labelCompact} title={l.baseLabel}>
                {l.baseLabel}
              </Tooltip>
            </div>
          ))}
        </div>
      ) : (
        <table className={css.innerLegend}>
          <tbody>
            {legendRows.map((l, index) => (
              <tr
                key={index}
                data-index={index}
                className={cn('', !l.noFocus && l.focus && css.focus, !l.show && css.hide)}
                style={{ opacity: l.alpha }}
                onMouseOut={onFocus}
                onMouseOver={onFocus}
              >
                <td colSpan={index === 0 ? 3 : 1} data-index={index} onClick={onShow}>
                  <div className={cn(css.labelOuter, index === 0 && css.time)}>
                    <div
                      className={css.marker}
                      style={{
                        borderColor: l.stroke,
                        borderWidth: l.width ? `${l.width}px` : '1px',
                        background: l.fill,
                        borderStyle: l.dash?.length ? 'dashed' : l.stroke ? 'solid' : 'none',
                      }}
                    ></div>
                    <Tooltip
                      style={{
                        width: index !== 0 ? `${legendWidth}px` : undefined,
                        minWidth: index === 0 ? 250 : undefined,
                      }}
                      className={css.label}
                      title={l.label}
                    >
                      {index !== 0 ? (
                        l.values ? (
                          <>
                            {l.timeShift && <span className="text-secondary">{l.timeShift} </span>}
                            <span>{l.baseLabel}</span>
                          </>
                        ) : (
                          l.baseLabel
                        )
                      ) : (
                        l.value || ' '
                      )}
                    </Tooltip>
                  </div>
                </td>
                {index !== 0 && (
                  <>
                    <td className={css.value}>
                      <div className="d-flex justify-content-end w-100">
                        <div className="w-0 flex-grow-1">
                          <AlignByDot
                            title={<PlotValueUnit unit={unit} value={l.values?.rawValue} />}
                            value={l.values?.value ?? '—'}
                          />
                        </div>
                      </div>
                    </td>
                    <td className={css.percent}>
                      <div className="d-flex justify-content-end w-100">
                        <div className="w-0 flex-grow-1">
                          <AlignByDot className={css.percentSuffix} title={l.percent} value={l.percent} unit="%" />
                        </div>
                      </div>
                    </td>
                    {maxHost && (
                      <td className={css.maxHost}>
                        {!!seriesTimeShift && seriesTimeShift[index - 1] <= 0 && !!l.values?.rawValue && (
                          <PlotLegendMaxHost
                            seriesIdx={index - 1}
                            idx={l.noFocus ? undefined : l.values?.idx}
                            visible={visible}
                            priority={priority}
                          />
                        )}
                      </td>
                    )}
                    <td className={css.timeShift}>
                      <Tooltip
                        className="text-secondary text-truncate"
                        title={!!l.deltaTime && `${secondsRangeToString(Math.abs(l.deltaTime), true)} ago`}
                      >
                        {!!l.deltaTime && `${secondsRangeToString(Math.abs(l.deltaTime), true)} ago`}
                      </Tooltip>
                    </td>
                  </>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
});
