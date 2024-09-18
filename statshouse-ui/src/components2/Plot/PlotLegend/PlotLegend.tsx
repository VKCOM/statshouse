// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useMemo, useRef } from 'react';
import cn from 'classnames';

import css from './style.module.css';
import { AlignByDot } from './AlignByDot';
import { PlotLegendMaxHost } from './PlotLegendMaxHost';
import { PlotValueUnit } from './PlotValueUnit';
import { METRIC_TYPE, MetricType } from 'api/enum';
import { type PlotValues } from 'store2/plotDataStore';
import { type PlotKey } from 'url2';
import { useResizeObserver } from 'hooks/useResizeObserver';
import { secondsRangeToString, timeShiftDesc } from 'view/utils2';
import { LegendItem } from 'components/UPlotWrapper';
import { Tooltip } from 'components/UI';

type PlotLegendProps = {
  plotKey: PlotKey;
  legend: LegendItem<PlotValues>[];
  compact?: boolean;
  onLegendFocus?: (index: number, focus: boolean) => void;
  onLegendShow?: (index: number, show: boolean, single: boolean) => void;
  className?: string;
  unit?: MetricType;
};

export const _PlotLegend: React.FC<PlotLegendProps> = ({
  plotKey,
  legend,
  onLegendShow,
  onLegendFocus,
  className,
  compact,
  unit = METRIC_TYPE.none,
}) => {
  const refDiv = useRef<HTMLDivElement>(null);
  const { width } = useResizeObserver(refDiv);
  const onFocus = useCallback(
    (event: React.MouseEvent) => {
      const index = parseInt(event.currentTarget.getAttribute('data-index') ?? '') || null;
      const focus = event.type === 'mouseover';
      index && onLegendFocus?.(index, focus);
    },
    [onLegendFocus]
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
    const h = legend.some((l) => l.values?.max_host);
    if (v && h) {
      return 0.3;
    } else if (v) {
      return 0.5;
    }
    return 0.8;
  }, [legend]);

  const legendWidth = useMemo(
    () => Math.min((Math.max(...legend.map((l) => l.label?.length ?? 0)) + 2) * 8, width * min),
    [min, legend, width]
  );

  return (
    <div ref={refDiv} className={cn(css.legend, compact && css.compact, className)}>
      {compact ? (
        <div className={css.innerLegendCompact}>
          {legend.slice(1).map((l, index) => (
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
              <Tooltip className={css.labelCompact} title={l.label}>
                {l.label}
              </Tooltip>
            </div>
          ))}
        </div>
      ) : (
        <table className={css.innerLegend}>
          <tbody>
            {legend.map((l, index) => (
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
                            {!!l.values.timeShift && (
                              <span className="text-secondary">{timeShiftDesc(l.values.timeShift)} </span>
                            )}
                            <span>{l.values.baseLabel}</span>
                          </>
                        ) : (
                          l.label
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
                          <AlignByDot
                            className={css.percentSuffix}
                            title={l.values?.percent}
                            value={l.values?.percent ?? ''}
                            unit="%"
                          />
                        </div>
                      </div>
                    </td>
                    {l.values?.max_host && (
                      <td className={css.maxHost}>
                        <PlotLegendMaxHost
                          value={l.noFocus ? l.values.top_max_host : l.values.max_host}
                          placeholder={
                            l.noFocus
                              ? l.values.top_max_host_percent
                              : `${l.values.max_host}: ${l.values.max_host_percent}`
                          }
                          plotKey={plotKey}
                          idx={index}
                        />
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
};

export const PlotLegend = memo(_PlotLegend);
