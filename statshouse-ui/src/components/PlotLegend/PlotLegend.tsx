import React, { useCallback, useMemo, useRef } from 'react';
import { LegendItem } from '../UPlotWrapper';
import cn from 'classnames';
import { PlotValues } from '../../store';

import css from './style.module.css';
import { AlignByDot } from './AlignByDot';
import { useResizeObserver } from '../../view/utils';

type PlotLegendProps = {
  legend: LegendItem<PlotValues>[];
  compact?: boolean;
  onLegendFocus?: (index: number, focus: boolean) => void;
  onLegendShow?: (index: number, show: boolean, single: boolean) => void;
  className?: string;
};

export const PlotLegend: React.FC<PlotLegendProps> = ({ legend, onLegendShow, onLegendFocus, className, compact }) => {
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
    () => Math.min(Math.max(...legend.map((l) => l.label.length)) * 8, width * min),
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
              <div className={css.labelCompact} title={l.label}>
                {l.label}
              </div>
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
                className={cn('', l.focus && css.focus, !l.show && css.hide)}
                style={{ opacity: l.alpha }}
                onMouseOut={onFocus}
                onMouseOver={onFocus}
                onClick={onShow}
              >
                <th colSpan={index === 0 ? 2 : 1}>
                  <div className={cn(css.labelOuter, index === 0 && css.time)}>
                    <div
                      className={css.marker}
                      style={{ border: l.stroke && `${l.width}px solid ${l.stroke}`, background: l.fill }}
                    ></div>
                    <div
                      style={{ width: index !== 0 ? `${legendWidth}px` : undefined }}
                      className={css.label}
                      title={l.label}
                    >
                      {l.label}
                    </div>
                    {index === 0 && (
                      <div className={css.timeValue}>
                        <div></div>
                        <AlignByDot value={l.value?.toString() ?? '—'} />
                      </div>
                    )}
                  </div>
                </th>
                {index !== 0 && (
                  <>
                    <td className={css.value}>
                      <AlignByDot value={l.values?.value ?? '—'} />
                    </td>
                    <td className={css.percent}>
                      <AlignByDot className={css.percentSuffix} value={l.values?.percent ?? ''} unit="%" />
                    </td>
                    {l.values?.max_host && <td className={css.maxHost}>{l.values.max_host}</td>}
                    {l.values?.max_host_percent && (
                      <td className={css.maxHostPercent}>
                        <AlignByDot className={css.percentSuffix} value={l.values?.max_host_percent ?? ''} unit="%" />
                      </td>
                    )}
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
