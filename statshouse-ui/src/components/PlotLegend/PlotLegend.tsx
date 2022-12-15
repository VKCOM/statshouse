import React, { useCallback } from 'react';
import { LegendItem } from '../UPlotWrapper';
import cn from 'classnames';
import { PlotValues } from '../../store';

type PlotLegendProps = {
  legend: LegendItem<PlotValues>[];
  onLegendFocus?: (index: number, focus: boolean) => void;
  onLegendShow?: (index: number, show: boolean, single: boolean) => void;
  className?: string;
};

export const PlotLegend: React.FC<PlotLegendProps> = ({ legend, onLegendShow, onLegendFocus, className }) => {
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
      const show = event.currentTarget.className.includes('u-off');

      if (index) {
        onLegendShow?.(index, show, event.ctrlKey || event.metaKey);
      }
    },
    [onLegendShow]
  );
  return (
    <div className={cn('plot-legend', className)}>
      <table className="u-legend u-inline u-live">
        <tbody>
          {legend.map((l, index) => (
            <tr
              key={index}
              data-index={index}
              className={cn('u-series', l.focus && 'plot-legend-focus', !l.show && 'u-off')}
              style={{ opacity: l.alpha }}
              onMouseOut={onFocus}
              onMouseOver={onFocus}
              onClick={onShow}
            >
              <th>
                <div
                  className="u-marker"
                  style={{ border: l.stroke && `${l.width}px solid ${l.stroke}`, background: l.fill }}
                ></div>
                <div className="u-label" title={l.label}>
                  {l.label}
                </div>
              </th>
              {/*<td className="u-value" data-name="time" colSpan={2}>*/}
              {/*  {l.value}*/}
              {/*</td>*/}
              {index === 0 ? (
                <td className="u-value" data-name="time" colSpan={2}>
                  {l.value}
                </td>
              ) : (
                <>
                  <td className="u-value" data-name="value">
                    {l.values?.value ?? '—'}
                  </td>
                  <td className="u-value" data-name="percent">
                    {l.values?.percent}
                  </td>
                  {l.values?.max_host && (
                    <td className="u-value" data-name="max_host">
                      {l.values?.max_host}
                    </td>
                  )}
                  {l.values?.max_host_percent && (
                    <td className="u-value" data-name="max_host_percent">
                      {l.values?.max_host_percent}
                    </td>
                  )}
                </>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};
