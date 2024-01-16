import React, { memo, useCallback, useMemo, useRef } from 'react';
import css from './style.module.css';
import { Popper } from '../../UI';
import { useDebounceState } from '../../../hooks';
import { PlotEventOverlayTable } from './PlotEventOverlayTable';
import { TimeRange } from '../../../common/TimeRange';
import cn from 'classnames';
import { PlotParams } from '../../../url/queryParams';
import { isNotNil, uniqueArray } from '../../../common/helpers';

export type PlotEventFlagProps = {
  plots: PlotParams[];
  range: TimeRange;
  agg: string;
  width: number;
  plotWidth: number;
  height: number;
  index: number;
  x: number;
  opacity: number;
  flagWidth: number;
  flagHeight: number;
  groups: { color: string; idx: number; x: number; plotIndex: number }[];
  small?: boolean;
};
export function _PlotEventFlag({
  plots,
  range,
  agg,
  plotWidth,
  x,
  height,
  opacity,
  groups,
  flagWidth,
  flagHeight,
  small,
}: PlotEventFlagProps) {
  const refFlag = useRef<SVGRectElement>(null);
  const flagGroup = useMemo(() => {
    const idx = groups[0]?.idx ?? 0;
    return groups.filter((g) => g.idx === idx).slice(0, 5);
  }, [groups]);
  const flagGroup2 = useMemo(() => {
    const idx = groups[0]?.idx ?? 0;
    return groups.filter((g) => g.idx !== idx).slice(0, 5);
  }, [groups]);
  const flagGroupHeight = flagHeight / flagGroup.length;
  const flagGroupHeight2 = flagHeight / flagGroup2.length;
  const [, debounceHover, setHover] = useDebounceState(false, 200);
  const _onMouseOut = useCallback(() => {
    setHover(false);
  }, [setHover]);
  const _onMouseOver = useCallback(() => {
    setHover(true);
  }, [setHover]);
  const plotsInfo = useMemo(
    () =>
      uniqueArray(groups.map((g) => g.plotIndex))
        .map((idx) => plots[idx])
        .filter(isNotNil),
    [groups, plots]
  );
  return (
    <g transform={`translate(${x}, 4)`} opacity={opacity}>
      <line x1="0" x2="0" y1="0" y2={height} />

      <g className={css.overlayFlag} onMouseOut={_onMouseOut} onMouseOver={_onMouseOver}>
        <rect
          ref={refFlag}
          x="0"
          y="0"
          width={flagWidth}
          height={flagHeight + (flagGroup2.length ? flagHeight + 3 : 0)}
          fill="transparent"
          strokeWidth="0"
          data-x={x}
        />
        <g clipPath="url(#flag)" strokeWidth="0">
          {flagGroup.map((g, indexG) => (
            <rect
              key={indexG}
              x="0"
              y={flagGroupHeight * indexG}
              width={flagWidth}
              height={flagGroupHeight}
              fill={g.color}
            />
          ))}
        </g>
        <use href="#flagPath" fill="transparent" />
        {!!flagGroup2.length && (
          <g transform={`translate(3, ${flagHeight + 3}) scale(0.95)`}>
            <g clipPath="url(#flag)" strokeWidth="0">
              {flagGroup2.map((g, indexG) => (
                <rect
                  key={indexG}
                  x="0"
                  y={flagGroupHeight2 * indexG}
                  width={flagWidth}
                  height={flagGroupHeight2}
                  fill={g.color}
                />
              ))}
            </g>
            <use href="#flagPath" fill="transparent" />
            <line x1="0" x2="0" y1={flagHeight} y2={flagHeight + 5} />
            <line x1="0" x2="-3" y1={flagHeight + 5} y2={flagHeight + 10} />
          </g>
        )}
      </g>
      <Popper targetRef={refFlag} horizontal="out-right" vertical="out-bottom" fixed={false} show={debounceHover}>
        <div
          className={cn(
            'card overflow-auto d-flex flex-column',
            css.overlayCardTable,
            small && css.overlayCardTableSmall,
            small ? 'p-1' : 'p-2'
          )}
          onMouseOut={_onMouseOut}
          onMouseOver={_onMouseOver}
          style={{ minWidth: 100, minHeight: 20 }}
        >
          {plotsInfo.map((p, key) => (
            <PlotEventOverlayTable key={key} plot={p} range={range} agg={agg} width={plotWidth} />
          ))}
        </div>
      </Popper>
    </g>
  );
}

export const PlotEventFlag = memo(_PlotEventFlag);
