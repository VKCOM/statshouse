import React, { useCallback, useMemo } from 'react';
import css from './style.module.css';

export type PlotEventFlagProps = {
  height: number;
  index: number;
  x: number;
  opacity: number;
  flagWidth: number;
  flagHeight: number;
  groups: { color: string; idx: number; x: number }[];
  onMouseOut?: (index: number) => void;
  onMouseOver?: (index: number) => void;
};
export function PlotEventFlag({
  index,
  x,
  height,
  opacity,
  groups,
  flagWidth,
  flagHeight,
  onMouseOut,
  onMouseOver,
}: PlotEventFlagProps) {
  const flagGroup = useMemo(() => {
    const idx = groups[0]?.idx ?? 0;
    return groups.filter((g) => g.idx === idx).slice(0, 5);
  }, [groups]);
  const flagGroup2 = useMemo(() => {
    const idx = groups[0]?.idx ?? 0;
    return groups.filter((g) => g.idx !== idx).slice(0, 5);
  }, [groups]);
  const flagGroupHeight = flagHeight / flagGroup.length;
  const _onMouseOut = useCallback(() => onMouseOut?.bind(undefined, index), [index, onMouseOut]);
  const _onMouseOver = useCallback(() => onMouseOver?.bind(undefined, index), [index, onMouseOver]);
  return (
    <g transform={`translate(${x}, 4)`} opacity={opacity}>
      <line x1="0" x2="0" y1="0" y2={height} />
      <g className={css.overlayFlag} onMouseOut={_onMouseOut} onMouseOver={_onMouseOver}>
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
                  y={flagGroupHeight * indexG}
                  width={flagWidth}
                  height={flagGroupHeight}
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
    </g>
  );
}
