import React, { MutableRefObject, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { EventObserver } from '../../../common/EventObserver';
import { UPlotWrapperPropsHooks } from '../../UPlotWrapper';
import uPlot from 'uplot';
import { useResizeObserver } from '../../../view/utils';
import css from './style.module.css';
import { selectorParamsPlotsByIndex, selectorPlotsData, useStore } from '../../../store';

export type PlotEventOverlayProps = {
  indexPlot: number;
  hooks?: MutableRefObject<EventObserver<keyof UPlotWrapperPropsHooks>>;
  flagHeight?: number;
};

type Flag = {
  x: number;
  key: string;
  opacity: number;
  groups: { color: string }[];
};
export function PlotEventOverlay({ indexPlot, hooks, flagHeight = 8 }: PlotEventOverlayProps) {
  const uPlotRef = useRef<uPlot>();
  const uRefDiv = useRef<HTMLDivElement>(null);
  const { width, height } = useResizeObserver(uRefDiv);
  const selectorParamsPlot = useMemo(() => selectorParamsPlotsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const { events: eventsIndex } = useStore(selectorParamsPlot);
  const eventsData = useStore(selectorPlotsData);

  const [lines, setLines] = useState<Flag[]>([]);

  const update = useCallback(() => {
    if (uPlotRef.current) {
      const flags: Record<string, Flag> = {};
      eventsIndex.forEach((indexEvent) => {
        const time = eventsData[indexEvent].data[0] ?? [];
        const data = eventsData[indexEvent].data.slice(1);
        const maxY = Math.max(...data.flat().filter(Boolean).map(Number));
        data.forEach((d, groupIndex) => {
          d.forEach((val, idx) => {
            if (val) {
              flags[idx] ??= {
                groups: [],
                key: `${idx}`,
                opacity: 0.3,
                x: uPlotRef.current?.valToPos(time[idx], 'x') ?? 0,
              };
              flags[idx].groups.push({
                color: eventsData[indexEvent].series[groupIndex].stroke?.toString() ?? '',
              });
              const opacity = Math.max(0.3, val / maxY);
              flags[idx].opacity = Math.min(1, Math.max(flags[idx].opacity, opacity));
            }
          });
        });
      });
      setLines(Object.values(flags));
    } else {
      setLines([]);
    }
  }, [eventsData, eventsIndex]);

  useEffect(() => {
    if (hooks) {
      const offInit = hooks.current.on(
        'onInit',
        (u: uPlot) => {
          uPlotRef.current = u;
        },
        true
      );
      const offDestroy = hooks.current.on('onDestroy', () => {
        uPlotRef.current = undefined;
      });

      return () => {
        offInit();
        offDestroy();
      };
    }
  }, [hooks]);

  useEffect(() => {
    update();
  }, [update, width, height, eventsData, eventsIndex]);
  return (
    <div ref={uRefDiv} className={css.overlay}>
      <svg xmlns="http://www.w3.org/2000/svg" width={width} height={height}>
        <defs>
          <clipPath id="flag">
            <path
              d={`M0,0 h${flagHeight} l-${flagHeight / 2},${flagHeight / 2} l${flagHeight / 2},${
                flagHeight / 2
              } h-${flagHeight} z`}
            />
          </clipPath>
        </defs>
        {/*<rect x={0} y={0} width={width} height={height} stroke="black" strokeWidth="1" fill="transparent" />*/}
        {lines.map((r) => (
          <g
            key={r.key}
            transform={`translate(${r.x}, 0)`}
            stroke="green"
            strokeWidth="0.5"
            opacity={r.opacity}
            fill="green"
          >
            <line x1="0" x2="0" y1="0" y2={height} />
            <g clipPath="url(#flag)" className={css.overlayFlag}>
              {r.groups.map((g, indexG, arrG) => (
                <rect
                  key={indexG}
                  x="0"
                  y={(flagHeight / arrG.length) * indexG}
                  width={flagHeight}
                  height={flagHeight / arrG.length}
                  fill={g.color}
                  strokeWidth="0"
                />
              ))}
            </g>
          </g>
        ))}
      </svg>
    </div>
  );
}
