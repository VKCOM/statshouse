import React, { MutableRefObject, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { EventObserver } from '../../../common/EventObserver';
import { UPlotWrapperPropsHooks } from '../../UPlotWrapper';
import uPlot from 'uplot';
import { useResizeObserver } from '../../../view/utils';
import css from './style.module.css';
import { PlotStore, selectorParamsPlotsByIndex, selectorPlotsData, useStore } from '../../../store';
import { PlotEventFlag } from './PlotEventFlag';

type Flag = {
  x: number;
  idx: number;
  key: string;
  opacity: number;
  groups: { color: string; idx: number; x: number }[];
};

function getEventLines(eventsIndex: number[], eventsData: PlotStore[], u: uPlot, flagWidth: number): Flag[] {
  const flags: Record<string, Flag> = {};
  eventsIndex.forEach((indexEvent) => {
    const time = eventsData[indexEvent].data[0] ?? [];
    const data = eventsData[indexEvent].data.slice(1);
    const maxY = Math.max(...data.flat().filter(Boolean).map(Number));
    let prevIdx = 0;
    for (let idx = 0, iMax = time.length; idx < iMax; idx++) {
      for (let s = 0, sMax = data.length; s < sMax; s++) {
        const val = data[s][idx];
        if (val) {
          const x = u.valToPos(time[idx], 'x') ?? 0;
          if (flags[prevIdx] && x - flags[prevIdx].x > flagWidth * 1.5) {
            prevIdx = idx;
          }
          flags[prevIdx] ??= {
            groups: [],
            key: `${idx}`,
            idx,
            opacity: 0.3,
            x,
          };
          flags[prevIdx].groups.push({
            color: eventsData[indexEvent].series[s].stroke?.toString() ?? '',
            idx,
            x,
          });
          const opacity = Math.max(0.3, val / maxY);
          flags[prevIdx].opacity = Math.min(1, Math.max(flags[prevIdx].opacity, opacity));
        }
      }
    }
  });
  return Object.values(flags);
}

export type PlotEventOverlayProps = {
  indexPlot: number;
  hooks?: MutableRefObject<EventObserver<keyof UPlotWrapperPropsHooks>>;
  flagHeight?: number;
};

export function PlotEventOverlay({ indexPlot, hooks, flagHeight = 8 }: PlotEventOverlayProps) {
  const uPlotRef = useRef<uPlot>();
  const uRefDiv = useRef<HTMLDivElement>(null);
  const { width, height } = useResizeObserver(uRefDiv);
  const selectorParamsPlot = useMemo(() => selectorParamsPlotsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const { events: eventsIndex } = useStore(selectorParamsPlot);
  const eventsData = useStore(selectorPlotsData);
  const flagWidth = flagHeight * 1.5;
  const [lines, setLines] = useState<Flag[]>([]);

  const update = useCallback(() => {
    if (uPlotRef.current) {
      setLines(getEventLines(eventsIndex, eventsData, uPlotRef.current, flagWidth));
    } else {
      setLines([]);
    }
  }, [eventsData, eventsIndex, flagWidth]);

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
      <svg xmlns="http://www.w3.org/2000/svg" width={width} height={height} viewBox={`0 0 ${width} ${height}`}>
        <defs>
          <path
            id="flagPath"
            d={`M0,0 h${flagWidth} l-${flagHeight / 2},${flagHeight / 2} l${flagHeight / 2},${
              flagHeight / 2
            } h-${flagWidth} z`}
          />
          <path id="flagPath2" d={`M0,0 l${flagWidth},${flagHeight / 2} l${-flagWidth},${flagHeight / 2} z`} />
          <clipPath id="flag">
            <use href="#flagPath" />
          </clipPath>
        </defs>
        <g stroke="gray" strokeWidth="0.5" fill="gray">
          {lines.map((r) => (
            <PlotEventFlag
              key={r.key}
              index={r.idx}
              flagWidth={flagWidth}
              flagHeight={flagHeight}
              height={height}
              x={r.x}
              opacity={r.opacity}
              groups={r.groups}
            />
          ))}
        </g>
      </svg>
    </div>
  );
}
