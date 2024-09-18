import React, { memo, MutableRefObject, useCallback, useEffect, useRef, useState } from 'react';
import { EventObserver } from '../../../common/EventObserver';
import { UPlotWrapperPropsHooks } from '../../UPlotWrapper';
import uPlot from 'uplot';
import css from './style.module.css';
import { PlotStore, selectorParams, selectorPlotsData, useStore } from '../../../store';
import { PlotEventFlag } from './PlotEventFlag';
import { TimeRange } from '../../../common/TimeRange';
import { useResizeObserver } from '../../../hooks/useResizeObserver';

type Flag = {
  x: number;
  idx: number;
  key: string;
  opacity: number;
  range: TimeRange;
  agg: string;
  plotIndex: number;
  groups: { color: string; idx: number; x: number; plotIndex: number }[];
};

function getEventLines(eventsIndex: number[], eventsData: PlotStore[], u: uPlot, flagWidth: number): Flag[] {
  const aFlags: Flag[] = [];
  eventsIndex.forEach((indexEvent) => {
    const time = eventsData[indexEvent]?.data[0] ?? [];
    const data =
      eventsData[indexEvent]?.data
        .slice(1)
        .filter((d, indexData) => !eventsData[indexEvent].seriesTimeShift[indexData]) ?? [];
    const values = data.flat().filter(Boolean).map(Number);
    const maxY = values.reduce((res, item) => Math.max(res, item), values[0]);
    for (let idx = 0, iMax = time.length; idx < iMax; idx++) {
      for (let s = 0, sMax = data.length; s < sMax; s++) {
        const val = data[s][idx];
        if (val != null) {
          const x = Math.round(Math.min(100000, u.valToPos(time[idx], 'x') ?? 0));
          if (x > 0) {
            aFlags.push({
              groups: [
                {
                  plotIndex: indexEvent,
                  color: eventsData[indexEvent].series[s].stroke?.toString() ?? '',
                  idx,
                  x,
                },
              ],
              key: `${idx}`,
              idx,
              opacity: Math.min(1, Math.max(0.3, val / maxY)),
              x,
              plotIndex: indexEvent,
              range: new TimeRange({ from: time[idx], to: time[idx + 1] }),
              agg: `${time[idx + 1] - time[idx]}s`,
            });
          }
        }
      }
    }
  });
  aFlags.sort((a, b) => a.x - b.x);
  const flagsGroup: Record<string, Flag> = {};
  let prevFlag: Flag;
  aFlags.forEach((info) => {
    if (!prevFlag || Math.abs(info.x - prevFlag.x) > flagWidth * 1.5) {
      prevFlag = info;
    }
    if (flagsGroup[prevFlag.idx]) {
      flagsGroup[prevFlag.idx].groups = [...flagsGroup[prevFlag.idx].groups, ...info.groups];
      flagsGroup[prevFlag.idx].range = new TimeRange({ from: flagsGroup[prevFlag.idx].range.from, to: info.range.to });
      flagsGroup[prevFlag.idx].opacity = Math.min(1, Math.max(flagsGroup[prevFlag.idx].opacity, info.opacity));
    } else {
      flagsGroup[prevFlag.idx] = info;
    }
  });
  return Object.values(flagsGroup);
}

export type PlotEventOverlayProps = {
  indexPlot: number;
  hooks?: MutableRefObject<EventObserver<keyof UPlotWrapperPropsHooks>>;
  flagHeight?: number;
  compact?: boolean;
};

export function _PlotEventOverlay({ indexPlot, hooks, flagHeight = 8, compact }: PlotEventOverlayProps) {
  const uPlotRef = useRef<uPlot>();
  const uRefDiv = useRef<HTMLDivElement>(null);
  const { width, height } = useResizeObserver(uRefDiv);
  const params = useStore(selectorParams);
  const plot = params.plots[indexPlot];
  const eventsData = useStore(selectorPlotsData);
  const [plotWidth, setPlotWidth] = useState(width);

  const flagWidth = flagHeight * 1.5;
  const [lines, setLines] = useState<Flag[]>([]);

  const update = useCallback(() => {
    if (uPlotRef.current) {
      setLines(getEventLines(plot.events, eventsData, uPlotRef.current, flagWidth));
      setPlotWidth(uPlotRef.current?.bbox.width || 0);
    } else {
      setLines([]);
    }
  }, [eventsData, flagWidth, plot.events]);

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
      const offDraw = hooks.current.on('onDraw', () => {
        update();
      });

      return () => {
        offInit();
        offDestroy();
        offDraw();
      };
    }
  }, [hooks, update]);

  useEffect(() => {
    update();
  }, [update, width, height, eventsData]);
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
              plots={params.plots}
              plotWidth={plotWidth}
              range={r.range}
              agg={r.agg}
              width={width}
              key={r.key}
              index={r.idx}
              flagWidth={flagWidth}
              flagHeight={flagHeight}
              height={height}
              x={r.x}
              opacity={r.opacity}
              groups={r.groups}
              small={compact}
            />
          ))}
        </g>
      </svg>
    </div>
  );
}

export const PlotEventOverlay = memo(_PlotEventOverlay);
