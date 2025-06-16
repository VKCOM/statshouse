// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, ReactNode, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import uPlot from 'uplot';
import { debug } from '@/common/debug';
import { deepClone, labelAsString } from '@/common/helpers';
import { useResizeObserver } from '@/hooks/useResizeObserver';

export type LegendItem<T = Record<string, unknown>> = {
  label: string;
  width: number;
  fill?: string;
  stroke?: string;
  show: boolean;
  value?: string;
  values?: T;
  alpha?: number;
  focus?: boolean;
  dash?: number[];
  deltaTime?: number;
  noFocus?: boolean;
};
export type UPlotWrapperPropsOpts = Partial<uPlot.Options>;
export type UPlotWrapperPropsScales = Record<string, { min: number; max: number }>;
export type UPlotWrapperPropsHooks = {
  onInit?: (u: uPlot, opts: uPlot.Options, data: uPlot.AlignedData) => void;
  onDestroy?: (u: uPlot) => void;
  onDraw?: (u: uPlot) => void;
  onDrawAxes?: (u: uPlot) => void;
  onDrawClear?: (u: uPlot) => void;
  onReady?: (u: uPlot) => void;
  onSetData?: (u: uPlot) => void;
  onSyncRect?: (u: uPlot, rect: DOMRect) => void;
  onSetSeries?: (u: uPlot, seriesIdx: number | null, opts: uPlot.Series) => void;
  onAddSeries?: (u: uPlot, seriesIdx: number) => void;
  onDelSeries?: (u: uPlot, seriesIdx: number) => void;
  onDrawSeries?: (u: uPlot, seriesIdx: number) => void;
  onSetScale?: (u: uPlot, scaleKey: string) => void;
  onSetSize?: (u: uPlot) => void;
  onSetCursor?: (u: uPlot) => void;
  onSetLegend?: (u: uPlot) => void;
  onSetSelect?: (u: uPlot) => void;
};

export type UPlotWrapperProps<LV = Record<string, unknown>> = {
  opts?: UPlotWrapperPropsOpts;
  data?: uPlot.AlignedData;
  scales?: UPlotWrapperPropsScales;
  series?: uPlot.Series[];
  bands?: uPlot.Band[];
  legendTarget?: HTMLDivElement | null;
  onUpdatePreview?: (u: uPlot) => void;
  onUpdateLegend?: React.Dispatch<React.SetStateAction<LegendItem<LV>[]>>;
  className?: string;
  children?: ReactNode;
} & UPlotWrapperPropsHooks;

const microTask =
  typeof queueMicrotask === 'undefined' ? (fn: () => void) => Promise.resolve().then(fn) : queueMicrotask;

function readLegend<LV = Record<string, unknown>>(u: uPlot): LegendItem<LV>[] {
  let lastIdx = (u.data?.[0]?.length ?? 0) - 1;
  const xMax = u.scales.x?.max ?? 0;
  while (lastIdx >= 0 && u.data?.[0]?.[lastIdx] > xMax) {
    lastIdx--;
  }
  return u.series.map((s, index) => {
    let idx = u.legend.idxs?.[index] ?? null;
    let lastTime = '';
    let deltaTime = 0;
    let noFocus = false;
    if (idx === null && u.data?.[index]?.length) {
      noFocus = true;
      let lastIndex = lastIdx;
      while (typeof u.data[index][lastIndex] !== 'number' && lastIndex > 0) {
        lastIndex--;
      }
      idx = lastIndex;
      if (idx / u.data[index].length < 0.9) {
        deltaTime = ((u.data?.[0] && u.data[0][idx]) ?? 0) - xMax;
      }
    }
    if (idx != null && index === 0) {
      lastTime =
        (typeof s.value === 'function' ? s.value(u, u.data[index][idx], 0, idx) : s.value)
          ?.toString()
          .replace('--', '') ?? ''; // replace '--' uplot
    }
    return {
      label: labelAsString(s.label) ?? '',
      width:
        (u.legend.markers?.width instanceof Function ? u.legend.markers?.width(u, index) : u.legend.markers?.width) ??
        1,
      fill: s.fill instanceof Function ? s.fill(u, index)?.toString() : s.fill?.toString(),
      stroke: s.stroke instanceof Function ? s.stroke(u, index)?.toString() : s.stroke?.toString(),
      show: s.show ?? false,
      value: u.legend.values?.[index]?.['_']?.toString().replace('--', '') || lastTime, // replace '--' uplot
      values: typeof idx === 'number' ? (s.values?.(u, index, idx) as LV) : undefined,
      alpha: s.alpha,
      dash: s.dash,
      deltaTime,
      noFocus,
    };
  });
}

const defaultData: uPlot.AlignedData = [[]];
const defaultSeries: uPlot.Series[] = [];
const defaultScales: UPlotWrapperPropsScales = {};
const defaultBands: uPlot.Band[] = [];

function UPlotWrapperNoMemo<LV = Record<string, unknown>>({
  opts,
  data = defaultData,
  series = defaultSeries,
  scales = defaultScales,
  bands = defaultBands,
  legendTarget,
  onUpdatePreview,
  onUpdateLegend,
  className,
  onInit,
  onSetCursor,
  onDelSeries,
  onDestroy,
  onAddSeries,
  onDrawAxes,
  onDrawClear,
  onDrawSeries,
  onSetData,
  onSetLegend,
  onSetScale,
  onSetSeries,
  onSetSize,
  onSyncRect,
  onReady,
  onDraw,
  onSetSelect,
  children,
}: UPlotWrapperProps<LV>) {
  const uRef = useRef<uPlot>(undefined);
  const uRefDiv = useRef<HTMLDivElement>(null);
  const { width, height } = useResizeObserver(uRefDiv);
  const hooksEvent = useRef<UPlotWrapperPropsHooks>({});
  const [seriesFocus, setSeriesFocus] = useState<null | number>(null);
  const [legend, setLegend] = useState<LegendItem<LV>[]>([]);

  useEffect(() => {
    hooksEvent.current = {
      onInit,
      onSetCursor,
      onDelSeries,
      onDestroy,
      onAddSeries,
      onDrawAxes,
      onDrawClear,
      onDrawSeries,
      onSetData,
      onSetLegend,
      onSetScale,
      onSetSeries,
      onSetSize,
      onSyncRect,
      onReady,
      onDraw,
      onSetSelect,
    };
  }, [
    onInit,
    onSetCursor,
    onDelSeries,
    onDestroy,
    onAddSeries,
    onDrawAxes,
    onDrawClear,
    onDrawSeries,
    onSetData,
    onSetLegend,
    onSetScale,
    onSetSeries,
    onSetSize,
    onSyncRect,
    onReady,
    onDraw,
    onSetSelect,
  ]);

  const updateScales = useCallback((scales?: UPlotWrapperPropsScales) => {
    if (scales && uRef.current) {
      uRef.current?.batch((u: uPlot) => {
        Object.entries(scales).forEach(([key, scale]) => {
          u.setScale(key, { ...scale });
        });
      });
      uRef.current?.redraw(true, true);
    }
  }, []);

  const updateSeries = useCallback((series: uPlot.Series[]) => {
    if (uRef.current) {
      if (series.length) {
        uRef.current.batch((u: uPlot) => {
          const show: Record<string, boolean | undefined> = {};
          for (let i = u.series.length - 1; i > 0; i--) {
            show[labelAsString(u.series[i].label)!] = u.series[i].show;
            u.delSeries(i);
          }

          let nextSeries = series.map((s) => ({
            ...s,
            show:
              typeof show[labelAsString(s.label)!] !== 'undefined' ? show[labelAsString(s.label)!] : (s.show ?? true),
          }));

          if (nextSeries.every((s) => !s.show)) {
            nextSeries = nextSeries.map((s) => ({ ...s, show: true }));
          }

          nextSeries.forEach((s) => {
            u.addSeries(s);
          });
        });
      } else {
        uRef.current.batch((u: uPlot) => {
          for (let i = u.series.length - 1; i > 0; i--) {
            u.delSeries(i);
          }
        });
      }
    }
  }, []);

  const updateData = useCallback((data: uPlot.AlignedData) => {
    if (uRef.current) {
      uRef.current.setData(deepClone(data));
    }
  }, []);

  const updateBands = useCallback((bands: uPlot.Band[]) => {
    if (uRef.current) {
      uRef.current.delBand();
      bands.forEach((band) => {
        uRef.current?.addBand(deepClone(band));
      });
    }
  }, []);

  const moveLegend = useCallback(() => {
    if (uRef.current && legendTarget) {
      const legend = uRef.current.root.querySelector('.u-legend');
      if (legend) {
        const prevLegend = legendTarget.querySelector('.u-legend');
        if (prevLegend && legend !== prevLegend) {
          legendTarget.removeChild(prevLegend);
        }
        legendTarget.append(legend);
      }
    }
  }, [legendTarget]);

  useEffect(() => {
    moveLegend();
  }, [moveLegend]);

  const uPlotPlugin = useMemo(
    (): uPlot.Plugin => ({
      opts: (_u, opts) => {
        delete opts.title;
        return opts;
      },
      hooks: {
        init: (u, opts, data) => {
          hooksEvent.current.onInit?.(u, opts, data);
        },
        destroy: (u) => {
          hooksEvent.current.onDestroy?.(u);
        },
        draw: (u) => {
          hooksEvent.current.onDraw?.(u);
        },
        drawAxes: (u) => {
          hooksEvent.current.onDrawAxes?.(u);
        },
        drawClear: (u) => {
          hooksEvent.current.onDrawClear?.(u);
        },
        ready: (u) => {
          setLegend(readLegend(u));
          hooksEvent.current.onReady?.(u);
        },
        setData: (u) => {
          setLegend(readLegend(u));
          hooksEvent.current.onSetData?.(u);
        },
        syncRect: (u, rect) => {
          hooksEvent.current.onSyncRect?.(u, rect);
        },
        setSeries: (u, seriesIdx, opts) => {
          if ((opts as { focus?: boolean }).focus) {
            setSeriesFocus(seriesIdx);
          }
          setLegend(readLegend(u));
          hooksEvent.current.onSetSeries?.(u, seriesIdx, opts);
        },
        addSeries: (u, seriesIdx) => {
          setLegend(readLegend(u));
          hooksEvent.current.onAddSeries?.(u, seriesIdx);
        },
        delSeries: (u, seriesIdx) => {
          setLegend(readLegend(u));
          hooksEvent.current.onDelSeries?.(u, seriesIdx);
        },
        drawSeries: (u, seriesIdx) => {
          hooksEvent.current.onDrawSeries?.(u, seriesIdx);
        },
        setScale: (u, scaleKey) => {
          hooksEvent.current.onSetScale?.(u, scaleKey);
        },
        setSize: (u) => {
          hooksEvent.current.onSetSize?.(u);
        },
        setCursor: (u) => {
          hooksEvent.current.onSetCursor?.(u);
        },
        setLegend: (u) => {
          setLegend(readLegend(u));
          hooksEvent.current.onSetLegend?.(u);
        },
        setSelect: (u) => {
          hooksEvent.current.onSetSelect?.(u);
        },
      },
    }),
    []
  );

  useEffect(() => {
    const legendF = legend.map((l, li) => ({
      ...l,
      dash: l.dash && [...l.dash],
      focus: li === seriesFocus ? true : l.focus,
    }));
    onUpdateLegend?.(legendF);
  }, [legend, seriesFocus, onUpdateLegend]);

  useEffect(
    () => () => {
      if (uRef.current) {
        debug.log('%cUPlotWrapper destroy', 'color:blue;');
        uRef.current?.destroy();
        uRef.current = undefined;
      }
    },
    [opts]
  );

  useEffect(() => {
    if (width === 0 || uRef.current) {
      return;
    }
    const opt: uPlot.Options = {
      width: uRefDiv.current?.clientWidth ?? 0,
      height: uRefDiv.current?.clientHeight ?? 0,
      series: [],
      ...opts,
      plugins: [...(opts?.plugins ?? []), uPlotPlugin],
    };
    debug.log('%cUPlotWrapper create %d %d', 'color:blue;', width, height);
    uRef.current = new uPlot(opt, undefined, uRefDiv.current!);
    updateData(data);
    updateSeries(series);
    updateBands(bands);
    updateScales(scales);
    moveLegend();
  }, [
    bands,
    data,
    height,
    moveLegend,
    opts,
    scales,
    series,
    uPlotPlugin,
    updateBands,
    updateData,
    updateScales,
    updateSeries,
    width,
  ]);

  useEffect(() => {
    const redraw = () => {
      if (window.document.visibilityState === 'visible') {
        uRef.current?.redraw();
      }
    };
    window.document.addEventListener('visibilitychange', redraw);
    return () => {
      window.document.removeEventListener('visibilitychange', redraw);
    };
  }, []);

  useEffect(() => {
    if (width > 0) {
      if (uRef.current) {
        const uWrap = uRef.current.root.querySelector<HTMLDivElement>('.u-wrap');
        if (uWrap) {
          uWrap.setAttribute('data-s', '1');
          uWrap.style.width = `${width}px`;
          uWrap.style.height = `${height}px`;
        }
        const timeout = setTimeout(() => {
          uRef.current?.setSize({ width, height });
        }, 100);
        return () => {
          clearTimeout(timeout);
        };
      }
    }
  }, [height, width]);

  useEffect(() => {
    updateData(data);
  }, [data, updateData]);

  useEffect(() => {
    updateSeries(series);
  }, [series, updateSeries]);

  useEffect(() => {
    updateBands(bands);
  }, [bands, updateBands]);

  useEffect(() => {
    updateScales(scales);
  }, [scales, updateScales, series, bands]);

  useEffect(() => {
    if (uRef.current) {
      const timeout = setTimeout(() => {
        microTask(() => {
          if (uRef.current) {
            onUpdatePreview?.(uRef.current);
          }
        });
      }, 200);

      return () => {
        clearTimeout(timeout);
      };
    }
  }, [series, data, scales, width, height, onUpdatePreview]);

  return (
    <div className={className} ref={uRefDiv}>
      {children}
    </div>
  );
}

export const UPlotWrapper = memo(UPlotWrapperNoMemo) as typeof UPlotWrapperNoMemo;
