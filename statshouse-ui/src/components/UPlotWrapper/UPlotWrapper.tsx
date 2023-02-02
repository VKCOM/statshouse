// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import uPlot from 'uplot';
import { useResizeObserver } from '../../view/utils';
import { canvasToImageData } from '../../common/canvasToImage';
import { debug } from '../../common/debug';

export type LegendItem<T = Object> = {
  label: string;
  width: number;
  fill?: string;
  stroke?: string;
  show: boolean;
  value?: string;
  values?: T;
  alpha?: number;
  focus?: boolean;
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

export type UPlotWrapperProps = {
  opts?: UPlotWrapperPropsOpts;
  data?: uPlot.AlignedData;
  scales?: UPlotWrapperPropsScales;
  series?: uPlot.Series[];
  legendTarget?: HTMLDivElement | null;
  onUpdatePreview?: React.Dispatch<React.SetStateAction<string>>;
  onUpdateLegend?: React.Dispatch<React.SetStateAction<LegendItem[]>>;
  className?: string;
} & UPlotWrapperPropsHooks;

export const microTask =
  typeof queueMicrotask === 'undefined' ? (fn: () => void) => Promise.resolve().then(fn) : queueMicrotask;

function readLegend(u: uPlot): LegendItem[] {
  return u.series.map((s, index) => {
    let idx = u.legend.idxs?.[index];
    let lastTime = '';
    if (idx === null && u.data?.[index]?.length) {
      let lastIndex = u.data[index].length - 1;
      while (typeof u.data[index][lastIndex] !== 'number' && lastIndex > 0) {
        lastIndex--;
      }
      idx = lastIndex;
      if (index === 0) {
        lastTime =
          (typeof s.value === 'function' ? s.value(u, u.data[index][idx], 0, idx) : s.value)
            ?.toString()
            .replace('--', '') ?? ''; // replace '--' uplot
      }
    }
    return {
      label: s.label ?? '',
      width:
        (u.legend.markers?.width instanceof Function ? u.legend.markers?.width(u, index) : u.legend.markers?.width) ??
        1,
      fill: s.fill instanceof Function ? s.fill(u, index)?.toString() : s.fill?.toString(),
      stroke: s.stroke instanceof Function ? s.stroke(u, index)?.toString() : s.stroke?.toString(),
      show: s.show ?? false,
      value: u.legend.values?.[index]?.['_']?.toString().replace('--', '') || lastTime, // replace '--' uplot
      values: typeof idx === 'number' ? s.values?.(u, index, idx) : undefined,
      alpha: s.alpha,
    };
  });
}

export const _UPlotWrapper: React.FC<UPlotWrapperProps> = ({
  opts,
  data = [[]],
  series = [],
  scales = {},
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
}) => {
  const uRef = useRef<uPlot>();
  const uRefDiv = useRef<HTMLDivElement>(null);
  const { width, height } = useResizeObserver(uRefDiv);
  const hooksEvent = useRef<UPlotWrapperPropsHooks>({});
  const [seriesFocus, setSeriesFocus] = useState<null | number>(null);
  const [legend, setLegend] = useState<LegendItem[]>([]);

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
  const getPreview = useCallback(
    async (u: uPlot) =>
      canvasToImageData(
        u.ctx.canvas,
        u.bbox.left,
        u.bbox.top,
        u.bbox.width,
        u.bbox.height,
        devicePixelRatio ? devicePixelRatio * 300 : 300
      ),
    []
  );

  const updateScales = useCallback((scales?: UPlotWrapperPropsScales) => {
    if (scales && uRef.current) {
      const nextScales: UPlotWrapperPropsScales = Object.fromEntries(
        Object.entries(scales).map(([key, value]) => [key, { ...value }])
      );
      uRef.current?.batch((u: uPlot) => {
        Object.entries(nextScales).forEach(([key, scale]) => {
          u.setScale(key, scale);
        });
      });
    }
  }, []);

  const updateSeries = useCallback((series: uPlot.Series[]) => {
    if (uRef.current) {
      if (series.length) {
        uRef.current.batch((u: uPlot) => {
          const show: Record<string, boolean | undefined> = {};
          for (let i = u.series.length - 1; i > 0; i--) {
            show[u.series[i].label!] = u.series[i].show;
            u.delSeries(i);
          }

          let nextSeries = series.map((s) => ({
            ...s,
            show: typeof show[s.label!] !== 'undefined' ? show[s.label!] : s.show ?? true,
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
      uRef.current.batch((u: uPlot) => {
        u.setData(data);
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
      opts: (u, opts) => {
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
    const legendF = legend.slice();
    if (seriesFocus !== null) {
      legendF[seriesFocus] = { ...legendF[seriesFocus], focus: true };
    }
    onUpdateLegend?.(legendF);
  }, [legend, seriesFocus, onUpdateLegend]);

  useLayoutEffect(() => {
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
    updateSeries(series);
    updateData(data);
    updateScales(scales);
    moveLegend();
  }, [data, height, moveLegend, opts, scales, series, uPlotPlugin, updateData, updateScales, updateSeries, width]);

  useLayoutEffect(
    () => () => {
      if (uRef.current) {
        debug.log('%cUPlotWrapper destroy', 'color:blue;');
        uRef.current?.destroy();
        uRef.current = undefined;
      }
    },
    [opts]
  );

  useLayoutEffect(() => {
    uRef.current?.setSize({ width, height });
  }, [height, width]);

  useEffect(() => {
    updateData(data);
  }, [data, updateData]);

  useEffect(() => {
    updateScales(scales);
  }, [scales, updateScales]);

  useEffect(() => {
    updateSeries(series);
    updateScales(scales);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [series, updateSeries]);

  useEffect(() => {
    if (!onUpdatePreview) {
      return;
    }
    const controller = new AbortController();
    const timer = setTimeout(
      () =>
        microTask(() =>
          uRef.current?.batch(async (u: uPlot) => {
            const url = await getPreview(u);
            if (!controller.signal.aborted) {
              onUpdatePreview?.((old = '') => {
                if (old.slice(0, 5) === 'blob:' && old !== url) {
                  URL.revokeObjectURL(old);
                }
                return url;
              });
            }
          })
        ),
      200
    );
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [getPreview, series, data, scales, width, height, onUpdatePreview]);

  return <div className={className} ref={uRefDiv}></div>;
};

export const UPlotWrapper = memo(_UPlotWrapper);
