import { MutableRefObject, useMemo, useRef } from 'react';
import type { Plugin } from 'uplot';
import { UPlotWrapperPropsHooks } from '../components/UPlotWrapper';
import { EventObserver } from '../common/EventObserver';

export function useUPlotPluginHooks(): [Plugin, MutableRefObject<EventObserver<keyof UPlotWrapperPropsHooks>>] {
  const hooksEvent = useRef<EventObserver<keyof UPlotWrapperPropsHooks>>(new EventObserver(true));
  const uPlotPlugin = useMemo(
    (): Plugin => ({
      hooks: {
        init: (u, opts, data) => {
          hooksEvent.current.trigger('onInit', u, opts, data);
        },
        destroy: (u) => {
          hooksEvent.current.trigger('onDestroy', u);
        },
        draw: (u) => {
          hooksEvent.current.trigger('onDraw', u);
        },
        drawAxes: (u) => {
          hooksEvent.current.trigger('onDrawAxes', u);
        },
        drawClear: (u) => {
          hooksEvent.current.trigger('onDrawClear', u);
        },
        ready: (u) => {
          hooksEvent.current.trigger('onReady', u);
        },
        setData: (u) => {
          hooksEvent.current.trigger('onSetData', u);
        },
        syncRect: (u, rect) => {
          hooksEvent.current.trigger('onSyncRect', u, rect);
        },
        setSeries: (u, seriesIdx, opts) => {
          hooksEvent.current.trigger('onSetSeries', u, seriesIdx, opts);
        },
        addSeries: (u, seriesIdx) => {
          hooksEvent.current.trigger('onAddSeries', u, seriesIdx);
        },
        delSeries: (u, seriesIdx) => {
          hooksEvent.current.trigger('onDelSeries', u, seriesIdx);
        },
        drawSeries: (u, seriesIdx) => {
          hooksEvent.current.trigger('onDrawSeries', u, seriesIdx);
        },
        setScale: (u, scaleKey) => {
          hooksEvent.current.trigger('onSetScale', u, scaleKey);
        },
        setSize: (u) => {
          hooksEvent.current.trigger('onSetSize', u);
        },
        setCursor: (u) => {
          hooksEvent.current.trigger('onSetCursor', u);
        },
        setLegend: (u) => {
          hooksEvent.current.trigger('onSetLegend', u);
        },
        setSelect: (u) => {
          hooksEvent.current.trigger('onSetSelect', u);
        },
      },
    }),
    []
  );
  return [uPlotPlugin, hooksEvent];
}
