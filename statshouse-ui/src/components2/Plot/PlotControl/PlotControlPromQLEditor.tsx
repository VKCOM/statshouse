// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { lazy, memo, Suspense, useCallback, useEffect, useMemo, useState } from 'react';
import { Button, TextArea } from '@/components/UI';
import { useStateToRef } from '@/hooks';
import cn from 'classnames';
import { ReactComponent as SVGArrowCounterclockwise } from 'bootstrap-icons/icons/arrow-counterclockwise.svg';
import { ReactComponent as SVGChevronCompactLeft } from 'bootstrap-icons/icons/chevron-compact-left.svg';
import { ReactComponent as SVGChevronCompactRight } from 'bootstrap-icons/icons/chevron-compact-right.svg';
import { PrometheusSwitch } from './PrometheusSwitch';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { setPlotData, usePlotsDataStore } from '@/store2/plotDataStore';

const FallbackEditor = (props: { className?: string; value?: string; onChange?: (value: string) => void }) => (
  <div className="input-group">
    <TextArea {...props} className="form-control-sm rounded font-monospace" autoHeight style={{ minHeight: 202 }} />
  </div>
);

const PromQLEditor = lazy(() =>
  import('@/components/UI/PromQLEditor').catch(() => ({
    default: FallbackEditor,
  }))
);

export type PlotControlPromQLEditorProps = {
  className?: string;
};

export const PlotControlPromQLEditor = memo(function PlotControlPromQLEditor({
  className,
}: PlotControlPromQLEditorProps) {
  const {
    plot: { id, promQL: promQLParam, prometheusCompat },
    setPlot,
  } = useWidgetPlotContext();
  const promqlExpand = usePlotsDataStore(useCallback(({ plotsData }) => !!plotsData[id]?.promqlExpand, [id]));

  const setData = useMemo(() => setPlotData.bind(undefined, id), [id]);

  const [promQL, setPromQL] = useState(promQLParam);
  const promQlRef = useStateToRef(promQL);

  const resetPromQL = useCallback(() => {
    setPromQL(promQLParam);
  }, [promQLParam]);

  const onTogglePromqlExpand = useCallback(() => {
    setData((d) => {
      d.promqlExpand = !d.promqlExpand;
    });
  }, [setData]);

  const sendPromQL = useCallback(() => {
    setPlot((p) => {
      p.promQL = promQlRef.current;
    });
  }, [promQlRef, setPlot]);

  useEffect(() => {
    setPromQL(promQLParam);
  }, [promQLParam]);

  const setPrometheusCompat = useCallback(
    (status: boolean) => {
      setPlot((p) => {
        p.prometheusCompat = status;
      });
    },
    [setPlot]
  );

  return (
    <div className={cn('d-flex flex-column gap-2', className)}>
      <Suspense fallback={<FallbackEditor value={promQL} onChange={setPromQL} />}>
        {!!PromQLEditor && <PromQLEditor className="input-group" value={promQL} onChange={setPromQL} />}
      </Suspense>
      <div className="d-flex flex-row gap-2">
        <Button
          onClick={onTogglePromqlExpand}
          className={cn('btn btn-outline-primary')}
          title={promqlExpand ? 'Narrow' : 'Expand'}
        >
          {promqlExpand ? <SVGChevronCompactRight /> : <SVGChevronCompactLeft />}
        </Button>
        <Button type="button" className="btn btn-outline-primary" title="Reset PromQL" onClick={resetPromQL}>
          <SVGArrowCounterclockwise />
        </Button>
        <span className="flex-grow-1"></span>
        <PrometheusSwitch prometheusCompat={prometheusCompat} setPrometheusCompat={setPrometheusCompat} />
        <Button type="button" className="btn btn-outline-primary" onClick={sendPromQL}>
          Run
        </Button>
      </div>
    </div>
  );
});
