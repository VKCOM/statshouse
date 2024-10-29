// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { lazy, memo, Suspense, useCallback, useEffect, useState } from 'react';
import { Button, TextArea } from 'components/UI';
import { useStateToRef } from 'hooks';
import cn from 'classnames';
import { ReactComponent as SVGArrowCounterclockwise } from 'bootstrap-icons/icons/arrow-counterclockwise.svg';
import { ReactComponent as SVGChevronCompactLeft } from 'bootstrap-icons/icons/chevron-compact-left.svg';
import { ReactComponent as SVGChevronCompactRight } from 'bootstrap-icons/icons/chevron-compact-right.svg';
import { getNewMetric, type PlotKey } from 'url2';
import { useStatsHouseShallow } from 'store2';
import { PrometheusSwitch } from './PrometheusSwitch';

const FallbackEditor = (props: { className?: string; value?: string; onChange?: (value: string) => void }) => (
  <div className="input-group">
    <TextArea {...props} className="form-control-sm rounded font-monospace" autoHeight style={{ minHeight: 202 }} />
  </div>
);

const PromQLEditor = lazy(() =>
  import('components/UI/PromQLEditor').catch(() => ({
    default: FallbackEditor,
  }))
);

export type PlotControlPromQLEditorProps = {
  className?: string;
  plotKey: PlotKey;
};

const { prometheusCompat: defaultPrometheusCompat } = getNewMetric();

export function _PlotControlPromQLEditor({ className, plotKey }: PlotControlPromQLEditorProps) {
  const { promQLParam, promqlExpand, togglePromqlExpand, setPlot, prometheusCompat } = useStatsHouseShallow(
    ({ params: { plots }, plotsData, togglePromqlExpand, setPlot }) => ({
      promQLParam: plots[plotKey]?.promQL ?? '',
      promqlExpand: plotsData[plotKey]?.promqlExpand ?? false,
      prometheusCompat: plots[plotKey]?.prometheusCompat ?? defaultPrometheusCompat,
      togglePromqlExpand,
      setPlot,
    })
  );

  const [promQL, setPromQL] = useState(promQLParam);
  const promQlRef = useStateToRef(promQL);

  const resetPromQL = useCallback(() => {
    setPromQL(promQLParam);
  }, [promQLParam]);

  const onTogglePromqlExpand = useCallback(() => {
    togglePromqlExpand(plotKey);
  }, [plotKey, togglePromqlExpand]);

  const sendPromQL = useCallback(() => {
    setPlot(plotKey, (p) => {
      p.promQL = promQlRef.current;
    });
  }, [plotKey, promQlRef, setPlot]);

  useEffect(() => {
    setPromQL(promQLParam);
  }, [promQLParam]);

  const setPrometheusCompat = useCallback(
    (status: boolean) => {
      setPlot(plotKey, (p) => {
        p.prometheusCompat = status;
      });
    },
    [plotKey, setPlot]
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
}

export const PlotControlPromQLEditor = memo(_PlotControlPromQLEditor);
